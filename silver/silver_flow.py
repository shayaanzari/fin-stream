"""
silver_flow.py — Micro-batch LLM enrichment pipeline.

Medallion pattern: silver.transactions is a SUPERSET of bronze.transactions.
Every bronze column is carried forward and the three enrichment columns are appended.
Downstream consumers (Gold, dbt, dashboards) query silver only — no join to bronze.

Flow:
  fetch_unclassified  → list[dict]  (full bronze rows not yet in silver)
  classify_batch      → list[dict]  (full rows + LLM predictions merged)
  write_silver        → int         (rows written)

Failure disposition:
  Full-batch API failure (error, length mismatch) → return [] → rows stay in bronze → retried next poll
  Low-confidence result (confidence < CONFIDENCE_THRESHOLD) → silver.transactions_dlq
"""

import os
import time
from datetime import datetime, timezone

import pydantic
import polars as pl
import openai
from openai import OpenAI
from prefect import flow, task, get_run_logger
from prefect.tasks import exponential_backoff

from silver.models import PredictionBatch, Prediction, CATEGORIES

# ── Config ────────────────────────────────────────────────────────────────────
LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "http://10.0.0.81:4000/v1")
LITELLM_API_KEY  = os.getenv("LITELLM_KEY") or "sk-dummy"
LLM_MODEL        = os.getenv("LLM_MODEL", "nemomoo")
BATCH_SIZE       = int(os.getenv("SILVER_BATCH_SIZE", "25"))
POLL_INTERVAL_S  = int(os.getenv("SILVER_POLL_INTERVAL_S", "60"))

CONFIDENCE_THRESHOLD = 0.5  # minimum confidence score to write to silver; below this → DLQ

DB_URI = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST','postgres')}:{os.getenv('POSTGRES_PORT','5432')}"
    f"/{os.getenv('POSTGRES_DB')}"
)

client = OpenAI(base_url=LITELLM_BASE_URL, api_key=LITELLM_API_KEY)

# Columns to SELECT from bronze (all of them, renamed where needed).
# amount is cast to float because ADBC's copy writer does not support
# writing Arrow double → PostgreSQL numeric; silver stores it as DOUBLE PRECISION.
BRONZE_SELECT = """
    b.id                  AS bronze_id,
    b.alert_datetime,
    b.bank,
    b.vendor,
    b.amount::float       AS amount,
    b.transaction_type,
    b.card_last_4,
    b.is_card_not_present,
    b.raw_text,
    b.created_at          AS bronze_created_at
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _write_silver_dlq(dlq_rows: list[dict]) -> None:
    """Persist low-confidence rows to silver.transactions_dlq for human review."""
    logger = get_run_logger()
    if not dlq_rows:
        return
    DLQ_COLS = [
        "bronze_id", "vendor", "amount", "transaction_type", "raw_text",
        "vendor_classification", "confidence_score", "llm",
        "failure_reason",
    ]
    df = (
        pl.DataFrame([{k: r.get(k) for k in DLQ_COLS} for r in dlq_rows])
        .with_columns([
            pl.col("bronze_id").cast(pl.Int32),
            pl.col("amount").cast(pl.Float64),
            pl.col("confidence_score").cast(pl.Float64),
        ])
    )
    df.write_database(
        table_name="silver.transactions_dlq",
        connection=DB_URI,
        engine="adbc",
        if_table_exists="append",
    )
    logger.warning(f"Wrote {len(dlq_rows)} row(s) to silver.transactions_dlq")


# ── Tasks ─────────────────────────────────────────────────────────────────────

@task(name="fetch-unclassified", retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=2))
def fetch_unclassified(limit: int = BATCH_SIZE) -> list[dict]:
    """Fetch full bronze rows that have no silver counterpart yet."""
    logger = get_run_logger()
    query = f"""
        SELECT {BRONZE_SELECT}
        FROM   bronze.transactions b
        WHERE  NOT EXISTS (SELECT 1 FROM silver.transactions     s WHERE s.bronze_id = b.id)
          AND  NOT EXISTS (SELECT 1 FROM silver.transactions_dlq d WHERE d.bronze_id = b.id)
        ORDER  BY b.alert_datetime ASC
        LIMIT  {limit}
    """
    df = pl.read_database_uri(query=query, uri=DB_URI, engine="adbc")
    logger.info(f"Fetched {len(df)} unclassified rows")
    return df.to_dicts()


def _predict_rows(rows: list[dict]) -> tuple[list[Prediction] | None, str | None, str | None]:
    """Helper to classify a specific number of rows.
    Returns (predictions, model_name, None) on success.
    Returns (None, model_name, error_reason) on parse error, length mismatch, or refusal.
    Raises transient API errors (e.g., openai.RateLimitError) to be caught by the caller.
    """
    logger = get_run_logger()
    if not rows:
        return [], None, None

    items = "\n".join(
        f"{r['vendor']} | ${r['amount']} | {r['transaction_type']}"
        for r in rows
    )

    system_prompt = (
        "You are a financial transaction classifier. "
        f"Classify each of the {len(rows)} transactions below IN EXACT ORDER into the schema.\n"
        f"Allowed categories: {', '.join(CATEGORIES)}."
    )

    try:
        response = client.beta.chat.completions.parse(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": items},
            ],
            response_format=PredictionBatch,
            temperature=0.0,
        )
    except openai.APIError as exc:
        raise exc  # All HTTP status codes (4xx, 5xx), timeouts, & connection drops bubble up
    except pydantic.ValidationError as exc:
        details = "; ".join(
            f"field={'.'.join(str(loc) for loc in e['loc'])!r} "
            f"got={e.get('input')!r} ({e['msg']})"
            for e in exc.errors()
        )
        logger.warning(f"Pydantic ValidationError for batch of {len(rows)}: {details}")
        return None, None, "parse_error"
    except Exception as exc:
        logger.warning(f"Parse error ({type(exc).__name__}) for batch of {len(rows)}: {exc}")
        return None, None, "parse_error"

    batch = response.choices[0].message.parsed
    model_used = response.model

    if batch is None:
        finish_reason = response.choices[0].finish_reason
        refusal = getattr(response.choices[0].message, "refusal", None)
        raw_content = response.choices[0].message.content or ""
        if refusal:
            logger.warning(
                f"Model refusal for batch of {len(rows)}: {refusal!r}"
            )
        elif finish_reason == "length":
            logger.warning(
                f"Token limit hit (finish_reason='length') for batch of {len(rows)}: "
                f"structured output was truncated before completion. "
                f"Consider reducing SILVER_BATCH_SIZE."
            )
        else:
            logger.warning(
                f"Null structured output for batch of {len(rows)} "
                f"(finish_reason={finish_reason!r}, content={raw_content[:200]!r}). "
                f"Model did not produce a parseable response."
            )
        return None, model_used, "parse_error"

    if len(batch.predictions) != len(rows):
        raw_content = response.choices[0].message.content or ""
        logger.warning(
            f"Length mismatch for batch of {len(rows)}: "
            f"expected {len(rows)}, got {len(batch.predictions)}. "
            f"Raw content (first 500 chars): {raw_content[:500]!r}"
        )
        return None, model_used, "parse_error"

    return batch.predictions, model_used, None


@task(name="classify-batch", retries=2, retry_delay_seconds=10)
def classify_batch(rows: list[dict]) -> list[dict]:
    """Send a batch of vendor strings to the LLM, merge predictions back into full rows.

    Uses OpenAI's beta.chat.completions.parse() to automatically parse and validate
    output into a typed PredictionBatch Pydantic model.

    Implements a fallback mechanism: If a bulk batch (e.g. 25) fails to parse, 
    it falls back to processing row-by-row to isolate the poison pill transaction 
    into the DLQ without blocking the rest of the batch.
    """
    logger = get_run_logger()
    if not rows:
        return []

    enriched, dlq_rows = [], []
    now = datetime.now(timezone.utc)
    
    try:
        # 1. Attempt bulk classification
        logger.info(f"Sending {len(rows)} row(s) to {LLM_MODEL!r} …")
        predictions, model_used, err_reason = _predict_rows(rows)
        
        # 2. Fallback to row-by-row if bulk fails (Sad Path Isolation)
        if predictions is None:
            logger.warning(
                f"Bulk classify failed ({err_reason}). "
                f"Falling back to {len(rows)} individual LLM calls."
            )
            predictions = []

            for i, row in enumerate(rows, start=1):
                logger.info(f"Attempting row [{i}/{len(rows)}]: vendor={row['vendor']!r}")
                single_pred, single_model, single_err = _predict_rows([row])
                model_used = single_model or model_used

                if single_pred is None:
                    # Isolated the poison pill!
                    logger.error(f"Isolated poison pill: '{row['vendor']}' failed parsing.")
                    dlq_rows.append({
                        **row,
                        "failure_reason": "parse_error",
                    })
                    predictions.append(None) # Keep index alignment
                else:
                    logger.info(
                        f"Row [{i}/{len(rows)}] → {single_pred[0].v!r} "
                        f"(conf={single_pred[0].c:.2f})"
                    )
                    predictions.append(single_pred[0])

    except Exception as exc:
        # Transient API error bubbled up (RateLimit, Connection, etc.)
        # Returning [] aborts the batch completely, leaving any unprocessed/processed rows in bronze for retry
        logger.error(f"Transient LLM API failure ({type(exc).__name__}); leaving rows in bronze for retry.")
        return []

    # 3. Process predictions (confidence fork)
    for row, pred in zip(rows, predictions):
        if pred is None:
            continue # Already handled (put in DLQ as parse error)

        if pred.c < CONFIDENCE_THRESHOLD or pred.v == "Other":
            if pred.v == "Other":
                reason = "category_is_other"
            else:
                reason = "low_confidence"

            dlq_rows.append({
                **row,
                "vendor_classification": pred.v,
                "confidence_score":      pred.c,
                "llm":                   model_used,
                "failure_reason":        reason,
            })
        else:
            enriched.append({
                # ── All bronze columns carried forward ───────────────────────
                **row,
                # ── Silver enrichment columns appended ──────────────────────
                "vendor_classification": pred.v,
                "confidence_score":      pred.c,
                "llm":                   model_used,
                "classified_at":         now,
            })

    if dlq_rows:
        _write_silver_dlq(dlq_rows)

    logger.info(
        f"Classified {len(enriched)} rows via {model_used}; "
        f"{len(dlq_rows)} row(s) below confidence threshold or unparseable → silver.transactions_dlq"
    )
    return enriched


@task(name="write-silver", retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=2))
def write_silver(enriched: list[dict]) -> int:
    """Write full enriched rows to silver.transactions via ADBC (zero-copy Arrow path)."""
    if not enriched:
        return 0

    df = pl.DataFrame(enriched).with_columns([
        pl.col("bronze_id").cast(pl.Int32),
        # amount is already Float64 (fetched as amount::float); silver DDL uses DOUBLE PRECISION
        pl.col("confidence_score").cast(pl.Float64),
        pl.col("classified_at").cast(pl.Datetime("us", "UTC")),
    ])

    rows_written = df.write_database(
        table_name="silver.transactions",
        connection=DB_URI,
        engine="adbc",
        if_table_exists="append",
    )
    get_run_logger().info(f"Wrote {rows_written} rows to silver.transactions")
    return rows_written


# ── Flow ──────────────────────────────────────────────────────────────────────

@flow(name="silver-enrichment-flow", log_prints=True)
def silver_flow() -> int:
    """Single-batch Prefect flow: fetch → classify → write."""
    logger = get_run_logger()
    logger.info(f"batch_size={BATCH_SIZE} | poll_interval={POLL_INTERVAL_S}s")
    rows = fetch_unclassified(limit=BATCH_SIZE)
    if not rows:
        logger.info("No unclassified rows found in bronze.")
        return 0

    enriched = classify_batch(rows)
    written = write_silver(enriched)

    if written == 0:
        logger.info("Batch processing deferred or sent to DLQ.")
    return written


def check_llm_connectivity() -> bool:
    """Pre-flight check to verify LiteLLM connectivity, credentials, and model availability."""
    import logging
    daemon_logger = logging.getLogger("silver_poller")
    daemon_logger.info(f"Testing LiteLLM connection at {LITELLM_BASE_URL} (model: '{LLM_MODEL}') …")
    try:
        client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=1,
            timeout=10.0,
        )
        daemon_logger.info(f"✅ LiteLLM connection verified (model: '{LLM_MODEL}')")
        return True
    except Exception as exc:
        daemon_logger.warning(f"⚠️ LiteLLM pre-flight check failed ({type(exc).__name__}): {exc}")
        return False


def run_loop():
    """External polling loop executing discrete batch flow runs."""
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    daemon_logger = logging.getLogger("silver_poller")

    daemon_logger.info("Initializing Silver Poller Daemon …")
    check_llm_connectivity()

    while True:
        try:
            silver_flow()
            daemon_logger.info(f"Batch complete. Sleeping {POLL_INTERVAL_S}s until next poll …")
            time.sleep(POLL_INTERVAL_S)
        except KeyboardInterrupt:
            daemon_logger.info("Polling loop stopped by user.")
            break
        except Exception as exc:
            daemon_logger.error(f"Unexpected error in batch cycle: {exc}. Retrying in {POLL_INTERVAL_S}s …")
            time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    run_loop()
