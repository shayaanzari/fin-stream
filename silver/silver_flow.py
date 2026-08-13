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

import polars as pl
from openai import OpenAI
from prefect import flow, task, get_run_logger
from prefect.tasks import exponential_backoff

from silver.models import PredictionBatch, CATEGORIES

# ── Config ────────────────────────────────────────────────────────────────────
LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "http://10.0.0.81:4000/v1")
LITELLM_API_KEY  = os.getenv("LITELLM_KEY", "")
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
    DLQ_COLS = ["bronze_id", "vendor", "amount", "transaction_type", "raw_text", "failure_reason"]
    df = pl.DataFrame([{k: r[k] for k in DLQ_COLS} for r in dlq_rows])
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
        WHERE  b.id NOT IN (SELECT bronze_id FROM silver.transactions)
          AND  b.id NOT IN (SELECT bronze_id FROM silver.transactions_dlq)
        ORDER  BY b.alert_datetime ASC
        LIMIT  {limit}
    """
    df = pl.read_database_uri(query=query, uri=DB_URI, engine="adbc")
    logger.info(f"Fetched {len(df)} unclassified rows")
    return df.to_dicts()


@task(name="classify-batch", retries=2, retry_delay_seconds=10)
def classify_batch(rows: list[dict]) -> list[dict]:
    """Send a batch of vendor strings to the LLM, merge predictions back into full rows.

    Uses OpenAI's beta.chat.completions.parse() to automatically parse and validate
    output into a typed PredictionBatch Pydantic model.

    Returns only rows whose confidence score meets CONFIDENCE_THRESHOLD.
    Low-confidence rows are written to silver.transactions_dlq and excluded from the return value.
    On a full-batch API failure, returns [] so all rows stay in bronze for automatic retry.
    """
    logger = get_run_logger()
    if not rows:
        return []

    # Build ordered (un-numbered) prompt — position = identity
    items = "\n".join(
        f"{r['vendor']} | ${r['amount']} | {r['transaction_type']}"
        for r in rows
    )

    system_prompt = (
        "You are a financial transaction classifier. "
        f"Classify each of the {len(rows)} transactions below IN EXACT ORDER into the schema.\n"
        f"Allowed categories: {', '.join(CATEGORIES)}."
    )

    # ── Structured output: automatically validated into Pydantic model ───────
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
    except Exception as exc:
        # Full-batch failure: schema parse error, API rate limit, network error, etc.
        # Leave rows in bronze — next poll cycle will retry them.
        logger.error(f"LLM API failure ({exc}); {len(rows)} rows left in bronze for retry")
        return []

    batch: PredictionBatch | None = response.choices[0].message.parsed
    model_used = response.model

    if batch is None:
        logger.error(f"LLM returned refusal/None; {len(rows)} rows left in bronze for retry")
        return []

    # ── Length mismatch guard ────────────────────────────────────────────────
    if len(batch.predictions) != len(rows):
        logger.error(
            f"LLM returned {len(batch.predictions)} predictions for {len(rows)} rows; "
            "treating as full-batch failure — rows left in bronze for retry"
        )
        return []

    # ── Confidence threshold fork ────────────────────────────────────────────
    enriched, dlq_rows = [], []
    now = datetime.now(timezone.utc)  # explicit UTC timestamp; not left to DB DEFAULT

    for row, pred in zip(rows, batch.predictions):
        if pred.c < CONFIDENCE_THRESHOLD:
            dlq_rows.append({
                **row,
                "failure_reason": (
                    f"low_confidence:{pred.c:.4f} "
                    f"(category={pred.v}, threshold={CONFIDENCE_THRESHOLD})"
                ),
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
        f"{len(dlq_rows)} row(s) below confidence threshold → silver.transactions_dlq"
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
def silver_flow():
    """Top-level Prefect flow: fetch → classify → write, then sleep."""
    logger = get_run_logger()
    while True:
        rows     = fetch_unclassified(limit=BATCH_SIZE)
        enriched = classify_batch(rows)
        written  = write_silver(enriched)

        if not rows:
            logger.info(f"No unclassified rows found in bronze. Sleeping {POLL_INTERVAL_S}s …")
        elif written == 0:
            logger.info(f"Batch processing deferred or sent to DLQ. Will retry next cycle in {POLL_INTERVAL_S}s …")
        
        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    silver_flow()
