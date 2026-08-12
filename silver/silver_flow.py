"""
silver_flow.py — Micro-batch LLM enrichment pipeline.

Medallion pattern: silver.transactions is a SUPERSET of bronze.transactions.
Every bronze column is carried forward and the three enrichment columns are appended.
Downstream consumers (Gold, dbt, dashboards) query silver only — no join to bronze.

Flow:
  fetch_unclassified  → list[dict]  (full bronze rows not yet in silver)
  classify_batch      → list[dict]  (full rows + LLM predictions merged)
  write_silver        → int         (rows written)
"""

import json
import os
import time
from datetime import datetime, timezone

import polars as pl
from openai import OpenAI
from prefect import flow, task, get_run_logger
from prefect.tasks import exponential_backoff

# ── Config ────────────────────────────────────────────────────────────────────
LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "http://10.0.0.81:4000/v1")
LITELLM_API_KEY  = os.getenv("LITELLM_KEY", "")
LLM_MODEL        = os.getenv("LLM_MODEL", "openrouter/openrouter/free")
BATCH_SIZE       = int(os.getenv("SILVER_BATCH_SIZE", "25"))
POLL_INTERVAL_S  = int(os.getenv("SILVER_POLL_INTERVAL_S", "60"))

DB_URI = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST','postgres')}:{os.getenv('POSTGRES_PORT','5432')}"
    f"/{os.getenv('POSTGRES_DB')}"
)

client = OpenAI(base_url=LITELLM_BASE_URL, api_key=LITELLM_API_KEY)

CATEGORIES = [
    "Groceries", "Dining", "Subscriptions", "Travel", "Gas",
    "Shopping", "Healthcare", "Entertainment", "Utilities",
    "ATM/Cash", "Transfer", "Credit/Refund", "Other",
]

# Columns to SELECT from bronze (all of them, renamed where needed)
BRONZE_SELECT = """
    b.id             AS bronze_id,
    b.alert_datetime,
    b.bank,
    b.vendor,
    b.amount,
    b.transaction_type,
    b.card_last_4,
    b.is_card_not_present,
    b.raw_text,
    b.created_at     AS bronze_created_at
"""

# ── Tasks ─────────────────────────────────────────────────────────────────────

@task(name="fetch-unclassified", retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=2))
def fetch_unclassified(limit: int = BATCH_SIZE) -> list[dict]:
    """Fetch full bronze rows that have no silver counterpart yet."""
    logger = get_run_logger()
    query = f"""
        SELECT {BRONZE_SELECT}
        FROM   bronze.transactions b
        WHERE  b.id NOT IN (SELECT bronze_id FROM silver.transactions)
        ORDER  BY b.alert_datetime ASC
        LIMIT  {limit}
    """
    df = pl.read_database_uri(query=query, uri=DB_URI, engine="adbc")
    logger.info(f"Fetched {len(df)} unclassified rows")
    return df.to_dicts()


@task(name="classify-batch", retries=2, retry_delay_seconds=10)
def classify_batch(rows: list[dict]) -> list[dict]:
    """Send a batch of vendor strings to the LLM, merge predictions back into full rows."""
    logger = get_run_logger()
    if not rows:
        return []

    # Build numbered prompt: "1. WHOLEFDS MKT 0483 | $47.23 | PURCHASE"
    items = "\n".join(
        f"{i+1}. {r['vendor']} | ${r['amount']} | {r['transaction_type']}"
        for i, r in enumerate(rows)
    )

    system_prompt = (
        "You are a financial transaction classifier. "
        "For each numbered transaction, output ONLY a JSON array (one object per item) "
        "with keys: 'index' (1-based int), 'vendor_classification' (string, one of the allowed categories), "
        "'confidence_score' (float 0-1).\n"
        f"Allowed categories: {', '.join(CATEGORIES)}.\n"
        "Output raw JSON only — no markdown, no explanation."
    )

    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": items},
        ],
        temperature=0.0,
    )

    raw = response.choices[0].message.content.strip()
    model_used = response.model

    try:
        predictions = json.loads(raw)
    except json.JSONDecodeError:
        logger.error(f"LLM returned non-JSON: {raw[:200]}")
        predictions = [
            {"index": i+1, "vendor_classification": "Other", "confidence_score": 0.0}
            for i in range(len(rows))
        ]

    pred_map = {p["index"]: p for p in predictions}
    enriched = []
    for i, row in enumerate(rows):
        pred = pred_map.get(i + 1, {"vendor_classification": "Other", "confidence_score": 0.0})
        enriched.append({
            # ── All bronze columns carried forward ───────────────────────────
            **row,
            # ── Silver enrichment columns appended ──────────────────────────
            "vendor_classification": pred.get("vendor_classification", "Other"),
            "confidence_score":      float(pred.get("confidence_score", 0.0)),
            "llm_model_used":        model_used,
            "classified_at":         datetime.now(timezone.utc),  # explicit UTC timestamp; not left to DB DEFAULT
        })

    logger.info(f"Classified {len(enriched)} rows via {model_used}")
    return enriched


@task(name="write-silver", retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=2))
def write_silver(enriched: list[dict]) -> int:
    """Write full enriched rows to silver.transactions via ADBC (zero-copy Arrow path)."""
    if not enriched:
        return 0

    df = pl.DataFrame(enriched).with_columns([
        pl.col("bronze_id").cast(pl.Int32),
        pl.col("amount").cast(pl.Decimal(10, 2)),
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

        if written == 0:
            logger.info(f"No new rows. Sleeping {POLL_INTERVAL_S}s …")
        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    silver_flow()
