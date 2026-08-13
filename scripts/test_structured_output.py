"""
test_structured_output.py — Probe structured output (response_format) support.

Tests whether the current LiteLLM endpoint + model supports
`client.beta.chat.completions.parse()` with a Pydantic schema.

Usage:
    # Load env vars first (same pattern as other test scripts)
    source .env && uv run python scripts/test_structured_output.py

What this checks:
    1. Basic .parse() call succeeds (no API-level rejection).
    2. .parsed is a populated PredictionBatch, not None.
    3. Every prediction has a valid category and a 0–1 confidence score.
    4. The number of predictions matches the number of input rows.
"""

import os
import sys
from typing import Literal

from openai import OpenAI
from pydantic import BaseModel, Field, field_validator

# ── Inline schema (mirrors what silver/models.py will define) ─────────────────

CATEGORIES = [
    "Groceries", "Dining", "Subscriptions", "Travel", "Gas",
    "Shopping", "Healthcare", "Entertainment", "Utilities",
    "ATM/Cash", "Transfer", "Credit/Refund", "Other",
]

CategoryLiteral = Literal[
    "Groceries", "Dining", "Subscriptions", "Travel", "Gas",
    "Shopping", "Healthcare", "Entertainment", "Utilities",
    "ATM/Cash", "Transfer", "Credit/Refund", "Other",
]


class Prediction(BaseModel):
    """Single-item classification — position is identity, no index field."""
    v: CategoryLiteral = Field(..., description="Vendor category")
    c: float = Field(..., ge=0.0, le=1.0, description="Confidence score 0–1")

    @field_validator("v", mode="before")
    @classmethod
    def normalise_category(cls, val: str) -> str:
        stripped = str(val).strip()
        for cat in CATEGORIES:
            if cat.lower() == stripped.lower():
                return cat
        raise ValueError(f"'{val}' is not an allowed category")


class PredictionBatch(BaseModel):
    """Top-level wrapper — json_schema mode requires an object, not a bare array."""
    predictions: list[Prediction]


# ── Sample batch (real-ish vendor strings) ────────────────────────────────────

SAMPLE_ROWS = [
    {"vendor": "WHOLEFDS MKT 0483",        "amount": 47.23,  "transaction_type": "PURCHASE"},
    {"vendor": "NETFLIX.COM",               "amount": 15.49,  "transaction_type": "PURCHASE"},
    {"vendor": "SHELL OIL 57442391",        "amount": 62.10,  "transaction_type": "PURCHASE"},
    {"vendor": "CHIPOTLE MEXICAN GRILL",    "amount": 13.75,  "transaction_type": "PURCHASE"},
    {"vendor": "USPS PO 4871130016 P",      "amount":  8.40,  "transaction_type": "PURCHASE"},
]

# ── Client ────────────────────────────────────────────────────────────────────

client = OpenAI(
    base_url=os.getenv("LITELLM_BASE_URL", "http://10.0.0.81:4000/v1"),
    api_key=os.getenv("LITELLM_KEY", ""),
)

MODEL = os.getenv("LLM_MODEL", "openrouter/openrouter/free")

# ── Build prompt ──────────────────────────────────────────────────────────────

items = "\n".join(
    f"{r['vendor']} | ${r['amount']} | {r['transaction_type']}"
    for r in SAMPLE_ROWS
)

system_prompt = (
    "You are a financial transaction classifier. "
    f"Classify each of the {len(SAMPLE_ROWS)} transactions below IN ORDER. "
    "Return one prediction per line in the same order as the input. "
    f"Allowed categories: {', '.join(CATEGORIES)}."
)

# ── Fire the request ──────────────────────────────────────────────────────────

print(f"Model  : {MODEL}")
print(f"Schema : {PredictionBatch.model_json_schema()}")
print("-" * 60)
print("Input transactions:")
for r in SAMPLE_ROWS:
    print(f"  {r['vendor']} | ${r['amount']} | {r['transaction_type']}")
print("-" * 60)

try:
    response = client.beta.chat.completions.parse(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": items},
        ],
        response_format=PredictionBatch,
        temperature=0.0,
    )
except Exception as exc:
    print(f"\n❌  API call failed: {exc}")
    print("\nConclusion: this model does NOT support structured output.")
    print("Full-batch failures will fall back to bronze retry — safe degradation path.")
    sys.exit(1)

# ── Inspect result ────────────────────────────────────────────────────────────

msg = response.choices[0].message

if msg.parsed is None:
    print(f"\n❌  .parsed is None — model refused or returned unparseable output.")
    print(f"    refusal : {msg.refusal}")
    print(f"    content : {msg.content}")
    sys.exit(1)

batch: PredictionBatch = msg.parsed

print(f"\n✅  .parse() succeeded — model returned a valid PredictionBatch.")
print(f"    Model used : {response.model}")
print(f"    Predictions: {len(batch.predictions)} (expected {len(SAMPLE_ROWS)})")
print()

# ── Validation checks ─────────────────────────────────────────────────────────

all_ok = True

if len(batch.predictions) != len(SAMPLE_ROWS):
    print(f"⚠️   Length mismatch: got {len(batch.predictions)}, expected {len(SAMPLE_ROWS)}")
    all_ok = False

for i, (row, pred) in enumerate(zip(SAMPLE_ROWS, batch.predictions)):
    ok_cat   = pred.v in CATEGORIES
    ok_conf  = 0.0 <= pred.c <= 1.0
    status   = "✅" if (ok_cat and ok_conf) else "❌"
    print(f"  [{i+1}] {status}  {row['vendor']!r:<30}  →  {pred.v!r:<20}  conf={pred.c:.2f}")
    if not ok_cat:
        print(f"        ❌ Bad category: {pred.v!r}")
        all_ok = False
    if not ok_conf:
        print(f"        ❌ Confidence out of range: {pred.c}")
        all_ok = False

print()
if all_ok:
    print("✅  All checks passed — structured output is compatible with this model.")
    print("    Safe to proceed with client.beta.chat.completions.parse() in silver_flow.py.")
else:
    print("❌  One or more checks failed — review output above.")
    sys.exit(1)
