"""
test_structured_output.py — Test structured output via client.beta.chat.completions.parse.
"""

import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from openai import OpenAI
from silver.models import PredictionBatch, CATEGORIES

SAMPLE_ROWS = [
    {"vendor": "WHOLEFDS MKT 0483",        "amount": 47.23,  "transaction_type": "PURCHASE"},
    {"vendor": "NETFLIX.COM",               "amount": 15.49,  "transaction_type": "PURCHASE"},
    {"vendor": "SHELL OIL 57442391",        "amount": 62.10,  "transaction_type": "PURCHASE"},
    {"vendor": "CHIPOTLE MEXICAN GRILL",    "amount": 13.75,  "transaction_type": "PURCHASE"},
    {"vendor": "USPS PO 4871130016 P",      "amount":  8.40,  "transaction_type": "PURCHASE"},
]

client = OpenAI(
    base_url=os.getenv("LITELLM_BASE_URL", "http://10.0.0.81:4000/v1"),
    api_key=os.getenv("LITELLM_KEY", ""),
)

MODEL = os.getenv("LLM_MODEL", "nemomoo")

items = "\n".join(
    f"{r['vendor']} | ${r['amount']} | {r['transaction_type']}"
    for r in SAMPLE_ROWS
)

system_prompt = (
    "You are a financial transaction classifier. "
    f"Classify each of the {len(SAMPLE_ROWS)} transactions below IN EXACT ORDER into the schema.\n"
    f"Allowed categories: {', '.join(CATEGORIES)}."
)

print(f"Testing model: {MODEL} via beta.chat.completions.parse()")
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
    
    batch: PredictionBatch = response.choices[0].message.parsed
    print(f"Model used: {response.model}")
    print(f"Parsed {len(batch.predictions)} items into PredictionBatch Pydantic model:")
    print("-" * 60)
    for i, (row, p) in enumerate(zip(SAMPLE_ROWS, batch.predictions)):
        print(f"  [{i+1}] {row['vendor']!r:<30} → {p.v!r:<15} (conf={p.c:.2f})")

except Exception as exc:
    print(f"❌ Failed: {exc}")
    sys.exit(1)
