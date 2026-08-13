"""
models.py — Pydantic schemas for Silver layer LLM classification.

CategoryLiteral is the single source of truth for allowed categories.
CATEGORIES is derived from it via get_args() so they are always in sync.
"""

from __future__ import annotations

from typing import Literal, get_args

from pydantic import BaseModel, Field, field_validator

CategoryLiteral = Literal[
    "Groceries", "Dining", "Subscriptions", "Travel", "Gas",
    "Shopping", "Healthcare", "Entertainment", "Utilities",
    "ATM/Cash", "Transfer", "Credit/Refund", "Other",
]

CATEGORIES: tuple[str, ...] = get_args(CategoryLiteral)


class Prediction(BaseModel):
    """Single-item classification — position is identity, no index field."""

    v: CategoryLiteral = Field(..., description="Vendor category")
    c: float = Field(..., ge=0.0, le=1.0, description="Confidence score 0–1")

    @field_validator("v", mode="before")
    @classmethod
    def normalise_category(cls, val: str) -> str:
        """Case-insensitive, whitespace-stripped match against allowed categories."""
        stripped = str(val).strip()
        for cat in CATEGORIES:
            if cat.lower() == stripped.lower():
                return cat
        raise ValueError(f"'{val}' is not an allowed category")


class PredictionBatch(BaseModel):
    """Top-level wrapper — json_schema mode requires an object, not a bare array."""

    predictions: list[Prediction]
