"""
Validation utilities for tabular and spatial inputs.

Uses `pydantic` models to enforce schema contracts for:
- survey participation data
- census demographic data
- park facility data
- geographic boundary attributes
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, validator


class SurveyRecord(BaseModel):
    region_id: str = Field(..., description="Geographic ID (county or tract).")
    population: int = Field(..., ge=0)
    participants: int = Field(..., ge=0)

    @validator("participants")
    def participants_le_population(cls, v: int, values: dict) -> int:  # type: ignore[override]
        pop = values.get("population")
        if pop is not None and v > pop:
            raise ValueError("participants cannot exceed population")
        return v


class CensusRecord(BaseModel):
    region_id: str
    total_population: int = Field(..., ge=0)
    median_income: Optional[float]
    poverty_rate: Optional[float]


class FacilityRecord(BaseModel):
    facility_id: str
    region_id: Optional[str]
    facility_type: str
    capacity: Optional[float]


class BoundaryRecord(BaseModel):
    region_id: str
    name: Optional[str]


class M4DemandRecord(BaseModel):
    """One row of activity-level demand (D8/M4 long layout): region_id, activity, demand >= 0."""
    region_id: str = Field(..., min_length=1)
    activity: str = Field(..., min_length=1)
    demand: float = Field(..., ge=0)


def validate_dataframe(df: pd.DataFrame, model: type[BaseModel]) -> pd.DataFrame:
    """
    Validate a DataFrame against a pydantic model.

    Returns a cleaned DataFrame with only records that pass validation.
    """
    valid_records = []
    for record in df.to_dict(orient="records"):
        try:
            model(**record)
            valid_records.append(record)
        except ValidationError:
            continue
    return pd.DataFrame(valid_records)

