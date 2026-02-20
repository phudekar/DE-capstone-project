"""Quality scoring — weighted 4-dimension score with letter grades."""

from dataclasses import dataclass


@dataclass(frozen=True)
class QualityScore:
    """Weighted quality score across four dimensions."""

    completeness: float  # 0.0–1.0
    accuracy: float
    freshness: float
    uniqueness: float

    WEIGHT_COMPLETENESS = 0.30
    WEIGHT_ACCURACY = 0.30
    WEIGHT_FRESHNESS = 0.20
    WEIGHT_UNIQUENESS = 0.20

    @property
    def overall(self) -> float:
        return (
            self.completeness * self.WEIGHT_COMPLETENESS
            + self.accuracy * self.WEIGHT_ACCURACY
            + self.freshness * self.WEIGHT_FRESHNESS
            + self.uniqueness * self.WEIGHT_UNIQUENESS
        )

    @property
    def grade(self) -> str:
        score = self.overall
        if score >= 0.95:
            return "A"
        if score >= 0.85:
            return "B"
        if score >= 0.70:
            return "C"
        if score >= 0.50:
            return "D"
        return "F"


def compute_quality_score(
    total_rows: int,
    null_count: int = 0,
    invalid_count: int = 0,
    duplicate_count: int = 0,
    within_freshness_sla: bool = True,
) -> QualityScore:
    """Compute a QualityScore from raw counts.

    Args:
        total_rows: Total number of rows in the dataset.
        null_count: Number of rows with null values in required columns.
        invalid_count: Number of rows failing accuracy/business-rule checks.
        duplicate_count: Number of duplicate rows.
        within_freshness_sla: Whether data is within the freshness SLA.
    """
    if total_rows == 0:
        return QualityScore(
            completeness=1.0,
            accuracy=1.0,
            freshness=1.0 if within_freshness_sla else 0.0,
            uniqueness=1.0,
        )

    completeness = 1.0 - (null_count / total_rows)
    accuracy = (total_rows - invalid_count) / total_rows
    freshness = 1.0 if within_freshness_sla else 0.0
    uniqueness = 1.0 - (duplicate_count / total_rows)

    return QualityScore(
        completeness=max(0.0, min(1.0, completeness)),
        accuracy=max(0.0, min(1.0, accuracy)),
        freshness=freshness,
        uniqueness=max(0.0, min(1.0, uniqueness)),
    )
