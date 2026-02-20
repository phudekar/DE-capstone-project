"""Tests for quality scoring module."""

from data_quality.scoring import QualityScore, compute_quality_score


class TestQualityScore:
    def test_perfect_score(self):
        score = QualityScore(completeness=1.0, accuracy=1.0, freshness=1.0, uniqueness=1.0)
        assert score.overall == 1.0
        assert score.grade == "A"

    def test_grade_a_boundary(self):
        score = QualityScore(completeness=0.95, accuracy=0.95, freshness=0.95, uniqueness=0.95)
        assert score.overall >= 0.95
        assert score.grade == "A"

    def test_grade_b(self):
        score = QualityScore(completeness=0.90, accuracy=0.90, freshness=0.80, uniqueness=0.80)
        assert score.grade == "B"

    def test_grade_c(self):
        score = QualityScore(completeness=0.80, accuracy=0.80, freshness=0.70, uniqueness=0.70)
        assert score.grade == "C"

    def test_grade_d(self):
        score = QualityScore(completeness=0.50, accuracy=0.50, freshness=0.50, uniqueness=0.50)
        assert score.grade == "D"

    def test_grade_f(self):
        score = QualityScore(completeness=0.3, accuracy=0.3, freshness=0.0, uniqueness=0.0)
        assert score.grade == "F"


class TestComputeQualityScore:
    def test_all_clean(self):
        score = compute_quality_score(total_rows=1000)
        assert score.overall == 1.0
        assert score.grade == "A"

    def test_with_nulls(self):
        score = compute_quality_score(total_rows=100, null_count=10)
        assert score.completeness == 0.9
        assert score.accuracy == 1.0

    def test_with_invalids(self):
        score = compute_quality_score(total_rows=100, invalid_count=20)
        assert score.accuracy == 0.8

    def test_with_duplicates(self):
        score = compute_quality_score(total_rows=100, duplicate_count=5)
        assert score.uniqueness == 0.95

    def test_stale_data(self):
        score = compute_quality_score(total_rows=100, within_freshness_sla=False)
        assert score.freshness == 0.0
        assert score.overall < 1.0

    def test_empty_dataset(self):
        score = compute_quality_score(total_rows=0)
        assert score.completeness == 1.0
        assert score.overall == 1.0
