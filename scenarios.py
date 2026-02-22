"""
High-level analysis scenarios using analysis_engine.
"""

from core.analysis_engine import (
    build_merged_from_raw,
    aggregate,
    metric_mean,
    metric_richness,
)

merged = build_merged_from_raw()

# пример сценария
result = aggregate(
    merged,
    filters={"impact_type": {"contains": "верхний бьеф"}},
    groupby=["year"],
    metrics=[
        metric_mean("projective_cover", "mean_projective_cover"),
        metric_richness(),
    ],
)

print(result)
