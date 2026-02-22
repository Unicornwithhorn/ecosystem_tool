"""
TEMPLATE: Time-series analysis at DESCRIPTION level

How to use:
1. Copy this file
2. Rename it (e.g. scenario_kostinka_np_cover_trend.py)
3. Edit:
   - SCENARIO_NAME
   - FILTERS
   - METRICS
   - GROUPBY
"""

from core.analysis_engine import (
    load_processed,
    aggregate_descriptions,
)

# =====================
# SCENARIO CONFIG
# =====================

SCENARIO_NAME = "TEMPLATE_time_series"

# --- Filters are "layered" ---
# Each condition reduces the dataset
FILTERS = {
    # Examples (uncomment & edit):
    # "source_file": {"contains": "Костинка"},
    # "geomorph_level": "НП",
    # "impact_type": "верхний бьеф",
    # "year": {"between": (2010, 2020)},
}

# --- Grouping axis ---
GROUPBY = ["year"]

# --- Metrics (DESCRIPTION-level!) ---
METRICS = [
    # Examples:
    # metric_mean("projective_cover", "mean_projective_cover"),
    # metric_count("description_id", "n_descriptions"),
]

# =====================
# INTERNAL HELPERS
# =====================

def ensure_year_column(df):
    """
    Guarantees a real 'year' column even if groupby returned index.
    """
    out = df.reset_index()

    if "year" in out.columns:
        return out

    for cand in ("index", "level_0"):
        if cand in out.columns:
            return out.rename(columns={cand: "year"})

    raise KeyError(
        f"Cannot find 'year' after reset_index(). Columns: {list(out.columns)}"
    )

# =====================
# MAIN
# =====================

def main():
    print(f"\n▶ Running scenario: {SCENARIO_NAME}")

    # 1️⃣ Load normalized + merged data
    df = load_processed()
    print(f"  total rows loaded: {len(df)}")

    # 2️⃣ Aggregate (description-level enforced inside)
    result = aggregate_descriptions(
        df,
        filters=FILTERS,
        groupby=GROUPBY,
        metrics=METRICS,
    )

    if result.empty:
        print("⚠️ Result is EMPTY after filters + aggregation")
        return

    # 3️⃣ Ensure proper year column
    result = ensure_year_column(result)

    # 4️⃣ Sort for time-series readability
    if "year" in result.columns:
        result = result.sort_values("year")

    result = result.reset_index(drop=True)

    # 5️⃣ Output
    print("\nResult preview:")
    print(result)

    # Optional: save
    # out = f"data/processed/{SCENARIO_NAME}.csv"
    # result.to_csv(out, index=False, encoding="utf-8")
    # print(f"Saved: {out}")

if __name__ == "__main__":
    main()
