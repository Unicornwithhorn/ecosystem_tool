import pandas as pd
import matplotlib
matplotlib.use("Agg")  # no GUI, save to file only
import matplotlib.pyplot as plt
from pathlib import Path

from core.analysis_engine import (
    load_processed,
    aggregate_descriptions,
    metric_mean,
)

def ensure_year_column(df):
    out = df.reset_index()
    if "year" in out.columns:
        return out
    for cand in ("index", "level_0"):
        if cand in out.columns:
            return out.rename(columns={cand: "year"})
    raise KeyError(f"Cannot find 'year' after reset_index(). Columns: {list(out.columns)}")

def main():
    df = load_processed()
    print("Total rows:", len(df))

    # --- 1) filter only Kostinka and show what's inside ---
    df_k = df[df["source_file"].astype("string").str.contains("Костинка", case=False, na=False)].copy()
    print("Rows with source_file contains 'Костинка':", len(df_k))
    if df_k.empty:
        print("❌ No rows matched 'Костинка' in source_file. Check spelling in source_file values.")
        print("Unique source_file examples:", df["source_file"].dropna().unique()[:20])
        return

    # show unique levels
    if "geomorph_level" in df_k.columns:
        levels = sorted(df_k["geomorph_level"].dropna().astype(str).unique())
        print("geomorph_level unique (Kostinka):", levels[:30])
    else:
        print("⚠️ column geomorph_level not found")

    # --- 2) choose the correct way to filter low floodplain ---
    # Primary: geomorph_level == 'НП' (after normalization)
    def is_np(series: pd.Series) -> pd.Series:
        return (
            series.astype("string")
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
            .str.upper()
            .eq("НП")
        )

    filters = {
        "source_file": {"contains": "Костинка"},
    }

    # Try geomorph_level first, fallback to geomorphology
    if "geomorph_level" in df_k.columns and is_np(df_k["geomorph_level"]).any():
        filters["geomorph_level"] = lambda s: is_np(s)
        print("✅ Using geomorph_level for 'НП'")
    elif "geomorphology" in df_k.columns and df_k["geomorphology"].astype("string").str.contains("НП", na=False).any():
        # more permissive fallback
        filters["geomorphology"] = {"contains": "НП"}
        print("✅ Using geomorphology contains 'НП' as fallback")
    else:
        print("❌ Could not find low floodplain marker 'НП' in Kostinka data.")
        print("Sample geomorph_level:", df_k.get("geomorph_level", pd.Series(dtype="string")).dropna().head(10).tolist())
        print("Sample geomorphology:", df_k.get("geomorphology", pd.Series(dtype="string")).dropna().head(10).tolist())
        return

    # --- 3) aggregate ---
    result = aggregate_descriptions(
        df,
        filters=filters,
        groupby=["year"],
        metrics=[metric_mean("projective_cover", "mean_projective_cover")],
    )

    if result.empty:
        print("❌ Still empty after filters. Something is off with year/projective_cover presence.")
        return

    result = ensure_year_column(result).sort_values("year").reset_index(drop=True)
    print(result)

    # --- 4) plot ---
    x = pd.to_numeric(result["year"], errors="coerce")
    y = result["mean_projective_cover"]

    plt.figure()
    plt.plot(x, y, marker="o")
    plt.xlabel("Year")
    plt.ylabel("Mean projective cover (%)")
    plt.title("Kostinka: low floodplain (НП) projective cover trend")
    plt.grid(True)

    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    OUT_DIR = PROJECT_ROOT / "data" / "processed"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    out_path = OUT_DIR / "plot_kostinka_np_cover_trend.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved plot: {out_path}")



if __name__ == "__main__":
    main()
