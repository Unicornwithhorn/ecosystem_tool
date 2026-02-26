from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd

# чтобы импорт core работал при запуске как файла (на всякий случай)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from core.scenario_runner import run_scenario  # noqa: E402


OUT_CSV = PROJECT_ROOT / "data" / "processed" / "grid_eco_vs_climate_results.csv"


def run_one(trait_scale: str, eco_metric: str, period: str, lag: int, window: int, year_between=None):
    filters = {}
    if year_between:
        filters = {"year": {"between": list(year_between)}}

    spec = SimpleNamespace(
        name=f"grid_{trait_scale}_{eco_metric}_{period}_lag{lag}_win{window}",
        analysis="eco_vs_climate",
        filters=filters,
        trait_scale=trait_scale,
        eco_metric=eco_metric,
        period=period,
        lag=lag,
        window=window,
        plot=None,  # в гриде картинки не строим
    )
    df, _ = run_scenario(spec)

    n = len(df)
    pearson = float(df["pearson_r"].iloc[0]) if n > 0 and "pearson_r" in df.columns else float("nan")
    spearman = float(df["spearman_rho"].iloc[0]) if n > 0 and "spearman_rho" in df.columns else float("nan")

    years = sorted(df["year"].unique().tolist()) if n > 0 and "year" in df.columns else []
    year_min = years[0] if years else None
    year_max = years[-1] if years else None

    return {
        "trait_scale": trait_scale,
        "eco_metric": eco_metric,
        "period": period,
        "lag": lag,
        "window": window,
        "n": n,
        "year_min": year_min,
        "year_max": year_max,
        "pearson_r": pearson,
        "spearman_rho": spearman,
        "abs_pearson_r": abs(pearson) if pd.notna(pearson) else float("nan"),
        "abs_spearman_rho": abs(spearman) if pd.notna(spearman) else float("nan"),
    }


def main():
    # что гоняем
    trait_scale = "M"  # влажность (у тебя в таблице это M)
    eco_metrics = ["cwm", "sigma"]  # попробуем среднее и разброс
    periods = ["JJA", "warm_half_year", "MAM", "DJF", "cold_half_year"]
    lags = [0, 1, 2]
    windows = [1, 2, 3]

    # если хочешь жёстко ограничить годами полевых данных:
    year_between = (2009, 2019)

    rows = []
    total = len(eco_metrics) * len(periods) * len(lags) * len(windows)
    k = 0

    for eco_metric in eco_metrics:
        for period in periods:
            for lag in lags:
                for window in windows:
                    k += 1
                    try:
                        res = run_one(
                            trait_scale=trait_scale,
                            eco_metric=eco_metric,
                            period=period,
                            lag=lag,
                            window=window,
                            year_between=year_between,
                        )
                        rows.append(res)
                        print(f"[{k:>3}/{total}] OK  {eco_metric:>5} {trait_scale} vs Pedya({period}) lag={lag} win={window} "
                              f"n={res['n']} r={res['pearson_r']:.3f} rho={res['spearman_rho']:.3f}")
                    except Exception as e:
                        print(f"[{k:>3}/{total}] ERR {eco_metric:>5} {trait_scale} vs Pedya({period}) lag={lag} win={window}: {e}")

    out = pd.DataFrame(rows)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    print("\nSaved:", OUT_CSV)
    # топ-10 по |r|
    top = out.dropna(subset=["abs_pearson_r"]).sort_values("abs_pearson_r", ascending=False).head(10)
    print("\nTOP-10 by |pearson_r|:\n", top[[
        "trait_scale", "eco_metric", "period", "lag", "window", "n", "pearson_r", "spearman_rho"
    ]].to_string(index=False))


if __name__ == "__main__":
    main()