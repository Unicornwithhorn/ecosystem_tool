from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd

IN_PATH = Path("data/processed/meteo_monthly_1991_2000.csv")
OUT_NORMALS = Path("data/processed/meteo_monthly_normals_1991_2000.csv")
OUT_PEDYA_MONTHLY = Path("data/processed/meteo_pedya_monthly_1991_2000.csv")
OUT_PEDYA_PERIODS = Path("data/processed/meteo_pedya_periods_1991_2000.csv")

EPS = 1e-9

def compute_normals(baseline: pd.DataFrame, ddof: int = 0) -> pd.DataFrame:
    g = baseline.groupby("month", as_index=False)
    out = g.agg(
        t_norm=("t_mean_c", "mean"),
        t_sigma=("t_mean_c", lambda s: float(np.nanstd(s.to_numpy(dtype=float), ddof=ddof))),
        p_norm=("precip_mm", "mean"),
        p_sigma=("precip_mm", lambda s: float(np.nanstd(s.to_numpy(dtype=float), ddof=ddof))),
        n_years=("year", lambda s: int(pd.Series(s).dropna().nunique())),
    )
    out["t_sigma"] = out["t_sigma"].clip(lower=EPS)
    out["p_sigma"] = out["p_sigma"].clip(lower=EPS)
    return out

def attach_pedya(monthly: pd.DataFrame, normals: pd.DataFrame) -> pd.DataFrame:
    df = monthly.merge(normals, on="month", how="left", validate="many_to_one")
    df["t_anom"] = df["t_mean_c"] - df["t_norm"]
    df["p_anom"] = df["precip_mm"] - df["p_norm"]
    df["pedya"] = (df["t_anom"] / df["t_sigma"]) - (df["p_anom"] / df["p_sigma"])
    return df

def add_climate_year_for_winter(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["climate_year"] = out["year"]
    out.loc[out["month"] == 12, "climate_year"] = out.loc[out["month"] == 12, "year"] + 1
    return out

def aggregate_periods(df: pd.DataFrame) -> pd.DataFrame:
    # mean по месяцам периода (не по дням) — для Pedya это обычно ок.
    # Если захочешь весить по n_days — скажи, сделаем.
    periods = [
        ("DJF", (12, 1, 2), True),
        ("MAM", (3, 4, 5), False),
        ("JJA", (6, 7, 8), False),
        ("SON", (9, 10, 11), False),
        ("cold_half_year", (10, 11, 12, 1, 2, 3), True),
        ("warm_half_year", (4, 5, 6, 7, 8, 9), False),
    ]

    base = add_climate_year_for_winter(df)
    rows = []
    for name, months, use_cy in periods:
        sub = base[base["month"].isin(months)].copy()
        if sub.empty:
            continue
        ycol = "climate_year" if use_cy else "year"
        agg = (
            sub.groupby(ycol, as_index=False)
               .agg(period_value=("pedya", "mean"), n_months=("pedya", lambda s: int(pd.Series(s).notna().sum())))
               .rename(columns={ycol: "year"})
        )
        agg["period"] = name
        rows.append(agg)

    if not rows:
        return pd.DataFrame(columns=["year", "period", "period_value", "n_months"])

    return pd.concat(rows, ignore_index=True).sort_values(["period", "year"])

def main():
    if not IN_PATH.exists():
        raise SystemExit(f"Input not found: {IN_PATH.resolve()}")

    df = pd.read_csv(IN_PATH)
    need = {"year", "month", "t_mean_c", "precip_mm"}
    miss = need - set(df.columns)
    if miss:
        raise KeyError(f"Missing columns: {sorted(miss)}. Columns: {list(df.columns)}")

    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype(int)
    df["month"] = pd.to_numeric(df["month"], errors="coerce").astype(int)

    normals = compute_normals(df)
    pedya_monthly = attach_pedya(df, normals)
    pedya_periods = aggregate_periods(pedya_monthly)

    OUT_NORMALS.parent.mkdir(parents=True, exist_ok=True)
    normals.to_csv(OUT_NORMALS, index=False)
    pedya_monthly.to_csv(OUT_PEDYA_MONTHLY, index=False)
    pedya_periods.to_csv(OUT_PEDYA_PERIODS, index=False)

    print("Saved:", OUT_NORMALS)
    print("Saved:", OUT_PEDYA_MONTHLY)
    print("Saved:", OUT_PEDYA_PERIODS)
    print("\nNormals head:\n", normals.head(12))
    print("\nPedya monthly head:\n", pedya_monthly.head(12))
    print("\nPedya periods head:\n", pedya_periods.head(12))

if __name__ == "__main__":
    main()