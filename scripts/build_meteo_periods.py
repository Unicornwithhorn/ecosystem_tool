from __future__ import annotations

from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED = PROJECT_ROOT / "data" / "processed"

# входы (переименуй при желании)
MONTHLY_CSV = PROCESSED / "meteo_monthly_1991_2020.csv"
PEDYA_MONTHLY_CSV = PROCESSED / "meteo_pedya_monthly_1991_2020.csv"

# выход
OUT_PERIODS_CSV = PROCESSED / "meteo_periods_1991_2020.csv"


def _require_cols(df: pd.DataFrame, required: set[str], name: str):
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"{name} missing columns: {sorted(missing)}; found: {list(df.columns)}")


def _normalize_monthly(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df = df.dropna(subset=["year", "month"]).copy()
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    return df


def _period_rows(df_monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Делает таблицу (year, month, period, period_year) для всех периодов.
    Периоды:
      - DJF (Dec(t-1)-Feb(t)) -> period_year = t
      - MAM (Mar-May)         -> period_year = t
      - JJA (Jun-Aug)         -> period_year = t
      - SON (Sep-Nov)         -> period_year = t
      - cold_half_year (Oct(t-1)-Mar(t)) -> period_year = t
      - warm_half_year (Apr-Sep)         -> period_year = t
    """
    base = df_monthly[["year", "month"]].drop_duplicates().copy()

    def add_period(period: str, months: set[int], shift_months: set[int] | None = None) -> pd.DataFrame:
        d = base[base["month"].isin(months)].copy()
        d["period"] = period
        d["period_year"] = d["year"]
        if shift_months:
            mask = d["month"].isin(shift_months)
            d.loc[mask, "period_year"] = d.loc[mask, "period_year"] + 1
        return d

    out = []
    out.append(add_period("DJF", {12, 1, 2}, shift_months={12}))
    out.append(add_period("MAM", {3, 4, 5}))
    out.append(add_period("JJA", {6, 7, 8}))
    out.append(add_period("SON", {9, 10, 11}))
    out.append(add_period("cold_half_year", {10, 11, 12, 1, 2, 3}, shift_months={10, 11, 12}))
    out.append(add_period("warm_half_year", {4, 5, 6, 7, 8, 9}))

    return pd.concat(out, ignore_index=True)


def main():
    if not MONTHLY_CSV.exists():
        raise FileNotFoundError(f"Missing: {MONTHLY_CSV}")
    if not PEDYA_MONTHLY_CSV.exists():
        raise FileNotFoundError(f"Missing: {PEDYA_MONTHLY_CSV}")

    met = pd.read_csv(MONTHLY_CSV)
    _require_cols(met, {"year", "month", "t_mean_c", "precip_mm", "n_days"}, "meteo_monthly")
    met = _normalize_monthly(met)
    met["t_mean_c"] = pd.to_numeric(met["t_mean_c"], errors="coerce")
    met["precip_mm"] = pd.to_numeric(met["precip_mm"], errors="coerce")
    met["n_days"] = pd.to_numeric(met["n_days"], errors="coerce")
    met = met.dropna(subset=["t_mean_c", "precip_mm", "n_days"]).copy()

    ped = pd.read_csv(PEDYA_MONTHLY_CSV)
    # поддержим оба варианта колонок: pedya / pedya_value
    if "pedya" not in ped.columns and "pedya_value" in ped.columns:
        ped = ped.rename(columns={"pedya_value": "pedya"})
    _require_cols(ped, {"year", "month", "pedya"}, "meteo_pedya_monthly")
    ped = _normalize_monthly(ped)
    ped["pedya"] = pd.to_numeric(ped["pedya"], errors="coerce")
    ped = ped.dropna(subset=["pedya"]).copy()

    # merge monthly climate
    df = met.merge(ped[["year", "month", "pedya"]], on=["year", "month"], how="left")

    # map months to periods + shifted period_year
    mapping = _period_rows(df)
    df2 = df.merge(mapping, on=["year", "month"], how="inner")

    # агрегирование по периодам
    # temp: взвешенное по числу дней среднее
    def wavg_temp(g: pd.DataFrame) -> float:
        w = g["n_days"].astype(float)
        x = g["t_mean_c"].astype(float)
        return float((x * w).sum() / w.sum()) if w.sum() > 0 else float("nan")

    out = (
        df2.groupby(["period_year", "period"], as_index=False)
        .apply(
            lambda g: pd.Series(
                {
                    "t_mean_c": wavg_temp(g),
                    "precip_mm": float(g["precip_mm"].sum()),
                    "pedya": float(g["pedya"].mean()),  # среднее индекса по месяцам периода
                    "n_months": int(g["month"].nunique()),
                    "n_days": int(g["n_days"].sum()),
                }
            )
        )
        .reset_index(drop=True)
        .rename(columns={"period_year": "year"})
        .sort_values(["period", "year"])
    )

    OUT_PERIODS_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_PERIODS_CSV, index=False)
    print("Saved:", OUT_PERIODS_CSV)
    print(out.head(10).to_string(index=False))


if __name__ == "__main__":
    main()