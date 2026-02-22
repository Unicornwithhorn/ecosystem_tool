from __future__ import annotations

from pathlib import Path
import pandas as pd


IN_PATH = Path("data/processed/meteo_daily_1991_2000.csv")
OUT_PATH = Path("data/processed/meteo_monthly_1991_2000.csv")


def main() -> None:
    if not IN_PATH.exists():
        raise SystemExit(f"Input not found: {IN_PATH.resolve()}")

    df = pd.read_csv(IN_PATH)

    # date -> datetime
    if "date" not in df.columns:
        raise KeyError(f"'date' column not found. Columns: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    bad = df["date"].isna().sum()
    if bad:
        raise ValueError(f"Bad/empty dates: {bad}")

    # На всякий: number иногда остаётся, если ты не дропнул его в daily-скрипте
    if "number" in df.columns:
        # он должен быть либо весь 0, либо один уровень; просто убираем
        df = df.drop(columns=["number"])

    # Проверим обязательные поля
    need = {"t_mean_c", "precip_mm"}
    miss = need - set(df.columns)
    if miss:
        raise KeyError(f"Missing columns: {sorted(miss)}. Columns: {list(df.columns)}")

    # year/month
    df["year"] = df["date"].dt.year.astype(int)
    df["month"] = df["date"].dt.month.astype(int)

    # Агрегация:
    # - t_mean_c: средняя за месяц
    # - precip_mm: сумма за месяц
    monthly = (
        df.groupby(["year", "month"], as_index=False)
          .agg(
              t_mean_c=("t_mean_c", "mean"),
              precip_mm=("precip_mm", "sum"),
              n_days=("date", "count"),
          )
          .sort_values(["year", "month"])
    )

    # Быстрые sanity-checks
    # 1) n_days обычно 28-31
    if monthly["n_days"].min() < 28 or monthly["n_days"].max() > 31:
        print("[WARN] n_days out of expected range. min/max:", monthly["n_days"].min(), monthly["n_days"].max())

    # 2) температура и осадки не должны быть NaN
    for col in ["t_mean_c", "precip_mm"]:
        if monthly[col].isna().any():
            raise ValueError(f"NaNs found in {col}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    monthly.to_csv(OUT_PATH, index=False)

    print("Saved:", OUT_PATH)
    print(monthly.head(12))
    print(
        "Months:", len(monthly),
        "| years:", monthly["year"].min(), "-", monthly["year"].max()
    )


if __name__ == "__main__":
    main()