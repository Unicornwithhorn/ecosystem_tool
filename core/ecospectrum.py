import numpy as np
import pandas as pd


def weighted_quantile(x: np.ndarray, w: np.ndarray, q: float) -> float:
    """
    Взвешенный квантиль q для значений x с весами w.
    q в [0,1].
    """
    if len(x) == 0:
        return np.nan

    order = np.argsort(x)
    x_sorted = x[order]
    w_sorted = w[order]

    cum_w = np.cumsum(w_sorted)
    total_w = cum_w[-1]
    if total_w <= 0:
        return np.nan

    cutoff = q * total_w
    idx = np.searchsorted(cum_w, cutoff, side="left")
    idx = min(idx, len(x_sorted) - 1)
    return float(x_sorted[idx])


def compute_ecospectrum_stats(
    df_one: pd.DataFrame,
    trait_col: str = "M",
    weight_col: str = "w",
    q_low: float = 0.05,
    q_high: float = 0.95,
) -> dict:
    """
    Считает параметры экологического спектра для ОДНОГО описания.
    """
    d = df_one[[trait_col, weight_col]].copy()

    d = d.dropna(subset=[trait_col, weight_col])
    d = d[d[weight_col] > 0]

    n = len(d)
    if n == 0:
        return {
            "n_rows_used": 0,
            "sum_w": 0.0,
            "cwm": np.nan,
            "sigma": np.nan,
            "w_median": np.nan,
            "w_min": np.nan,
            "w_max": np.nan,
        }

    x = d[trait_col].to_numpy(dtype=float)
    w = d[weight_col].to_numpy(dtype=float)

    sum_w = float(w.sum())
    if sum_w <= 0:
        return {
            "n_rows_used": n,
            "sum_w": sum_w,
            "cwm": np.nan,
            "sigma": np.nan,
            "w_median": np.nan,
            "w_min": np.nan,
            "w_max": np.nan,
        }

    cwm = float(np.sum(w * x) / sum_w)
    var = float(np.sum(w * (x - cwm) ** 2) / sum_w)
    sigma = float(np.sqrt(var))

    w_median = weighted_quantile(x, w, 0.50)
    w_min = weighted_quantile(x, w, q_low)
    w_max = weighted_quantile(x, w, q_high)

    return {
        "n_rows_used": int(n),
        "sum_w": sum_w,
        "cwm": cwm,
        "sigma": sigma,
        "w_median": w_median,
        "w_min": w_min,
        "w_max": w_max,
    }
def compute_ecospectrum_by_description(
    df: pd.DataFrame,
    trait_col: str = "M",
    weight_col: str = "w",
    q_low: float = 0.05,
    q_high: float = 0.95,
    id_col: str = "description_id",
) -> pd.DataFrame:
    """
    Считает экоспектр-метрики для каждого description_id.
    df должен быть на уровне видов и содержать колонки:
      - description_id
      - trait_col (например "M")
      - weight_col (например "w")

    Возвращает DataFrame:
      description_id, n_rows_used, sum_w, cwm, sigma, w_median, w_min, w_max
    """

    def _calc(group: pd.DataFrame) -> pd.Series:
        stats = compute_ecospectrum_stats(
            group,
            trait_col=trait_col,
            weight_col=weight_col,
            q_low=q_low,
            q_high=q_high,
        )
        return pd.Series(stats)

    out = (
        df.groupby(id_col, sort=False)
          .apply(_calc)
          .reset_index()
    )
    return out
