# core/panel_api.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf


@dataclass(frozen=True)
class PanelSpec:
    scale: str            # "N", "M", ...
    eco_metric: str       # "sigma" or "cwm" (и т.д., если захочешь)
    climate_var: str      # "pedya" | "precip_mm" | "t_mean_c"
    period: str           # "DJF" etc
    lag: int              # 0..2
    window: int           # 1..3


def _panel_path(scale: str, eco_metric: str) -> str:
    # согласуем с тем, как ты сохраняешь panel-eco
    return f"data/processed/panel_eco_{scale}_{eco_metric}.csv"


def _build_climate_signal(
    meteo: pd.DataFrame,
    climate_var: str,
    period: str,
    lag: int,
    window: int,
) -> pd.DataFrame:
    clim = (
        meteo.loc[meteo["period"] == period, ["year", climate_var]]
        .rename(columns={climate_var: "clim"})
        .sort_values("year")
        .reset_index(drop=True)
    )

    # window затем lag (как мы делали)
    clim["clim"] = clim["clim"].rolling(window).mean()
    clim["clim"] = clim["clim"].shift(lag)
    return clim


def fit_panel_ols_cluster(
    spec: PanelSpec,
    include_controls: bool = True,
) -> pd.DataFrame:
    """
    Возвращает 1 строку с slopes + deltas + p-values + r2.
    """
    panel = pd.read_csv(_panel_path(spec.scale, spec.eco_metric))
    meteo = pd.read_csv("data/processed/meteo_periods_1991_2020.csv")

    clim = _build_climate_signal(
        meteo=meteo,
        climate_var=spec.climate_var,
        period=spec.period,
        lag=spec.lag,
        window=spec.window,
    )

    df = panel.merge(clim, on="year", how="left")

    need = ["eco", "clim", "afforestation", "site_id"]
    if include_controls:
        need += ["geomorph_level", "impact_type"]

    df = df.dropna(subset=need).copy()

    # глобальное центрирование климата
    df["clim_c"] = df["clim"] - df["clim"].mean()

    if include_controls:
        formula = "eco ~ clim_c * C(afforestation) + C(geomorph_level) + C(impact_type)"
    else:
        formula = "eco ~ clim_c * C(afforestation)"

    model = smf.ols(formula, data=df).fit(
        cov_type="cluster",
        cov_kwds={"groups": df["site_id"]},
    )

    params = model.params
    pvals = model.pvalues

    slope_meadow = float(params.get("clim_c", np.nan))
    delta_sparse = float(params.get("clim_c:C(afforestation)[T.1]", 0.0))
    delta_forest = float(params.get("clim_c:C(afforestation)[T.2]", 0.0))

    out = pd.DataFrame([{
        "scale": spec.scale,
        "eco_metric": spec.eco_metric,
        "climate_var": spec.climate_var,
        "period": spec.period,
        "lag": spec.lag,
        "window": spec.window,
        "n_obs": int(len(df)),

        "beta_meadow": slope_meadow,
        "beta_sparse": slope_meadow + delta_sparse,
        "beta_forest": slope_meadow + delta_forest,

        "delta_sparse_meadow": delta_sparse,
        "delta_forest_meadow": delta_forest,

        "p_clim": float(pvals.get("clim_c", np.nan)),
        "p_sparse_interaction": float(pvals.get("clim_c:C(afforestation)[T.1]", np.nan)),
        "p_forest_interaction": float(pvals.get("clim_c:C(afforestation)[T.2]", np.nan)),

        "r2": float(model.rsquared),
    }])

    return out


def run_panel_batch(
    scale: str,
    eco_metric: str,
    climate_vars: Iterable[str],
    periods: Iterable[str],
    lags: Iterable[int],
    windows: Iterable[int],
    include_controls: bool = True,
) -> pd.DataFrame:
    rows = []
    for cv in climate_vars:
        for p in periods:
            for w in windows:
                for lg in lags:
                    spec = PanelSpec(
                        scale=scale,
                        eco_metric=eco_metric,
                        climate_var=cv,
                        period=p,
                        lag=int(lg),
                        window=int(w),
                    )
                    try:
                        r = fit_panel_ols_cluster(spec, include_controls=include_controls)
                        rows.append(r.iloc[0].to_dict())
                    except Exception as e:
                        rows.append({
                            "scale": scale,
                            "eco_metric": eco_metric,
                            "climate_var": cv,
                            "period": p,
                            "lag": int(lg),
                            "window": int(w),
                            "error": f"{type(e).__name__}: {e}",
                        })
    df = pd.DataFrame(rows)
    # сортируем по силе сигнала в редколесье (как ты уже делал)
    if "p_sparse_interaction" in df.columns:
        df = df.sort_values("p_sparse_interaction", na_position="last")
    return df