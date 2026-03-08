# core/panel_model.py

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

from core.panel_dataset import PanelEcoSpec, build_panel_eco_dataset, save_panel_eco_dataset


def _ensure_panel_eco(scale: str, metric: str) -> str:
    """
    Ensures that eco panel (site_id x year -> eco + meta) exists on disk.
    Builds it once and caches in data/processed/.
    """
    out_path = Path(f"data/processed/panel_eco_{scale}_{metric}.csv")
    if out_path.exists():
        return str(out_path)

    spec = PanelEcoSpec(
        trait_scale=scale,
        eco_metric=metric,
        filters=[],  # build full eco-panel; UI filters are applied later in run_panel_model
        out_path=str(out_path),
    )

    panel = build_panel_eco_dataset(spec)
    save_panel_eco_dataset(panel, spec)

    return str(out_path)


def _apply_panel_filters(df: pd.DataFrame, filters: dict[str, Any]) -> pd.DataFrame:
    """
    UI filters applied to already-built panel_eco dataframe.
    Expected keys (optional):
      - river (str or "All")
      - geomorph_level (str or "All")
      - impact_type (str or "All")
      - afforestation (list[int] or None)
    """
    if not filters:
        return df

    out = df

    source_file = filters.get("source_file")
    if source_file and source_file != "All":
        if "source_file" in out.columns:
            out = out[out["source_file"] == source_file]

    geom = filters.get("geomorph_level")
    if geom and geom != "All":
        out = out[out["geomorph_level"] == geom]

    impact = filters.get("impact_type")
    if impact and impact != "All":
        out = out[out["impact_type"] == impact]

    aff = filters.get("afforestation")  # list[int] or None
    if aff is not None:
        out = out[out["afforestation"].isin(aff)]

    return out


def run_panel_model(spec: dict[str, Any]) -> pd.DataFrame:
    """
    Runs panel OLS with cluster-robust SE and interaction climate x afforestation.

    Required spec keys:
      - scale: str  (e.g. "N")
      - metric: str (e.g. "sigma")
      - climate_var: str ("t_mean_c" | "precip_mm" | "pedya")
      - period: str ("DJF" | "MAM" | ...)
      - lag: int
      - window: int
      - filters: dict (optional, see _apply_panel_filters)
    """
    scale = spec["scale"]
    metric = spec["metric"]
    climate_var = spec["climate_var"]
    period = spec["period"]
    lag = int(spec.get("lag", 0))
    window = int(spec.get("window", 1))

    panel_path = _ensure_panel_eco(scale, metric)
    panel = pd.read_csv(panel_path)

    meteo = pd.read_csv("data/processed/meteo_periods_1991_2020.csv")

    # --- build climate signal (period -> rolling(window) -> shift(lag)) ---
    clim = (
        meteo.loc[meteo["period"] == period, ["year", climate_var]]
        .rename(columns={climate_var: "clim"})
        .sort_values("year")
        .reset_index(drop=True)
    )

    clim["clim"] = clim["clim"].rolling(window).mean()
    clim["clim"] = clim["clim"].shift(lag)

    df = panel.merge(clim, on="year", how="left")

    # --- apply UI filters ---
    df = _apply_panel_filters(df, spec.get("filters", {}) or {})

    # --- clean & center ---
    df = df.dropna(
        subset=[
            "eco",
            "clim",
            "afforestation",
            "geomorph_level",
            "impact_type",
            "site_id",
        ]
    ).copy()
    if len(df) == 0:
        return pd.DataFrame(
            [
                {
                    "period": period,
                    "lag": lag,
                    "window": window,
                    "climate_var": climate_var,
                    "n_obs": 0,
                    "beta_meadow": np.nan,
                    "beta_sparse": np.nan,
                    "beta_forest": np.nan,
                    "delta_sparse_meadow": np.nan,
                    "delta_forest_meadow": np.nan,
                    "p_clim": np.nan,
                    "p_sparse_interaction": np.nan,
                    "p_forest_interaction": np.nan,
                    "r2": np.nan,
                }
            ]
        )

    df["clim_c"] = df["clim"] - df["clim"].mean()

    # --- model ---
    formula = "eco ~ clim_c * C(afforestation) + C(geomorph_level) + C(impact_type)"
    model = smf.ols(formula, data=df).fit(
        cov_type="cluster",
        cov_kwds={"groups": df["site_id"]},
    )

    params = model.params
    pvals = model.pvalues

    slope_meadow = float(params.get("clim_c", np.nan))
    delta_sparse = float(params.get("clim_c:C(afforestation)[T.1]", 0.0))
    delta_forest = float(params.get("clim_c:C(afforestation)[T.2]", 0.0))

    out = pd.DataFrame(
        [
            {
                "period": period,
                "lag": lag,
                "window": window,
                "climate_var": climate_var,
                "n_obs": int(len(df)),
                "beta_meadow": slope_meadow,
                "beta_sparse": slope_meadow + delta_sparse,
                "beta_forest": slope_meadow + delta_forest,
                "delta_sparse_meadow": delta_sparse,
                "delta_forest_meadow": delta_forest,
                "p_clim": float(pvals.get("clim_c", np.nan)),
                "p_sparse_interaction": float(
                    pvals.get("clim_c:C(afforestation)[T.1]", np.nan)
                ),
                "p_forest_interaction": float(
                    pvals.get("clim_c:C(afforestation)[T.2]", np.nan)
                ),
                "r2": float(model.rsquared),
            }
        ]
    )
    return out