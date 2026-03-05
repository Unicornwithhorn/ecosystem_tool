# core/panel_model.py
# core/panel_model.py
from __future__ import annotations

import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
from pathlib import Path
from core.panel_dataset import PanelEcoSpec, build_panel_eco_dataset, save_panel_eco_dataset

def _ensure_panel_eco(scale: str, metric: str, filters: list[dict] | None = None) -> str:
    out_path = Path(f"data/processed/panel_eco_{scale}_{metric}.csv")
    if out_path.exists():
        return str(out_path)

    spec = PanelEcoSpec(
        trait_scale=scale,
        eco_metric=metric,
        filters=[],
        out_path=str(out_path),
    )
    panel = build_panel_eco_dataset(spec)
    save_panel_eco_dataset(panel, spec)
    return str(out_path)

def run_panel_model(spec: dict) -> pd.DataFrame:
    """
    spec keys:
      scale: str
      metric: str
      climate_var: str
      period: str
      lag: int
      window: int
      filters: dict with optional keys: river, geomorph_level, impact_type, afforestation(list[int])
    """

    scale = spec["scale"]
    metric = spec["metric"]
    climate_var = spec["climate_var"]
    period = spec["period"]
    lag = int(spec.get("lag", 0))
    window = int(spec.get("window", 1))

    panel_path = _ensure_panel_eco(scale, metric, filters=spec.get("filters_list_for_build"))
    panel = pd.read_csv(panel_path)
    meteo = pd.read_csv("data/processed/meteo_periods_1991_2020.csv")

    # --- build climate signal ---
    clim = (
        meteo.loc[meteo["period"] == period, ["year", climate_var]]
        .rename(columns={climate_var: "clim"})
        .sort_values("year")
        .reset_index(drop=True)
    )
    clim["clim"] = clim["clim"].rolling(window).mean()
    clim["clim"] = clim["clim"].shift(lag)

    df = panel.merge(clim, on="year", how="left")

    # --- apply UI filters (если заданы) ---
    f = spec.get("filters", {}) or {}

    river = f.get("river")
    if river and river != "All":
        # в panel-датасете нет source_file, зато есть profile_id
        df = df[df["profile_id"].astype(str).str.contains(str(river), na=False)]

    geom = f.get("geomorph_level")
    if geom and geom != "All":
        df = df[df["geomorph_level"] == geom]

    impact = f.get("impact_type")
    if impact and impact != "All":
        df = df[df["impact_type"] == impact]

    aff = f.get("afforestation")  # list[int] or None
    if aff is not None:
        df = df[df["afforestation"].isin(aff)]

    # --- clean ---
    df = df.dropna(subset=["eco", "clim", "afforestation", "site_id"]).copy()
    df["clim_c"] = df["clim"] - df["clim"].mean()

    # если после фильтров осталась 1 категория — interaction может стать вырожденным, это норм.
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

    out = pd.DataFrame([{
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
        "p_sparse_interaction": float(pvals.get("clim_c:C(afforestation)[T.1]", np.nan)),
        "p_forest_interaction": float(pvals.get("clim_c:C(afforestation)[T.2]", np.nan)),

        "r2": float(model.rsquared),
    }])

    return out