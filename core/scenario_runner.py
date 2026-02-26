from dataclasses import dataclass
from typing import Dict, Any, List
from core.abundance import attach_weights
from core.traits import attach_trait
from core.ecospectrum import compute_ecospectrum_by_description
from core.analysis_engine import apply_filters
import pandas as pd
from pathlib import Path
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PEDYA_PERIODS_CSV = PROJECT_ROOT / "data" / "processed" / "meteo_pedya_periods_1991_2020.csv"
METEO_PERIODS_CSV = PROJECT_ROOT / "data" / "processed" / "meteo_periods_1991_2020.csv"



from core.analysis_engine import (
    load_processed,
    aggregate_descriptions,
    metric_mean,
)

from core.plotting import plot_timeseries


@dataclass
class ScenarioSpec:
    name: str
    filters: Dict[str, Any]
    groupby: List[str]
    metric: Dict[str, Any]
    plot: Dict[str, Any] | None = None
    analysis: str = "aggregate"  # "aggregate" (как было) или "ecospectrum"
    trait_scale: str = "M"  # M = moisture
    eco_metric: str = "cwm"  # что строим: cwm/sigma/w_median/w_min/w_max
    climate_var: str = "pedya"
    period: str = "JJA"
    lag: int = 0
    window: int = 1
    climate_csv: str | None = None


def build_metric(metric_spec: Dict[str, Any]):
    t = metric_spec["type"]
    col = metric_spec["column"]
    out = metric_spec["out"]

    if t == "mean":
        return metric_mean(col, out)

    raise ValueError(f"Unknown metric type: {t}")

def load_meteo_periods(path: Path = METEO_PERIODS_CSV) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Meteo periods CSV not found: {path}. Run scripts/build_meteo_periods.py"
        )
    df = pd.read_csv(path)
    required = {"year", "period", "t_mean_c", "precip_mm", "pedya"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Missing columns in meteo periods CSV: {sorted(missing)}")

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["period"] = df["period"].astype("string")
    for c in ["t_mean_c", "precip_mm", "pedya"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["year", "period"]).copy()
    df["year"] = df["year"].astype(int)
    return df


def run_scenario(spec: ScenarioSpec):
    """
    Execute one analysis scenario and return (DataFrame, plot_path).

    Supported analysis modes (spec.analysis):

    1) "classic"
       Projective cover / classic vegetation metrics time series.

    2) "ecospectrum"
       Ellenberg ecospectrum metrics computed from species abundances:
         - per description: CWM, sigma, weighted median, min/max
         - then aggregated by year (and filtered by geomorph/impact/river etc.)
       Controls:
         - spec.trait_scale: "M", "T", "N", "R" (Ellenberg scale)
         - spec.eco_metric: "cwm", "sigma", "w_median", "w_min", "w_max"
         - spec.filters: dict passed to apply_filters()

    3) "climate"
       Climate index time series from unified period-level climate table.
       Source CSV (default):
         data/processed/meteo_periods_1991_2020.csv
       Expected columns:
         - year (int)
         - period (str): "DJF", "MAM", "JJA", "SON", "cold_half_year", "warm_half_year"
         - pedya, t_mean_c, precip_mm (float)
       Controls:
         - spec.period: which period to display (default "DJF")
         - spec.climate_csv: optional override path to meteo_periods csv
         - spec.filters: optional filters (e.g. year range)
       Output:
         DataFrame with at least ["year", "pedya"] (currently plotted variable is pedya).

    4) "eco_vs_climate"
       Scatter / correlation between yearly ecospectrum metric and a climate variable
       aggregated by period.
       Climate source (default):
         data/processed/meteo_periods_1991_2020.csv
       Controls:
         - spec.trait_scale: Ellenberg scale for eco side ("M","T","N","R")
         - spec.eco_metric: ecospectrum metric ("cwm","sigma","w_median","w_min","w_max")
         - spec.period: climate period (default "JJA")
         - spec.climate_var: "pedya" | "t_mean_c" | "precip_mm" (default "pedya")
         - spec.lag: int >= 0, shift climate signal by lag years (default 0)
         - spec.window: int >= 1, rolling mean window (years) for climate signal (default 1)
         - spec.filters: filters applied to eco data before yearly aggregation
         - spec.climate_csv: optional override path to meteo_periods csv
       Output:
         DataFrame with columns:
           year, eco, clim, clim_signal, pearson_r, spearman_rho, climate_var, period, lag, window

    Notes:
      - DJF and cold_half_year are computed with year-shift:
          DJF(t) = Dec(t-1) + Jan(t) + Feb(t)
          cold_half_year(t) includes Oct-Dec of (t-1) and Jan-Mar of t
      - Spearman correlation uses SciPy (required dependency).
    """

    analysis_kind = getattr(spec, "analysis", "aggregate")

    # ------------------------------------------------------------
    # ECO vs CLIMATE:
    #   yearly ecospectrum metric (eco) vs climate(period) variable
    #
    # Climate source: METEO_PERIODS_CSV (meteo_periods_1991_2020.csv)
    # Supported climate_var: "pedya" | "t_mean_c" | "precip_mm"
    #
    # Supports lag/window on climate signal and scatter plot.
    # ------------------------------------------------------------
    if analysis_kind == "eco_vs_climate":
        # ---- (A) build yearly eco metric via ecospectrum pipeline ----
        df = load_processed()

        eco_filters = getattr(spec, "filters", {}) or {}
        if eco_filters:
            df = apply_filters(df, eco_filters)

        # only abundance rows
        df = df[df["abundance_class"].notna()].copy()

        # weights
        df = attach_weights(df, abundance_col="abundance_class", out_col="w")
        df = df[df["w"].notna() & (df["w"] > 0)].copy()

        # attach trait scale (default M)
        scale = getattr(spec, "trait_scale", "M")
        df = attach_trait(df, scale=scale)

        # ecospectrum per description
        eco = compute_ecospectrum_by_description(df, trait_col=scale, weight_col="w")

        # merge description metadata (need year)
        meta_cols = ["description_id", "year"]
        meta = df[meta_cols].drop_duplicates("description_id")
        eco2 = eco.merge(meta, on="description_id", how="left")

        # aggregate eco by year
        metric_name = getattr(spec, "eco_metric", "cwm")
        eco_year = (
            eco2.groupby(["year"], as_index=False)[metric_name]
            .mean()
            .rename(columns={metric_name: "eco"})
            .sort_values("year")
        )

        # ---- (B) load climate from unified meteo_periods CSV ----
        csv_path = Path(getattr(spec, "climate_csv", None) or METEO_PERIODS_CSV)
        period = getattr(spec, "period", None) or "JJA"
        climate_var = getattr(spec, "climate_var", "pedya")  # pedya / t_mean_c / precip_mm

        dfc = load_meteo_periods(csv_path)
        dfc = dfc[dfc["period"] == period].copy()

        if climate_var not in dfc.columns:
            raise KeyError(
                f"Unknown climate_var='{climate_var}'. "
                f"Expected one of: pedya, t_mean_c, precip_mm. "
                f"Found columns: {list(dfc.columns)}"
            )

        clim = (
            dfc[["year", climate_var]]
            .rename(columns={climate_var: "clim"})
            .dropna(subset=["year", "clim"])
            .sort_values("year")
        )

        # ---- (C) join on year ----
        joined = eco_year.merge(clim, on="year", how="inner").sort_values("year")

        # ---- (D) apply window + lag to climate signal ----
        window = int(getattr(spec, "window", 1) or 1)
        lag = int(getattr(spec, "lag", 0) or 0)

        # rolling mean over window years (aligned at current year), then shift by lag
        clim_roll = joined["clim"].rolling(window=window, min_periods=window).mean()
        joined["clim_signal"] = clim_roll.shift(lag)

        # drop rows where signal undefined
        joined = joined.dropna(subset=["eco", "clim_signal"]).copy()

        # ---- (E) correlation numbers ----
        if len(joined) < 3:
            pearson = float("nan")
            spearman = float("nan")
        else:
            pearson = joined["eco"].corr(joined["clim_signal"], method="pearson")
            spearman = joined["eco"].corr(joined["clim_signal"], method="spearman")

        # ---- (F) plot scatter ----
        plot_path = None
        if getattr(spec, "plot", None):
            # allow auto-title if none provided
            if not spec.plot.get("title"):
                spec.plot["title"] = (
                    f"{metric_name}({scale}) vs {climate_var}({period}) | "
                    f"lag={lag}, window={window} | "
                    f"r={pearson:.2f}, ρ={spearman:.2f}, n={len(joined)}"
                )

            out_name_default = f"eco_vs_{climate_var}_{period}_lag{lag}_win{window}"

            plot_path = plot_timeseries(
                joined.rename(columns={"clim_signal": "x", "eco": "y"}),
                {
                    "kind": "scatter",
                    "x": "x",
                    "y": "y",
                    "title": spec.plot.get("title", ""),
                    "out_name": spec.plot.get("out_name", out_name_default),
                },
            )

        # return full table so you can inspect years and values
        joined["pearson_r"] = pearson
        joined["spearman_rho"] = spearman
        joined["climate_var"] = climate_var
        joined["period"] = period
        joined["lag"] = lag
        joined["window"] = window
        return joined, plot_path

    # -------------------------
    # 1) CLIMATE (Pedya periods)
    # -------------------------
    if analysis_kind == "climate":
        csv_path = Path(getattr(spec, "climate_csv", None) or METEO_PERIODS_CSV)
        dfc = load_meteo_periods(csv_path)

        period = getattr(spec, "period", None) or "DJF"
        dfc = dfc[dfc["period"] == period].copy()

        if getattr(spec, "filters", None):
            dfc = apply_filters(dfc, spec.filters)

        dfc = dfc.sort_values("year")
        climate_var = getattr(spec, "climate_var", "pedya")
        result = dfc[["year", climate_var]].rename(columns={climate_var: "value"}).copy()

        plot_path = plot_timeseries(result, spec.plot) if spec.plot else None
        return result, plot_path

    # -------------------------
    # 2) ECOSPECTRUM (Ellenberg)
    # -------------------------
    if analysis_kind == "ecospectrum":
        df = load_processed()

        # (A) filters before ecospectrum (river/geomorph/impact/year...)
        if spec.filters:
            df = apply_filters(df, spec.filters)

        # (B) only abundance rows
        df = df[df["abundance_class"].notna()].copy()

        # (C) weights
        df = attach_weights(df, abundance_col="abundance_class", out_col="w")
        df = df[df["w"].notna() & (df["w"] > 0)].copy()

        # (D) attach trait scale (default M)
        scale = getattr(spec, "trait_scale", "M")
        df = attach_trait(df, scale=scale)

        # (E) ecospectrum per description
        eco = compute_ecospectrum_by_description(df, trait_col=scale, weight_col="w")

        # (F) merge description metadata
        meta_cols = ["description_id", "year", "geomorph_level", "impact_type", "source_file"]
        meta = df[meta_cols].drop_duplicates("description_id")
        eco2 = eco.merge(meta, on="description_id", how="left")

        # (G) aggregate by scenario groupby (usually year)
        metric_name = getattr(spec, "eco_metric", "cwm")
        groupby = spec.groupby or ["year"]

        result = (
            eco2.groupby(groupby, as_index=False)[metric_name]
            .mean()
            .sort_values(groupby)
        )

        plot_path = None
        if spec.plot:
            plot_path = plot_timeseries(result, spec.plot)

        return result, plot_path

    # -------------------------
    # 3) CLASSIC AGGREGATE MODE
    # -------------------------
    df = load_processed()
    metric = build_metric(spec.metric)

    result = aggregate_descriptions(
        df,
        filters=spec.filters,
        groupby=spec.groupby,
        metrics=[metric],
    ).reset_index()

    plot_path = None
    if spec.plot:
        plot_path = plot_timeseries(result, spec.plot)

    return result, plot_path
