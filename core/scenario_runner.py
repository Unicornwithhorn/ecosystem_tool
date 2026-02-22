from dataclasses import dataclass
from typing import Dict, Any, List
from core.abundance import attach_weights
from core.traits import attach_trait
from core.ecospectrum import compute_ecospectrum_by_description
from core.analysis_engine import apply_filters



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


def build_metric(metric_spec: Dict[str, Any]):
    t = metric_spec["type"]
    col = metric_spec["column"]
    out = metric_spec["out"]

    if t == "mean":
        return metric_mean(col, out)

    raise ValueError(f"Unknown metric type: {t}")


def run_scenario(spec: ScenarioSpec):
    if getattr(spec, "analysis", "aggregate") == "ecospectrum":
        df = load_processed()

        # 1) обычные фильтры (река/геоморф/impact/годы) применяем ДО расчёта спектра
        if spec.filters:
            df = apply_filters(df, spec.filters)

        # 2) только строки с обилием
        df = df[df["abundance_class"].notna()].copy()

        # 3) веса
        df = attach_weights(df, abundance_col="abundance_class", out_col="w")
        df = df[df["w"].notna() & (df["w"] > 0)].copy()

        # 4) traits (M)
        scale = getattr(spec, "trait_scale", "M")
        df = attach_trait(df, scale=scale)

        # 5) экоспектр по описаниям
        eco = compute_ecospectrum_by_description(df, trait_col=scale, weight_col="w")

        # 6) приклеиваем метаданные описаний (год/геоморф/impact/река)
        meta_cols = ["description_id", "year", "geomorph_level", "impact_type", "source_file"]
        meta = df[meta_cols].drop_duplicates("description_id")
        eco2 = eco.merge(meta, on="description_id", how="left")

        # 7) группировка по сценарию (обычно year)
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

    df = load_processed()

    metric = build_metric(spec.metric)

    result = aggregate_descriptions(
        df,
        filters=spec.filters,
        groupby=spec.groupby,
        metrics=[metric],
    )

    result = result.reset_index()

    plot_path = None
    if spec.plot:
        plot_path = plot_timeseries(result, spec.plot)

    return result, plot_path
