from __future__ import annotations

from dataclasses import dataclass, field

from core.analysis_engine import load_processed, apply_filters
from core.ecospectrum import compute_ecospectrum_by_description
from core.traits import attach_trait
from core.abundance import attach_weights  # если у тебя так называется; если иначе — поправим импорт

# core/panel_model.py
from pathlib import Path
import pandas as pd

# --- Настройки вывода ---
PROCESSED_DIR = Path("data/processed")


def make_site_id(df_desc: pd.DataFrame) -> pd.Series:
    """
    Stable site id: profile_id + cross_section_number + point_number.
    Assumes these columns exist in descriptions/merged table.
    """
    return (
        df_desc["profile_id"].astype(str)
        + "_cs" + df_desc["cross_section_number"].astype(str)
        + "_pt" + df_desc["point_number"].astype(str)
    )


@dataclass(frozen=True)
class PanelEcoSpec:
    trait_scale: str          # e.g. "N", "R", "T", "L", etc.
    eco_metric: str           # e.g. "cwm" or "sigma"
    filters: list[dict] = field(default_factory=list)  # <-- вот это главное
    out_path: str | None = None


def build_panel_eco_dataset(spec: PanelEcoSpec) -> pd.DataFrame:
    """
    Build ecological panel dataset: site_id x year -> eco.
    No climate merge here.
    """
    # 1) Load processed merged table (observations + descriptions + registry data)
    df = load_processed()

    # 2) Apply your universal filters (river, floodplain level, impact, afforestation, etc.)
    df = apply_filters(df, spec.filters)

    # 3) Attach weights + traits for Ellenberg
    df = attach_weights(df)
    df = attach_trait(df, scale=spec.trait_scale)   # 'N', 'R', 'T', ...

    print("Columns after attach_trait:",
          [c for c in df.columns if c in ["N", "R", "T", "L", "M"] or "ell" in c.lower() or "trait" in c.lower()])
    print("Has w:", "w" in df.columns, "Has N:", "N" in df.columns)

    # 4) Compute ecospectrum per description (description_id granularity)
    # IMPORTANT: this function should return df with:
    # - description_id
    # - year (or can be merged from descriptions)
    # - metric columns (cwm/sigma/...)
    eco_desc = compute_ecospectrum_by_description(
        df,
        trait_col=spec.trait_scale,  # 'N' / 'R' / ...
        weight_col="w",  # у тебя attach_weights как раз делает 'w'
        id_col="description_id",
    )

    # 5) Pull metadata needed for panel aggregation
    # We assume df contains description_id rows (observations), so we take unique description-level info.
    meta_cols = [
        "description_id",
        "year",
        "profile_id",
        "cross_section_number",
        "point_number",
        "afforestation",
        "geomorph_level",
        "impact_type",
    ]
    # add river if exists
    if "river" in df.columns:
        meta_cols.append("river")

    print("NA in afforestation before aggregation:",
          df["afforestation"].isna().sum())

    agg_map = {
        "year": "first",
        "profile_id": "first",
        "cross_section_number": "first",
        "point_number": "first",
        "source_file": "first",
        "afforestation": "max",
        "geomorph_level": "first",
        "impact_type": "first",
    }

    if "river" in df.columns:
        agg_map["river"] = "first"

    desc_meta = (
        df.groupby("description_id", as_index=False)
        .agg(agg_map)
    )
    desc_meta["site_id"] = make_site_id(desc_meta)

    # 6) Merge ecospectrum values with meta
    eco_desc = eco_desc.merge(desc_meta, on="description_id", how="inner")

    if spec.eco_metric not in eco_desc.columns:
        raise KeyError(
            f"eco_metric='{spec.eco_metric}' not found in ecospectrum output. "
            f"Available columns: {list(eco_desc.columns)}"
        )

    # 7) Aggregate to site_id x year
    agg_kwargs = {
        "eco": (spec.eco_metric, "mean"),
        "n_desc": ("description_id", "nunique"),
        "afforestation": ("afforestation", "first"),
        "geomorph_level": ("geomorph_level", "first"),
        "impact_type": ("impact_type", "first"),
        "profile_id": ("profile_id", "first"),
        "source_file": ("source_file", "first"),
    }

    if "river" in eco_desc.columns:
        agg_kwargs["river"] = ("river", "first")

    panel = (
        eco_desc
        .groupby(["site_id", "year"], as_index=False)
        .agg(**agg_kwargs)
    )

    # 8) Basic cleanup / types
    panel["year"] = panel["year"].astype(int)
    panel["afforestation"] = (
        pd.to_numeric(panel["afforestation"], errors="coerce")
        .astype("Int64")  # допускает NA
    )

    # если хочешь строго 0/1/2 без пропусков:
    # panel = panel.dropna(subset=["afforestation"]).copy()
    # panel["afforestation"] = panel["afforestation"].astype(int)

    bad = panel.loc[~panel["afforestation"].isin([0, 1, 2]) & panel["afforestation"].notna(), "afforestation"].unique()
    if len(bad) > 0:
        raise ValueError(f"Unexpected afforestation codes: {bad}")

    print("Ecospectrum columns:", list(eco_desc.columns))

    return panel


def save_panel_eco_dataset(panel: pd.DataFrame, spec: PanelEcoSpec) -> Path:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    if spec.out_path:
        out = Path(spec.out_path)
    else:
        out = PROCESSED_DIR / f"panel_eco_{spec.trait_scale}_{spec.eco_metric}.csv"

    panel.to_csv(out, index=False)
    return out