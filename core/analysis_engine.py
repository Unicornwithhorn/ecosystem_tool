from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd

# import normalize  # твой normalize.py (с функциями load_observations/load_metadata)


# ----------------------------
# Paths
# ----------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # корень проекта ecosystem_tool/


RAW_DIR = PROJECT_ROOT / "data" / "raw"
REGISTRY_PROFILES = PROJECT_ROOT / "data" / "registry" / "profiles.csv"

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OBS_FILE = PROCESSED_DIR / "observations.csv"
META_FILE = PROCESSED_DIR / "descriptions.csv"
MERGED_OUT = PROCESSED_DIR / "merged_with_profiles.csv"

DESCRIPTION_KEYS = ["description_id", "source_file"]

# ----------------------------
# Geomorphology normalization
# ----------------------------

# Нормализованные уровни (то, что ты хочешь использовать в сценариях)
# NB: дополни по мере надобности, если у тебя есть другие коды.
GEOMORPH_CODE_TO_LEVEL: dict[str, str] = {
    "НП": "low_floodplain",        # низкая пойма
    "СП": "medium_floodplain",     # средняя пойма
    "ВП": "high_floodplain",       # высокая пойма
    "НТ": "terrace",               # надпойменная терраса
    "ВР": "watershed",             # водораздел
    # Примеры (если встречаются):
    # "ПР": "riverbed",            # прирусловая часть / русло — зависит от твоих кодов
    # "С": "slope",                # склон
}

def add_geomorph_level(
    df: pd.DataFrame,
    *,
    src_col: str = "geomorphology",
    out_col: str = "geomorph_level",
    mapping: dict[str, str] = GEOMORPH_CODE_TO_LEVEL,
) -> pd.DataFrame:
    """
    Добавляет колонку geomorph_level (low_floodplain/high_floodplain/terrace/...)
    на основе кодов в geomorphology (например НП/ВП/НТ).

    Ничего не "портит": исходный geomorphology оставляем.
    Неизвестные коды -> <NA>.
    """
    if src_col not in df.columns:
        # если в каких-то наборах данных нет geomorphology — просто возвращаем как есть
        return df

    out = df.copy()
    codes = out[src_col].astype("string").str.strip()

    # иногда Excel приносит NBSP или странные пробелы
    codes = (
        codes
        .str.replace("\u00A0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    # берём только первый "токен" на случай "НП (что-то)" или "НП/ВП"
    # если у тебя строго коды "НП" без хвостов — всё равно не навредит
    codes_first = codes.str.extract(r"^([A-Za-zА-Яа-яЁё]{1,3})", expand=False)

    out[out_col] = codes_first.map(mapping).astype("string")
    return out

# ----------------------------
# Filtering helpers
# ----------------------------

FilterValue = Any
FilterSpec = dict[str, FilterValue]


def apply_filters(df: pd.DataFrame, filters: FilterSpec | None) -> pd.DataFrame:
    """
    filters supports:
      - {"col": value}                    -> equality
      - {"col": {"in": [a,b,c]}}          -> membership
      - {"col": {"between": (lo, hi)}}    -> numeric range inclusive
      - {"col": {"contains": "text"}}     -> substring search (case-insensitive)
      - {"col": {"regex": "pattern"}}     -> regex match
      - {"col": callable}                -> callable(series)->bool mask
    """
    if not filters:
        return df

    mask = pd.Series(True, index=df.index)

    for col, rule in filters.items():
        if col not in df.columns:
            raise KeyError(f"Filter column not found: {col}")

        s = df[col]

        if callable(rule):
            m = rule(s)
            if not isinstance(m, pd.Series) or m.dtype != bool:
                raise TypeError(f"Callable filter for '{col}' must return boolean Series.")
            mask &= m
            continue

        if isinstance(rule, dict):
            if "in" in rule:
                mask &= s.isin(rule["in"])
            elif "between" in rule:
                lo, hi = rule["between"]
                s_num = pd.to_numeric(s, errors="coerce")
                mask &= s_num.between(lo, hi, inclusive="both")
            elif "contains" in rule:
                pattern = str(rule["contains"])
                mask &= s.astype("string").str.contains(pattern, case=False, na=False)
            elif "regex" in rule:
                pattern = str(rule["regex"])
                mask &= s.astype("string").str.contains(pattern, regex=True, na=False)
            else:
                raise ValueError(f"Unknown filter op for '{col}': {rule}")
        else:
            mask &= (s == rule)

    return df[mask].copy()


def to_descriptions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse species-level merged table to description-level:
    1 row = 1 description (relevé).
    """
    missing = [c for c in DESCRIPTION_KEYS if c not in df.columns]
    if missing:
        raise ValueError(f"to_descriptions: missing columns {missing}")

    return df.drop_duplicates(subset=DESCRIPTION_KEYS, keep="first").copy()


def aggregate_descriptions(
    df: pd.DataFrame,
    *,
    filters: dict | None = None,
    groupby: list[str] | None = None,
    metrics: list | None = None,
) -> pd.DataFrame:
    """
    Like aggregate(), but forces description-level unit first.
    """
    df0 = df
    if filters:
        df0 = apply_filters(df0, filters)

    df_desc = to_descriptions(df0)

    return aggregate(
        df_desc,
        filters=None,      # уже применили
        groupby=groupby,
        metrics=metrics,
    )



# ----------------------------
# Registry: profiles.csv
# ----------------------------

def load_profiles_registry(path: Path = REGISTRY_PROFILES) -> pd.DataFrame:
    """
    Loads data/registry/profiles.csv.
    Works with both ';' and ',' separators (auto-detect).
    Requires columns: profile_id, source_file, impact_type
    """
    if not path.exists():
        raise FileNotFoundError(f"Profiles registry not found: {path}")

    # auto-detect separator; tolerant to Excel/PyCharm save style
    reg = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")

    required = {"profile_id", "source_file", "impact_type"}
    missing = required - set(reg.columns)
    if missing:
        raise ValueError(f"profiles.csv missing columns: {sorted(missing)}")

    # normalize strings
    for col in ["profile_id", "source_file", "impact_type"]:
        reg[col] = reg[col].astype("string").str.strip()

    # sanity: mapping must be one row per source_file
    if reg["source_file"].duplicated().any():
        dups = reg.loc[reg["source_file"].duplicated(), "source_file"].unique().tolist()
        raise ValueError(f"profiles.csv has duplicated source_file values: {dups}")

    return reg


def add_profile_attributes(merged: pd.DataFrame, reg: pd.DataFrame) -> pd.DataFrame:
    """
    Adds profile_id + impact_type to merged by joining on source_file.
    """
    out = merged.merge(reg, on="source_file", how="left", validate="many_to_one")

    missing = int(out["profile_id"].isna().sum())
    if missing:
        print(
            f"WARNING: {missing} rows have no profile mapping (profile_id is NA). "
            f"Check source_file names vs {REGISTRY_PROFILES}."
        )
    return out


# ----------------------------
# Build merged from RAW (multiple .xlsm)
# ----------------------------

def load_processed() -> pd.DataFrame:
    if not OBS_FILE.exists():
        raise FileNotFoundError(f"Missing {OBS_FILE}. Run normalize.py first.")
    if not META_FILE.exists():
        raise FileNotFoundError(f"Missing {META_FILE}. Run normalize.py first.")

    obs = pd.read_csv(OBS_FILE, encoding="utf-8")
    meta = pd.read_csv(META_FILE, encoding="utf-8")

    required_obs = {"description_id", "source_file"}
    required_meta = {"description_id", "source_file"}
    if not required_obs <= set(obs.columns):
        raise ValueError(f"observations.csv missing columns: {sorted(required_obs - set(obs.columns))}")
    if not required_meta <= set(meta.columns):
        raise ValueError(f"descriptions.csv missing columns: {sorted(required_meta - set(meta.columns))}")

    merged = obs.merge(
        meta,
        on=["description_id", "source_file"],
        how="left",
        validate="many_to_one",
    )

    # Missing metadata check
    if "year" in merged.columns:
        missing = int(merged["year"].isna().sum())
        if missing:
            print(f"WARNING: {missing} observation rows have no matching metadata (year is NA).")

    # profile registry (impact_type etc.)
    reg = load_profiles_registry(REGISTRY_PROFILES)
    merged = add_profile_attributes(merged, reg)

    # geomorph level
    merged = add_geomorph_level(merged)

    return merged

def build_merged_from_raw(raw_dir: str | Path = RAW_DIR) -> pd.DataFrame:
    """
    Backward-compatible name.
    In the new pipeline we DON'T parse raw xlsm here.
    We analyze processed CSVs produced by normalize.py.
    """
    return load_processed()


# ----------------------------
# Metrics (computed per DESCRIPTION block)
# ----------------------------

@dataclass(frozen=True)
class Metric:
    name: str
    func: Callable[[pd.DataFrame], float]


def metric_mean(col: str, name: str | None = None) -> Metric:
    n = name or f"mean_{col}"

    def _f(df: pd.DataFrame) -> float:
        return pd.to_numeric(df[col], errors="coerce").mean()

    return Metric(n, _f)


def metric_count(col: str, out_name: str):
    """
    Count non-null values per group (DESCRIPTION-level).
    """
    return {
        "out": out_name,
        "func": lambda s: s[col].notna().sum(),
    }


def metric_sum(col: str, out_name: str):
    """
    Sum numeric column per group.
    """
    return {
        "out": out_name,
        "func": lambda s: pd.to_numeric(s[col], errors="coerce").sum(),
    }


def metric_richness(name: str = "species_richness") -> Metric:
    def _f(df: pd.DataFrame) -> float:
        return float(df["species"].nunique())

    return Metric(name, _f)


def metric_presence(species_name: str, name: str | None = None) -> Metric:
    """
    Presence within description = 1 if species occurs at least once, else 0.
    When averaged over groupby, becomes "share of descriptions where species is present".
    """
    n = name or f"presence_{species_name}"

    def _f(df: pd.DataFrame) -> float:
        return float((df["species"] == species_name).any())

    return Metric(n, _f)


def metric_species_mean(col: str, species_name: str, name: str | None = None) -> Metric:
    n = name or f"{col}_mean_{species_name}"

    def _f(df: pd.DataFrame) -> float:
        sub = df[df["species"] == species_name]
        return pd.to_numeric(sub[col], errors="coerce").mean()

    return Metric(n, _f)


# ----------------------------
# Aggregation engine (2-step)
# ----------------------------

def aggregate(
    merged: pd.DataFrame,
    *,
    filters: FilterSpec | None = None,
    groupby: list[str] | None = None,
    metrics: list[Metric] | None = None,
    description_id_col: str = "description_id",
) -> pd.DataFrame:
    """
    Universal aggregation:

    Step 1: Filter merged rows
    Step 2: Compute metrics per description_id (+ groupby dims)
    Step 3: Aggregate description-level metrics across groupby dims (mean),
            and count unique descriptions.

    This avoids bias because merged has many rows per description (one per species).
    """
    if groupby is None:
        groupby = ["year"]

    if metrics is None:
        metrics = [
            metric_richness("mean_species_richness"),
            metric_mean("projective_cover", "mean_projective_cover"),
        ]

    df = apply_filters(merged, filters)

    # validate columns
    if description_id_col not in df.columns:
        raise KeyError(f"Missing {description_id_col} in merged")
    if "species" not in df.columns:
        raise KeyError("Missing 'species' in merged")
    for g in groupby:
        if g not in df.columns:
            raise KeyError(f"Missing groupby column: {g}")

    # Step 2: per-description metrics
    desc_keys = [description_id_col] + groupby
    rows: list[dict[str, Any]] = []

    for key, block in df.groupby(desc_keys, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        rec = dict(zip(desc_keys, key))
        for m in metrics:
            rec[m.name] = m.func(block)
        rows.append(rec)

    desc_df = pd.DataFrame(rows)
    if desc_df.empty:
        return desc_df

    # Step 3: aggregate over descriptions in each group
    out = (
        desc_df.groupby(groupby, dropna=False)
        .agg({m.name: "mean" for m in metrics} | {description_id_col: "nunique"})
        .rename(columns={description_id_col: "n_descriptions"})
        .reset_index()
        .sort_values(groupby)
    )

    return out


# ----------------------------
# Example / smoke test
# ----------------------------

def main() -> None:
    merged = build_merged_from_raw(RAW_DIR)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_csv(MERGED_OUT, index=False)

    # пример: динамика по годам для профилей с "верхний бьеф"
    result = aggregate(
        merged,
        filters={"impact_type": {"contains": "верхний бьеф"}},
        groupby=["year", "impact_type"],
        metrics=[
            metric_mean("projective_cover", "mean_projective_cover"),
            metric_richness("mean_species_richness"),
        ],
    )

    print("Saved:", MERGED_OUT)
    print("\nPreview:")
    print(result.head(20))


# if __name__ == "__main__":
#     main()

