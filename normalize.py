from __future__ import annotations

from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
OBS_OUT = Path("data/processed/observations.csv")
META_OUT = Path("data/processed/descriptions.csv")

OBS_RENAME_MAP = {
    "Индивидuальный ID описания": "description_id",
    "Название вида": "species",
    "Высота (м) от": "high_min",
    "Высота (м) до": "high_max",
    "Высота (м) сред": "high_medium",
    "Фeнoфаза": "phenophase",
    "Жизненность": "vitality",
    "Обилие": "abundance_class",
    "Кол-во стволов/ кустов": "number_tree",
}

META_RENAME_MAP = {
    "Индивидuальный ID строки": "description_id",
    "Год": "year",
    "№точки на профиле": "point_number",
    "Профиль №": "cross_section_number",
    "Широта": "latitude",
    "Долгота": "longitude",
    "Геоморфология": "geomorphology",
    "Доминант древесного яруса": "tree_dominant",
    "0 луг (кск до 0,11), 1 разреженный лес (до 0,21), 2 лес (>=0,21) ": "afforestation",
    "Общее п.п. (%)": "projective_cover",
    "Сомкнuтость крон": "crown_density",
    "Величина площадки (м2)": "description_area",
}

OBS_COLUMNS = [
    "description_id",
    "species",
    "high_min",
    "high_max",
    "high_medium",
    "phenophase",
    "vitality",
    "abundance_class",
    "number_tree",
]

META_COLUMNS = [
    "description_id",
    "year",
    "point_number",
    "cross_section_number",
    "latitude",
    "longitude",
    "geomorphology",
    "tree_dominant",
    "afforestation",
    "projective_cover",
    "crown_density",
    "description_area",
]


def validate_required_columns(df: pd.DataFrame, required: set[str], context: str) -> None:
    missing = required - set(df.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Missing required columns in {context}: {missing_list}")


def normalize_species(series: pd.Series) -> pd.Series:
    # normalize NBSP and whitespace; keep as pandas string dtype
    return (
        series.astype("string")
        .str.replace("\u00A0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def load_observations(files: list[Path]) -> pd.DataFrame:
    required = set(OBS_RENAME_MAP.keys())
    frames: list[pd.DataFrame] = []
    removed_rows = 0

    for file_path in files:
        df = pd.read_excel(file_path, sheet_name="Геоботаника")
        validate_required_columns(df, required, f"{file_path} (Геоботаника)")

        df = df.rename(columns=OBS_RENAME_MAP)
        df["description_id"] = df["description_id"].astype("Int64")

        before = len(df)
        df["species"] = normalize_species(df["species"])
        df = df[df["species"].notna() & (df["species"] != "#")]
        removed_rows += before - len(df)

        frames.append(df[OBS_COLUMNS].copy())

    if not frames:
        raise ValueError("No observation files found for processing.")

    observations = pd.concat(frames, ignore_index=True)
    if observations.empty:
        raise ValueError("Observation output is empty after processing.")

    print(f"Удалено пустых строк: {removed_rows}")
    return observations


def load_metadata(files: list[Path]) -> pd.DataFrame:
    required = set(META_RENAME_MAP.keys())
    frames: list[pd.DataFrame] = []

    for file_path in files:
        meta = pd.read_excel(file_path, sheet_name="Сводная")
        validate_required_columns(meta, required, f"{file_path} (Сводная)")

        meta = meta.rename(columns=META_RENAME_MAP)
        meta["description_id"] = meta["description_id"].astype("Int64")

        # keep as float because values like 1.5 exist
        meta["point_number"] = pd.to_numeric(meta["point_number"], errors="coerce")

        frames.append(meta[META_COLUMNS].copy())

    if not frames:
        raise ValueError("No metadata files found for processing.")

    metadata = pd.concat(frames, ignore_index=True)
    if metadata.empty:
        raise ValueError("Metadata output is empty after processing.")

    return metadata


def main() -> None:
    files = sorted(RAW_DIR.glob("*.xlsm"))
    if not files:
        raise ValueError(f"No .xlsm files found in {RAW_DIR}.")

    OBS_OUT.parent.mkdir(parents=True, exist_ok=True)

    observations = load_observations(files)
    observations.to_csv(OBS_OUT, index=False)

    metadata = load_metadata(files)
    metadata.to_csv(META_OUT, index=False)


if __name__ == "__main__":
    main()

