from pathlib import Path
import pandas as pd

# =====================
# PATHS
# =====================
PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUT_OBS = PROJECT_ROOT / "data" / "processed" / "observations.csv"
OUT_META = PROJECT_ROOT / "data" / "processed" / "descriptions.csv"
OUT_UNMATCHED = PROJECT_ROOT / "data" / "processed" / "unmatched_species.csv"
ALIASES_FILE = PROJECT_ROOT / "data" / "registry" / "species_aliases.csv"
ELLENBERG_XLSX = PROJECT_ROOT / "data" / "external" / "Indicator_values_Tichy_et_al.xlsx"

ELLENBERG_SHEET = "Tab-IVs-Tichy-et-al2022"

NBSP = "\u00A0"


# =====================
# HELPERS
# =====================

def normalize_text(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
        .str.replace(NBSP, " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def load_species_aliases(path: Path) -> dict[str, str]:
    if not path.exists():
        print("ℹ️ species_aliases.csv not found — aliases not applied")
        return {}

    df = pd.read_csv(path, encoding="utf-8-sig", sep=None, engine="python")
    df.columns = normalize_text(df.columns)

    if not {"raw_species", "canonical_species"} <= set(df.columns):
        raise ValueError("species_aliases.csv must contain raw_species, canonical_species")

    df["raw_species"] = normalize_text(df["raw_species"])
    df["canonical_species"] = normalize_text(df["canonical_species"])

    df = df.dropna(subset=["raw_species", "canonical_species"])
    df = df.drop_duplicates(subset=["raw_species"], keep="last")

    print(f"ℹ️ Loaded {len(df)} species aliases")
    return dict(zip(df["raw_species"], df["canonical_species"]))


def apply_species_aliases(df: pd.DataFrame, species_col: str, aliases: dict) -> pd.DataFrame:
    out = df.copy()
    out["species_raw"] = normalize_text(out[species_col])
    out["species_canonical"] = out["species_raw"].map(aliases).fillna(out["species_raw"])
    return out


def load_ellenberg_species() -> set[str]:
    print(
        "ELLENBERG source:",
        ELLENBERG_XLSX.resolve(),
        "exists =",
        ELLENBERG_XLSX.exists()
    )

    if not ELLENBERG_XLSX.exists():
        print("⚠️ Ellenberg xlsx not found — skipping check")
        return set()

    df = pd.read_excel(
        ELLENBERG_XLSX,
        sheet_name=ELLENBERG_SHEET,
        usecols=[1],   # колонка B
    )

    df.columns = ["species"]
    df["species"] = normalize_text(df["species"])

    species = df["species"].dropna().unique().tolist()
    print(f"ℹ️ Loaded {len(species)} Ellenberg species")
    return set(species)


# =====================
# MAIN PIPELINE
# =====================

def main() -> None:
    files = sorted(fp for fp in RAW_DIR.glob("*.xlsm") if not fp.name.startswith("~$"))

    if not files:
        raise RuntimeError("No .xlsm files found in data/raw")

    print(f"📂 Found {len(files)} raw files")

    aliases = load_species_aliases(ALIASES_FILE)
    ellenberg_species = load_ellenberg_species()

    obs_frames = []
    meta_frames = []

    for file in files:
        print(f"\n➡ Processing: {file.name}")
        source = (
            str(file.stem)
            .replace("\u00A0", " ")
            .strip()
        )

        # -------- GEO-BOTANY --------
        df = pd.read_excel(file, sheet_name="Геоботаника")

        df = df.rename(columns={
            "Индивидuальный ID описания": "description_id",
            "Название вида": "species",
            "Высота (м) от": "height_min",
            "Высота (м) до": "height_max",
            "Высота (м) сред": "height_mean",
            "Фeнoфаза": "phenophase",
            "Жизненность": "vitality",
            "Обилие": "abundance_class",
            "Кол-во стволов/ кустов": "n_individuals",
        })

        df["description_id"] = df["description_id"].astype("Int64")
        df["species"] = normalize_text(df["species"])

        before = len(df)
        df = df[df["species"].notna() & (df["species"] != "#")]
        print(f"  removed empty species: {before - len(df)}")

        obs = df[
            [
                "description_id",
                "species",
                "height_min",
                "height_max",
                "height_mean",
                "phenophase",
                "vitality",
                "abundance_class",
                "n_individuals",
            ]
        ].copy()

        obs["source_file"] = source
        obs = apply_species_aliases(obs, "species", aliases)

        obs_frames.append(obs)

        # -------- METADATA --------
        meta_raw = pd.read_excel(file, sheet_name="Сводная")

        # Множество ID из геоботаники (то, с чем должны совпасть метаданные)
        obs_ids = set(pd.to_numeric(obs["description_id"], errors="coerce").astype("Int64").dropna().tolist())

        # Кандидаты для ID в "Сводной" (у разных файлов бывает по-разному)
        id_candidates = [
            "Индивидuальный ID описания",
            "Индивидuальный ID строки",
        ]

        # + добавим авто-поиск: любые колонки, где есть 'ID' и ( 'опис' или 'строк' )
        auto_candidates = []
        for col in meta_raw.columns:
            col_s = str(col).lower()
            if "id" in col_s and ("опис" in col_s or "строк" in col_s):
                auto_candidates.append(col)

        candidates = [c for c in id_candidates if c in meta_raw.columns] + auto_candidates

        best_col = None
        best_hits = -1

        for col in candidates:
            s = pd.to_numeric(meta_raw[col], errors="coerce").astype("Int64")
            hits = int(s.isin(list(obs_ids)).sum())
            if hits > best_hits:
                best_hits = hits
                best_col = col

        if best_col is None:
            raise ValueError(
                f"{file.name}: cannot find suitable ID column in 'Сводная'. "
                f"Available columns: {list(meta_raw.columns)}"
            )

        meta = meta_raw.rename(columns={
            best_col: "description_id",
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
        })

        meta["description_id"] = pd.to_numeric(meta["description_id"], errors="coerce").astype("Int64")
        meta["point_number"] = pd.to_numeric(meta["point_number"], errors="coerce")
        meta["source_file"] = str(file.stem).replace("\u00A0", " ").strip()

        before_meta = len(meta)
        meta = meta.dropna(subset=["description_id"])
        # year — обязательное поле, иначе это не метаданные описания
        if "year" in meta.columns:
            meta = meta.dropna(subset=["year"])
        print(f"  meta rows kept: {len(meta)} (dropped {before_meta - len(meta)})")


        # --- QA: missing metadata for this file ---
        meta_ids = set(meta["description_id"].dropna().astype("Int64").tolist())
        missing_ids = sorted(obs_ids - meta_ids)
        missing_here = len(missing_ids)

        if missing_here:
            out_missing = (
                PROJECT_ROOT
                / "data"
                / "processed"
                / f"missing_meta_{source}.csv"
            )
            pd.DataFrame(
                {"description_id": missing_ids}
            ).to_csv(out_missing, index=False, encoding="utf-8")

            print(
                f"  ⚠️ metadata missing for {missing_here} descriptions "
                f"(ID col in 'Сводная' = '{best_col}', hits={best_hits})"
            )
            print(f"  🧾 saved missing IDs list to: {out_missing}")
        else:
            print(
                f"  ✓ metadata matched "
                f"(ID col in 'Сводная' = '{best_col}', hits={best_hits})"
            )


        META_COLUMNS = [
            "description_id",
            "source_file",
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
        keep = [c for c in META_COLUMNS if c in meta.columns]
        meta = meta[keep].copy()

        meta_frames.append(meta)

    # -------- CONCATENATE --------
    obs_all = pd.concat(obs_frames, ignore_index=True)
    meta_all = pd.concat(meta_frames, ignore_index=True)

    # -------- UNMATCHED --------
    if ellenberg_species:
        unmatched = (
            obs_all.loc[
                ~obs_all["species_canonical"].isin(ellenberg_species),
                ["species_raw", "species_canonical"],
            ]
            .drop_duplicates()
            .sort_values("species_raw")
        )
    else:
        unmatched = pd.DataFrame(columns=["species_raw", "species_canonical"])

    OUT_UNMATCHED.parent.mkdir(parents=True, exist_ok=True)
    unmatched.to_csv(OUT_UNMATCHED, index=False, encoding="utf-8")

    print(f"\n⚠️ Total unmatched species: {len(unmatched)}")

    # -------- SAVE --------
    OUT_OBS.parent.mkdir(parents=True, exist_ok=True)
    OUT_META.parent.mkdir(parents=True, exist_ok=True)

    obs_all.to_csv(OUT_OBS, index=False, encoding="utf-8")
    meta_all.to_csv(OUT_META, index=False, encoding="utf-8")

    print("\n✅ DONE")
    print(f"Saved: {OUT_OBS}")
    print(f"Saved: {OUT_META}")
    print(f"Saved: {OUT_UNMATCHED}")


if __name__ == "__main__":
    main()
