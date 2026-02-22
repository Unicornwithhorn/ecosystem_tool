from __future__ import annotations
from core.ecospectrum import compute_ecospectrum_stats
from core.ecospectrum import compute_ecospectrum_by_description


from pathlib import Path
import pandas as pd

from core.analysis_engine import load_processed

# --- Paths (подстрой при необходимости) ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ABUNDANCE_XLSX = PROJECT_ROOT / "data" / "registry" / "обилие.xlsx"
ELLENBERG_XLSX = PROJECT_ROOT / "data" / "external" / "Indicator_values_Tichy_et_al.xlsx"


def load_abundance_weights(xlsx_path: Path) -> dict[str, float]:
    """
    Ожидаем 2 колонки: код обилия и число (вес).
    В твоём файле это как раз столбцы A и B.
    """
    df = pd.read_excel(xlsx_path)
    # Попробуем взять первые две колонки независимо от их названий
    code_col = df.columns[0]
    w_col = df.columns[1]

    weights = (
        df[[code_col, w_col]]
        .dropna()
        .assign(**{
            "code": lambda x: x[code_col].astype("string").str.strip(),
            "w": lambda x: pd.to_numeric(x[w_col], errors="coerce"),
        })
        .dropna(subset=["code", "w"])
    )

    return dict(zip(weights["code"], weights["w"]))


def load_ellenberg_scale(xlsx_path: Path, scale: str = "M") -> pd.DataFrame:
    """
    Загружает шкалу Элленберга из листа 'Tab-OriginalNamesValues'.
    Возвращает DataFrame:
        species | M
    (один вид = одно значение)
    """
    sheet = "Tab-OriginalNamesValues"
    df = pd.read_excel(xlsx_path, sheet_name=sheet)

    if "Taxon" not in df.columns:
        raise KeyError(f"Не нашёл колонку 'Taxon' на листе '{sheet}'. Колонки: {list(df.columns)[:20]}")

    if scale not in df.columns:
        raise KeyError(f"Не нашёл колонку '{scale}' на листе '{sheet}'. Колонки: {list(df.columns)[:20]}")

    out = df[["Taxon", scale]].copy()
    out.columns = ["species", scale]

    # базовая нормализация пробелов
    out["species"] = (
        out["species"]
        .astype("string")
        .str.replace("\u00A0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    out[scale] = pd.to_numeric(out[scale], errors="coerce")

    # убираем строки без значения шкалы
    out = out.dropna(subset=[scale])

    # IMPORTANT: один вид = одна строка
    out = out.drop_duplicates(subset=["species"])

    return out

def main():
    SCALE = "M"
    # 1) Загружаем processed
    df = load_processed()

    # 2) Берём только те строки, где есть обилие (как ты решил)
    df = df[df["abundance_class"].notna()].copy()

    # 3) Подгружаем веса обилия и считаем weight
    weights = load_abundance_weights(ABUNDANCE_XLSX)
    df["w"] = df["abundance_class"].astype("string").str.strip().map(weights)

    before = len(df)
    df = df[df["w"].notna()].copy()
    print(f"Rows with abundance_class: {before}, mapped to weights: {len(df)} (dropped {before - len(df)})")

    #TEMP
    # inspect_ellenberg_file(ELLENBERG_XLSX)
    #TEMP


    # 4) Подгружаем Элленберг (пока только M)
    ell = load_ellenberg_scale(ELLENBERG_XLSX, scale=SCALE)


    # 5) Join по species
    df["species"] = df["species"].astype("string").str.replace("\u00A0", " ", regex=False).str.replace(r"\s+", " ", regex=True).str.strip()
    # упрощаем названия видов (убираем agg., s.l., subsp. и т.п.)
    df["species"] = df["species"].str.replace(
        r"\s+(agg\.|s\.l\.|sensu lato|subsp\..*|ssp\..*|cf\..*)$",
        "",
        regex=True
    )
    df2 = df.merge(ell, on="species", how="left")

    # ===== ЭКОСПЕКТР ПО ВСЕМ ОПИСАНИЯМ =====

    eco = compute_ecospectrum_by_description(
        df2,
        trait_col="M",
        weight_col="w"
    )

    print("\n=== ECOSPECTRUM (by description) ===")
    print(eco.head())

    # присоединяем метаданные описаний
    meta_cols = ["description_id", "year", "geomorph_level", "impact_type", "source_file"]
    meta = df2[meta_cols].drop_duplicates("description_id")

    eco2 = eco.merge(meta, on="description_id", how="left")


    #TEMP__________
    print("\n=== BASIC CHECKS ===")
    print("Descriptions (rows in eco):", len(eco))
    print("Unique description_id in df2:", df2["description_id"].nunique())

    for col in ["cwm", "sigma", "w_median", "w_min", "w_max"]:
        print(f"{col}: NaN share =", eco[col].isna().mean())

    print("\n=== RANGE CHECKS ===")
    print("cwm min/max:", eco["cwm"].min(), eco["cwm"].max())
    print("sigma min/max:", eco["sigma"].min(), eco["sigma"].max())
    print("w_min min/max:", eco["w_min"].min(), eco["w_min"].max())
    print("w_max min/max:", eco["w_max"].min(), eco["w_max"].max())

    print("\n=== META JOIN CHECK ===")
    print("year NaN share:", eco2["year"].isna().mean())
    print("geomorph_level NaN share:", eco2["geomorph_level"].isna().mean())
    print("impact_type NaN share:", eco2["impact_type"].isna().mean())

    trend = eco2.groupby("year", as_index=False)["cwm"].mean()
    print("\n=== CWM TREND BY YEAR (head) ===")
    print(trend.head(20))
    print("Years in trend:", trend["year"].nunique())
    # TEMP__________

    # пример: средний CWM по годам
    trend = (
        eco2
        .groupby("year", as_index=False)["cwm"]
        .mean()
    )

    print("\n=== CWM TREND BY YEAR ===")
    print(trend.head(20))

    # 6) Отчёт по матчингу
    total_species = df2["species"].nunique(dropna=True)
    matched_species = df2[df2[SCALE].notna()]["species"].nunique(dropna=True)
    print(f"Species in data (with abundance): {total_species}")
    print(f"Species matched with Ellenberg M: {matched_species} ({matched_species / max(total_species,1):.1%})")

    # 7) Покажем пример по одному описанию (первому)
    first_descr = int(df2["description_id"].dropna().iloc[0])
    sample = df2[df2["description_id"] == first_descr][
        ["description_id", "species", "abundance_class", "w", SCALE, "source_file", "year", "geomorph_level", "impact_type"]
    ].sort_values(["w", "species"], ascending=[False, True])

    print("\n=== SAMPLE (one description) ===")
    print(sample.head(25).to_string(index=False))

    # 8) Топ “не сматченных” видов (чтобы понимать, что чинить)
    unmatched = (
        df2[df2[SCALE].isna()]
        .groupby("species", dropna=True)["w"]
        .sum()
        .sort_values(ascending=False)
        .head(30)
    )
    print("\n=== TOP UNMATCHED SPECIES (by total weight) ===")
    print(unmatched.to_string())

    desc_id = df2["description_id"].dropna().iloc[0]
    stats = compute_ecospectrum_stats(
        df2[df2["description_id"] == desc_id],
        trait_col="M",
        weight_col="w"
    )

    print("\n=== ECOSPECTRUM STATS (one description) ===")
    for k, v in stats.items():
        print(f"{k}: {v}")

if __name__ == "__main__":
    main()
