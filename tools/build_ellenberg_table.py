from __future__ import annotations

import re
from pathlib import Path
import pandas as pd
import requests


# Источник (FloraVeg.eu download)
ELLENBERG_XLSX_URL = (
    "https://files.ibot.cas.cz/cevs/downloads/floraveg/"
    "Indicator_values_Tichy_et_al%202022-11-29.xlsx"
)

DATA_DIR = Path("../data/external")
OUT_CSV = Path("../data/external/ellenberg_europe_LTF RNS.csv")


def download_file(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        return  # уже скачано

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def normalize_species_name(s: pd.Series) -> pd.Series:
    # NBSP → space, сжатие пробелов
    return (
        s.astype("string")
        .str.replace("\u00A0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def pick_best_sheet(xls: pd.ExcelFile) -> str:
    """
    Ищем лист, где есть столбец со “species/taxon/name” и набор L,T,F,R,N,S.
    Если структура файла изменится — всё равно попробуем найти лучший матч.
    """
    best = None
    best_score = -1

    for sheet in xls.sheet_names:
        df = xls.parse(sheet, nrows=50)
        cols = [str(c).strip() for c in df.columns]

        joined = " | ".join(cols).lower()

        has_name = any(re.search(r"(species|taxon|name|scientific)", c.lower()) for c in cols)
        score = 0
        if has_name:
            score += 5

        # считаем сколько индикаторов вообще похоже присутствуют
        for key in ["l", "t", "f", "r", "n", "s"]:
            # ищем отдельный столбец с меткой вида "... L" или "L_value" и т.п.
            if any(re.search(rf"(^|[^a-z]){key}([^a-z]|$)", c.lower()) for c in cols):
                score += 1

        # часто “harmonized” лист — лучший
        if "harm" in joined:
            score += 2

        if score > best_score:
            best_score = score
            best = sheet

    if best is None:
        raise ValueError("Не удалось подобрать лист в xlsx под таблицу индикаторов.")
    return best


def build_ellenberg_csv(xlsx_path: Path, out_csv: Path) -> None:
    xls = pd.ExcelFile(xlsx_path)
    sheet = pick_best_sheet(xls)
    df = xls.parse(sheet)

    # 1) Найдём колонку с названием вида
    # (подстраховка под разные имена колонок)
    lower_map = {str(c).strip().lower(): c for c in df.columns}

    name_candidates = [
        "species", "taxon", "taxon_name", "name", "scientific_name",
        "species name", "taxonname"
    ]

    name_col = None
    for cand in name_candidates:
        for k, orig in lower_map.items():
            if cand == k:
                name_col = orig
                break
        if name_col:
            break

    # fallback: первая колонка, где встречается species/taxon/name
    if name_col is None:
        for c in df.columns:
            if re.search(r"(species|taxon|name)", str(c).lower()):
                name_col = c
                break

    if name_col is None:
        raise ValueError(f"Не нашёл колонку с названием вида на листе '{sheet}'.")

    # 2) Подберём колонки индикаторов L,T,F,R,N,S
    # Будем искать по “похожести” имени колонки на одиночную букву
    def find_indicator(col_letter: str) -> str | None:
        pat = re.compile(rf"(^|[^a-z]){col_letter.lower()}([^a-z]|$)")
        for c in df.columns:
            if pat.search(str(c).strip().lower()):
                return c
        return None

    L_col = find_indicator("L")
    T_col = find_indicator("T")
    F_col = find_indicator("F")
    R_col = find_indicator("R")
    N_col = find_indicator("N")
    S_col = find_indicator("S")

    missing = [k for k, v in {"L": L_col, "T": T_col, "F": F_col, "R": R_col, "N": N_col, "S": S_col}.items() if v is None]
    if missing:
        raise ValueError(
            f"Не смог найти столбцы индикаторов {missing} на листе '{sheet}'. "
            f"Открой xlsx и посмотри точные названия колонок — подстроим маппинг."
        )

    out = pd.DataFrame({
        "species": normalize_species_name(df[name_col]),
        "L": pd.to_numeric(df[L_col], errors="coerce"),
        "T": pd.to_numeric(df[T_col], errors="coerce"),
        "F": pd.to_numeric(df[F_col], errors="coerce"),
        "R": pd.to_numeric(df[R_col], errors="coerce"),
        "N": pd.to_numeric(df[N_col], errors="coerce"),
        "S": pd.to_numeric(df[S_col], errors="coerce"),
    })

    # чистим пустые имена
    out = out[out["species"].notna() & (out["species"] != "")]
    out = out.drop_duplicates(subset=["species"], keep="first")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"OK: saved {out_csv} (sheet='{sheet}', rows={len(out)})")


def main() -> None:
    xlsx_path = DATA_DIR / "Indicator_values_Tichy_et_al.xlsx"
    download_file(ELLENBERG_XLSX_URL, xlsx_path)
    build_ellenberg_csv(xlsx_path, OUT_CSV)


if __name__ == "__main__":
    main()
