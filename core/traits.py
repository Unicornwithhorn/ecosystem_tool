from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ELLENBERG_XLSX = PROJECT_ROOT / "data" / "external" / "Indicator_values_Tichy_et_al.xlsx"


def load_ellenberg_scale(scale: str = "M", xlsx_path: Path = ELLENBERG_XLSX) -> pd.DataFrame:
    """
    Tichy et al. файл: берём лист 'Tab-OriginalNamesValues'.
    Возвращает: species | M (или L/T/R/N/S)
    """
    sheet = "Tab-OriginalNamesValues"
    df = pd.read_excel(xlsx_path, sheet_name=sheet)

    if "Taxon" not in df.columns:
        raise KeyError(f"Не нашёл 'Taxon' на листе '{sheet}'.")
    if scale not in df.columns:
        raise KeyError(f"Не нашёл шкалу '{scale}' на листе '{sheet}'.")

    out = df[["Taxon", scale]].copy()
    out.columns = ["species", scale]

    out["species"] = (
        out["species"].astype("string")
        .str.replace("\u00A0", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    out[scale] = pd.to_numeric(out[scale], errors="coerce")
    out = out.dropna(subset=[scale]).drop_duplicates(subset=["species"])
    return out


def simplify_species_name(s: pd.Series) -> pd.Series:
    """
    Простая эвристика для матчинга: убираем agg./s.l./subsp./ssp./cf.
    """
    s = s.astype("string").str.replace("\u00A0", " ", regex=False).str.replace(r"\s+", " ", regex=True).str.strip()
    s = s.str.replace(r"\s+(agg\.|s\.l\.|sensu lato|subsp\..*|ssp\..*|cf\..*)$", "", regex=True)
    return s


def attach_trait(df: pd.DataFrame, scale: str = "M") -> pd.DataFrame:
    ell = load_ellenberg_scale(scale=scale)
    out = df.copy()
    out["species"] = simplify_species_name(out["species"])
    return out.merge(ell, on="species", how="left")
