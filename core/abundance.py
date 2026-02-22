from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ABUNDANCE_XLSX = PROJECT_ROOT / "data" / "registry" / "обилие.xlsx"


def load_abundance_weights(xlsx_path: Path = ABUNDANCE_XLSX) -> dict[str, float]:
    df = pd.read_excel(xlsx_path)
    code_col = df.columns[0]
    w_col = df.columns[1]

    weights = (
        df[[code_col, w_col]]
        .dropna()
        .assign(
            code=lambda x: x[code_col].astype("string").str.strip(),
            w=lambda x: pd.to_numeric(x[w_col], errors="coerce"),
        )
        .dropna(subset=["code", "w"])
    )
    return dict(zip(weights["code"], weights["w"]))


def attach_weights(df: pd.DataFrame, abundance_col: str = "abundance_class", out_col: str = "w") -> pd.DataFrame:
    weights = load_abundance_weights()
    out = df.copy()
    out[out_col] = out[abundance_col].astype("string").str.strip().map(weights)
    return out
