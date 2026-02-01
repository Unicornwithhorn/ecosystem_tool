import pandas as pd

RAW_FILE = "data/raw/Вьюлка (условно-естественные условия).xlsm"
OBS_OUT = "data/processed/observations.csv"
META_OUT = "data/processed/descriptions.csv"

df = pd.read_excel(RAW_FILE, sheet_name="Геоботаника")

df = df.rename(columns={
    "Индивидuальный ID описания": "description_id",
    "Название вида": "species",
    "Высота (м) от": "high_min",
    "Высота (м) до": "high_max",
    "Высота (м) сред": "high_medium",
    "Фeнoфаза": "phenophase",
    "Жизненность": "vitality",
    "Обилие": "abundance_class",
    "Кол-во стволов/ кустов": "number_tree"
})

df["description_id"] = df["description_id"].astype("Int64")

before = len(df)

df["species"] = (
    df["species"]
    .astype("string")
    .str.replace("\u00A0", " ", regex=False)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
)

df = df[df["species"].notna() & (df["species"] != "#")]

after = len(df)
print(f"Удалено пустых строк: {before - after}")

obs = df[[
    "description_id",
    "species",
    "high_min",
    "high_max",
    "high_medium",
    "phenophase",
    "vitality",
    "abundance_class",
    "number_tree"
]].copy()

obs.to_csv(OBS_OUT, index=False)

meta = pd.read_excel(RAW_FILE, sheet_name="Сводная")

meta = meta.rename(columns={
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
    "Величина площадки (м2)": "description_area"
})

meta["description_id"] = meta["description_id"].astype("Int64")
meta["point_number"] = pd.to_numeric(
    meta["point_number"],
    errors="coerce"
)

meta = meta[[
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
    "description_area"
]].copy()

meta.to_csv(META_OUT, index=False)
