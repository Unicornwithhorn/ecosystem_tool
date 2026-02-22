from core.analysis_engine import load_processed, aggregate_descriptions, metric_mean

df = load_processed()

result = aggregate_descriptions(
    df,
    groupby=["year"],
    metrics=[metric_mean("projective_cover", "mean_projective_cover")],
)

print(result)


from core.analysis_engine import load_processed
import pandas as pd

df = load_processed()

# 1) сколько строк без года
print("Rows with year NA:", int(df["year"].isna().sum()))

# 2) сколько описаний без года
desc_keys = ["description_id", "source_file"]
na_desc = df.loc[df["year"].isna(), desc_keys].drop_duplicates()
print("Descriptions with year NA:", len(na_desc))

# 3) теперь проверим, есть ли эти описания в descriptions.csv вообще
# (читаем descriptions.csv напрямую, без merge)
from core.analysis_engine import META_FILE
meta = pd.read_csv(META_FILE, encoding="utf-8")

meta_keys = meta[desc_keys].drop_duplicates()
missing_in_meta = na_desc.merge(meta_keys, on=desc_keys, how="left", indicator=True)
missing_in_meta = missing_in_meta[missing_in_meta["_merge"] == "left_only"]

print("Descriptions missing in meta entirely:", len(missing_in_meta))
print(missing_in_meta.head(20))
