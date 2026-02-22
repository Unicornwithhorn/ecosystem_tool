import pandas as pd
from core.analysis_engine import OBS_FILE, META_FILE

obs = pd.read_csv(OBS_FILE, encoding="utf-8")
meta = pd.read_csv(META_FILE, encoding="utf-8")

keys = ["description_id", "source_file"]

# описания, где year = NA после merge
merged = obs.merge(meta, on=keys, how="left")
na_desc = merged.loc[merged["year"].isna(), keys].drop_duplicates()

print("Descriptions with NA year:", len(na_desc))

# проверим, есть ли они вообще в descriptions.csv
meta_keys = meta[keys].drop_duplicates()

missing_meta = na_desc.merge(
    meta_keys,
    on=keys,
    how="left",
    indicator=True
)

missing_meta = missing_meta[missing_meta["_merge"] == "left_only"]

print("Descriptions missing in descriptions.csv:", len(missing_meta))
print(missing_meta.head(20))
