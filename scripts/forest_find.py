from core.analysis_engine import load_processed
import pandas as pd

df = load_processed()

print("afforestation unique:", sorted(pd.Series(df["afforestation"]).dropna().unique().tolist()))
print(df["afforestation"].value_counts(dropna=False).head(10))