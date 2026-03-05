import pandas as pd

df = pd.read_csv("data/processed/panel_eco_N_sigma.csv")

print(df["afforestation"].isna().sum())
print(df.groupby("site_id")["afforestation"].nunique().max())