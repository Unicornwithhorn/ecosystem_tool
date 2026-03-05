import pandas as pd
import statsmodels.formula.api as smf




# -----------------------------

# 1. LOAD PANEL DATA

# -----------------------------

panel = pd.read_csv("data/processed/panel_eco_N_sigma.csv")


print("Observations used:", len(panel))
print("Sites:", panel["site_id"].nunique())
print("Years:", panel["year"].nunique())
# -----------------------------

# 2. LOAD CLIMATE DATA

# -----------------------------

meteo = pd.read_csv("data/processed/meteo_periods_1991_2020.csv")

# выбираем климатическую переменную

climate_var = "t_mean_c"

# выбираем период

period = "JJA"

clim = (
meteo.loc[meteo["period"] == period, ["year", climate_var]]
.rename(columns={climate_var: "clim"})
)

# -----------------------------

# 3. MERGE CLIMATE WITH PANEL

# -----------------------------

panel = panel.merge(clim, on="year", how="left")

# -----------------------------
# CLEAN DATA
# -----------------------------

panel = panel.dropna(subset=[
    "eco",
    "clim",
    "afforestation",
    "geomorph_level",
    "impact_type"
]).copy()



# -----------------------------
# 4. CENTER CLIMATE

# -----------------------------

panel["clim_c"] = panel["clim"] - panel["clim"].mean()

# -----------------------------

# 5. FIT OLS WITH CLUSTER SE

# -----------------------------

formula = "eco ~ clim_c * C(afforestation) + C(geomorph_level) + C(impact_type)"

model = smf.ols(formula, data=panel).fit(
cov_type="cluster",
cov_kwds={"groups": panel["site_id"]}
)

# -----------------------------

# 6. PRINT RESULTS

# -----------------------------

print(model.summary())
