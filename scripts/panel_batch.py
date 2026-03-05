import pandas as pd
import statsmodels.formula.api as smf

panel = pd.read_csv("data/processed/panel_eco_N_sigma.csv")
meteo = pd.read_csv("data/processed/meteo_periods_1991_2020.csv")

climate_vars = ["t_mean_c", "precip_mm", "pedya"]

periods = [
"DJF",
"MAM",
"JJA",
"SON",
"warm_half_year",
"cold_half_year",
]

lags = [0, 1, 2]
windows = [1, 2, 3]

results = []

for climate_var in climate_vars:
    for period in periods:

        clim = (
            meteo.loc[meteo["period"] == period, ["year", climate_var]]
            .rename(columns={climate_var: "clim"})
            .sort_values("year")
        )

        for window in windows:

            clim_w = clim.copy()
            clim_w["clim"] = clim_w["clim"].rolling(window).mean()

            for lag in lags:

                clim_l = clim_w.copy()
                clim_l["clim"] = clim_l["clim"].shift(lag)

                df = panel.merge(clim_l, on="year", how="left")

                df = df.dropna(subset=[
                    "eco",
                    "clim",
                    "afforestation",
                    "geomorph_level",
                    "impact_type",
                ]).copy()

                if len(df) < 50:
                    continue

                df["clim_c"] = df["clim"] - df["clim"].mean()

                formula = "eco ~ clim_c * C(afforestation) + C(geomorph_level) + C(impact_type)"

                model = smf.ols(formula, data=df).fit(
                    cov_type="cluster",
                    cov_kwds={"groups": df["site_id"]},
                )

                params = model.params
                pvals = model.pvalues

                slope_meadow = params.get("clim_c", float("nan"))
                delta_sparse = params.get("clim_c:C(afforestation)[T.1]", 0)
                delta_forest = params.get("clim_c:C(afforestation)[T.2]", 0)

                slope_sparse = slope_meadow + delta_sparse
                slope_forest = slope_meadow + delta_forest

                results.append(
                    {
                        "climate_var": climate_var,
                        "period": period,
                        "lag": lag,
                        "window": window,
                        "n_obs": len(df),

                        "beta_meadow": slope_meadow,
                        "beta_sparse": slope_sparse,
                        "beta_forest": slope_forest,

                        "delta_sparse_meadow": delta_sparse,
                        "delta_forest_meadow": delta_forest,

                        "p_clim": pvals.get("clim_c", float("nan")),
                        "p_sparse_interaction": pvals.get(
                            "clim_c:C(afforestation)[T.1]", float("nan")
                        ),
                        "p_forest_interaction": pvals.get(
                            "clim_c:C(afforestation)[T.2]", float("nan")
                        ),

                        "r2": model.rsquared,
                    }
                )


results_df = pd.DataFrame(results)

results_df = results_df.sort_values("p_sparse_interaction")

results_df.to_csv(
"data/processed/panel_climate_batch.csv",
index=False,
)

print(results_df.head(20))
