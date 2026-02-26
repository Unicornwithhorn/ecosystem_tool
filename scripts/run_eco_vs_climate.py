from types import SimpleNamespace
from core.scenario_runner import run_scenario

def main():
    # Настройки гипотезы:
    # CWM(F) в год t vs Pedya(JJA) со сглаживанием/лагом
    spec = SimpleNamespace(
        name="eco_vs_climate_demo",
        analysis="eco_vs_climate",

        # eco settings
        trait_scale="M",      # Ellenberg scale: F/T/N/R/M...
        eco_metric="cwm",     # cwm/sigma/w_median/w_min/w_max

        # climate settings
        period="JJA",         # DJF/MAM/JJA/SON/cold_half_year/warm_half_year
        lag=0,                # 0 = текущий год, 1 = прошлый год, ...
        window=1,             # 1 = без памяти, 2 = среднее (t,t-1), ...

        # Filters (применяются к eco-данным до агрегации)
        # если хочешь ограничить годы сразу:
        # filters={"year": {"between": [2009, 2019]}},
        filters={},

        # Plot (scatter). Title можно оставить пустым — автосгенерится в runner.
        plot={
            "title": "",
            "out_name": "eco_vs_pedya_demo"
        },
    )

    df, plot_path = run_scenario(spec)

    print("Rows:", len(df))
    print("Plot:", plot_path)
    print(df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()