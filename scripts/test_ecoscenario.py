from types import SimpleNamespace
from core.scenario_runner import run_scenario

spec = SimpleNamespace(
    name="eco_moisture_cwm_trend",
    analysis="ecospectrum",
    trait_scale="M",
    eco_metric="cwm",
    filters={
        "geomorph_level": "low_floodplain",
        # "impact_type": "отсутствие нарушений",
        # "source_file": {"contains": "Костинка"},
    },
    groupby=["year"],
    metric=None,  # не используется в ecospectrum ветке
    plot={
        "kind": "line",
        "x": "year",
        "y": "cwm",
        "title": "Moisture (M) CWM trend",
    }
)

df, png = run_scenario(spec)
print(df.head(20))
print("plot:", png)
