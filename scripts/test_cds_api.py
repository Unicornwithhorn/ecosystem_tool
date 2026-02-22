import os
import cdsapi

print("HOME:", os.path.expanduser("~"))
print("Looking for .cdsapirc at:", os.path.join(os.path.expanduser("~"), ".cdsapirc"))

c = cdsapi.Client()

c.retrieve(
    "reanalysis-era5-land",
    {
        "format": "netcdf",
        "variable": ["2m_temperature", "total_precipitation"],
        "year": "2019",
        "month": "07",
        "day": "01",
        "time": ["00:00", "06:00", "12:00", "18:00"],
        "area": [56.0, 36.0, 54.0, 40.0],  # N, W, S, E (пока пример)
    },
    "data/external/era5land_test_2019-07-01.nc"
)

print("Done! Saved to data/external/era5land_test_2019-07-01.nc")
