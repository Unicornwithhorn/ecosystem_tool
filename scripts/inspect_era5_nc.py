from pathlib import Path
import xarray as xr

path = Path("data/external/data_0.nc")

ds = xr.open_dataset(path)
print(ds)
print("\nVariables:", list(ds.data_vars))
print("Coords:", list(ds.coords))

# Быстрый просмотр размеров
for k, v in ds.sizes.items():
    print(f"{k}: {v}")
