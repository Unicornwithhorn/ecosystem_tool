from __future__ import annotations

from pathlib import Path
import xarray as xr
import pandas as pd
import zipfile


IN_DIR = Path("data/external")
OUT_PATH = Path("data/processed/meteo_daily_1991_2000.csv")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

def ensure_time_dim(ds: xr.Dataset) -> xr.Dataset:
    # типовые имена времени в выгрузках CDS/cfgrib
    for cand in ["time", "valid_time", "forecast_time", "datetime", "date"]:
        if cand in ds.coords or cand in ds.dims:
            if cand != "time":
                ds = ds.rename({cand: "time"})
            return ds

    # если время вдруг лежит как переменная
    for cand in ["time", "valid_time", "forecast_time"]:
        if cand in ds.data_vars:
            ds = ds.set_coords(cand)
            if cand != "time":
                ds = ds.rename({cand: "time"})
            return ds

    raise KeyError(f"Не нашёл временную координату. coords={list(ds.coords)}, dims={list(ds.dims)}")

def spatial_mean(da: xr.DataArray) -> xr.DataArray:
    dims = [d for d in da.dims if d in ("latitude", "longitude", "lat", "lon")]
    return da.mean(dim=dims, skipna=True) if dims else da


def find_var(ds: xr.Dataset, candidates: list[str]) -> str:
    for name in candidates:
        if name in ds.data_vars:
            return name
    raise KeyError(f"Не нашёл переменную среди {candidates}. Есть: {list(ds.data_vars)}")


def daily_precip_from_tp(tp: xr.DataArray) -> xr.DataArray:
    """
    Безопасная схема для ERA5/ERA5-Land:
    берём diff по времени, отрицательные (сбросы) -> 0,
    суммируем в сутки, переводим м -> мм.
    """
    tp = tp.sortby("time")
    d = tp.diff("time").clip(min=0)
    first = tp.isel(time=0)
    step = xr.concat([first, d], dim="time")
    step["time"] = tp["time"]
    step_mm = step * 1000.0
    return step_mm.resample(time="1D").sum()


def main():
    paths = []
    for p in sorted(IN_DIR.glob("era5land_*.nc")):
        if zipfile.is_zipfile(p):
            print(f"[SKIP] ZIP disguised as nc: {p.name}")
            continue
        paths.append(p)

    if not paths:
        raise SystemExit("No valid NetCDF files found")


    # открываем пачкой
    datasets = [xr.open_dataset(p, engine="netcdf4") for p in paths]
    ds = xr.combine_by_coords(
        datasets,
        combine_attrs="override"
    )
    ds = ensure_time_dim(ds)

    t_name = find_var(ds, ["2m_temperature", "t2m"])
    p_name = find_var(ds, ["total_precipitation", "tp"])

    t2m = spatial_mean(ds[t_name])
    tp = spatial_mean(ds[p_name])

    # Температура: K -> C, суточная средняя
    t_daily = (t2m - 273.15).resample(time="1D").mean()

    # Осадки: суточная сумма (мм)
    p_daily = daily_precip_from_tp(tp)

    out = xr.Dataset({"t_mean_c": t_daily, "precip_mm": p_daily}).to_dataframe().reset_index()

    out["date"] = pd.to_datetime(out["time"]).dt.date
    out = out.drop(columns=["time"])

    if "number" in out.columns:
        out = out.drop(columns=["number"])

    out.to_csv(OUT_PATH, index=False)

    print("Saved:", OUT_PATH)
    print(out.head())
    print("Rows:", len(out), "from", out["date"].min(), "to", out["date"].max())


if __name__ == "__main__":
    main()
