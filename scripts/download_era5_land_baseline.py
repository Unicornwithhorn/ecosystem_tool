import cdsapi
from pathlib import Path

OUT_DIR = Path("data/external")
OUT_DIR.mkdir(parents=True, exist_ok=True)

AREA = [
    57.0,   # North
    36.5,   # West
    56.2,   # South
    37.8,   # East
]


VARIABLES = [
    "2m_temperature",
    "total_precipitation",
]

TIMES = ["00:00", "12:00"]

def download(year_from: int, year_to: int, out_name: str):
    c = cdsapi.Client()
    years = [str(y) for y in range(year_from, year_to + 1)]

    target = OUT_DIR / out_name
    if target.exists():
        print(f"Skip (already exists): {target}")
        return

    print(f"Downloading {year_from}-{year_to} -> {target}")

    c.retrieve(
        "reanalysis-era5-land",
        {
            "format": "netcdf",
            "variable": VARIABLES,
            "year": years,
            "month": [f"{m:02d}" for m in range(1, 13)],
            "day": [f"{d:02d}" for d in range(1, 32)],
            "time": TIMES,
            "area": AREA,
        },
        str(target),
    )

    print("Done:", target)

def main():
    for y in range(1991, 2001):
        download(y, y, f"era5land_{y}.nc")


# def main():
#     download(1991, 2000, "era5land_1991_2000.nc")
    # download(2001, 2010, "era5land_2001_2010.nc")
    # download(2011, 2020, "era5land_2011_2020.nc")

if __name__ == "__main__":
    main()
