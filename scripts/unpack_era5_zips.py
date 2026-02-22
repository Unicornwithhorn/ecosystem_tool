from __future__ import annotations

import re
import zipfile
from pathlib import Path


EXTERNAL_DIR = Path("data/external")


YEAR_RE = re.compile(r"era5land_(\d{4})\.nc$", re.IGNORECASE)


def unpack_one(zip_path: Path) -> None:
    m = YEAR_RE.search(zip_path.name)
    if not m:
        return
    year = m.group(1)

    out_zip = zip_path.with_name(f"era5land_{year}.zip")
    out_nc = zip_path.with_name(f"era5land_{year}.nc")  # итоговый netcdf
    tmp_extract = zip_path.with_name(f"_tmp_{year}.nc")

    # Если out_nc уже есть и это не zip — считаем, что год уже распакован
    if out_nc.exists() and not zipfile.is_zipfile(out_nc):
        if zip_path.exists() and zip_path != out_zip and not out_zip.exists():
            zip_path.rename(out_zip)
            print(f"[OK] Renamed ZIP: {zip_path.name} -> {out_zip.name}")
        else:
            print(f"[SKIP] Already unpacked: {out_nc.name}")
        return

    if not zipfile.is_zipfile(zip_path):
        print(f"[WARN] Not a ZIP, skip: {zip_path.name}")
        return

    # 1) Сначала переименуем ZIP в .zip (не трогая out_nc)
    if zip_path != out_zip and not out_zip.exists():
        zip_path.rename(out_zip)
        zip_path = out_zip
        print(f"[OK] Renamed ZIP: -> {out_zip.name}")

    # 2) Распакуем .nc внутрь в временный файл
    with zipfile.ZipFile(zip_path, "r") as zf:
        nc_members = [n for n in zf.namelist() if n.lower().endswith(".nc")]
        if not nc_members:
            raise RuntimeError(f"No .nc inside ZIP: {zip_path}")
        member = nc_members[0]

        # извлекаем во временный файл (чтобы не зависеть от data_0.nc)
        with zf.open(member) as src, open(tmp_extract, "wb") as dst:
            dst.write(src.read())
        print(f"[OK] Extracted {member} -> {tmp_extract.name}")

    # 3) Атомарно заменим/поставим итоговый out_nc
    if out_nc.exists():
        out_nc.unlink()
    tmp_extract.replace(out_nc)
    print(f"[OK] Wrote NC: {out_nc.name}")



def main() -> None:
    if not EXTERNAL_DIR.exists():
        raise SystemExit(f"Folder not found: {EXTERNAL_DIR.resolve()}")

    candidates = sorted(EXTERNAL_DIR.glob("era5land_*.nc"))
    if not candidates:
        print(f"No files like era5land_YYYY.nc found in {EXTERNAL_DIR}")
        return

    for p in candidates:
        try:
            unpack_one(p)
        except Exception as e:
            print(f"[ERROR] {p.name}: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
