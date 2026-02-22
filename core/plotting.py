from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT_ROOT / "data" / "processed"

def plot_timeseries(df, spec):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    x = df[spec["x"]]
    y = df[spec["y"]]

    plt.figure()
    plt.plot(x, y, marker="o")
    plt.xlabel(spec["x"])
    plt.ylabel(spec["y"])
    plt.title(spec.get("title", ""))
    plt.grid(True)

    out_path = OUT_DIR / f"{spec['y']}_timeseries.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()

    return out_path
