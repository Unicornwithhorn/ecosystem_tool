from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT_ROOT / "data" / "processed"

def plot_timeseries(df, spec):
    """
    spec:
      - kind: "line" (default) or "scatter"
      - x, y: column names
      - title: optional
      - out_name: optional (without extension)
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    kind = spec.get("kind", "line")
    xcol = spec["x"]
    ycol = spec["y"]

    x = df[xcol]
    y = df[ycol]

    plt.figure()
    if kind == "scatter":
        plt.scatter(x, y)
    else:
        plt.plot(x, y, marker="o")

    plt.xlabel(xcol)
    plt.ylabel(ycol)
    plt.title(spec.get("title", ""))
    plt.grid(True)

    out_name = spec.get("out_name")
    if not out_name:
        out_name = f"{ycol}_{kind}"

    out_path = OUT_DIR / f"{out_name}.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()

    return out_path
