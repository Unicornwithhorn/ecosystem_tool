from __future__ import annotations

import argparse
from core.panel_dataset import PanelEcoSpec, build_panel_eco_dataset, save_panel_eco_dataset


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scale", required=True, help="Ellenberg scale, e.g. N, R, T, L")
    p.add_argument("--metric", required=True, help="Eco metric, e.g. cwm or sigma")
    p.add_argument("--out", default=None, help="Optional output path for CSV")
    args = p.parse_args()

    # Пока делаем MVP без сложных CLI-фильтров:
    # ты можешь руками зафиксировать filters в коде (как в сценариях UI),
    # а потом расширим до чтения JSON.
    filters = []  # позже сюда подставим твой fixed ecological setup

    spec = PanelEcoSpec(
        trait_scale=args.scale,
        eco_metric=args.metric,
        filters=filters,
        out_path=args.out,
    )

    panel = build_panel_eco_dataset(spec)
    out = save_panel_eco_dataset(panel, spec)

    print(f"Saved panel eco dataset: {out}")
    print(panel.head(10).to_string(index=False))
    print(f"n_obs={len(panel)} n_sites={panel['site_id'].nunique()} years={panel['year'].nunique()}")


if __name__ == "__main__":
    main()