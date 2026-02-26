from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout,
    QPushButton, QLabel, QComboBox
)

from PySide6.QtGui import QPixmap
from PySide6.QtCore import Qt
from core.scenario_runner import run_scenario, ScenarioSpec
from core.analysis_engine import load_processed
from PySide6.QtWidgets import QLineEdit, QTableWidget, QTableWidgetItem
from types import SimpleNamespace
import math

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Ecosystem Analysis Tool")

        central = QWidget()
        layout = QVBoxLayout()

        self.river = QComboBox()
        self.geom = QComboBox()
        self.impact = QComboBox()
        self.mode = QComboBox()
        self.scale = QComboBox()
        self.scale.addItems(["M", "T", "N", "R"])
        self.scale.setEnabled(False)  # активен только в ecospectrum
        self.period_combo = QComboBox()
        self.period_combo.addItems(["DJF", "MAM", "JJA", "SON", "cold_half_year", "warm_half_year"])
        self.period_combo.setEnabled(False)

        self.climate_var = QComboBox()
        self.climate_var.addItems(["pedya", "precip_mm", "t_mean_c"])
        self.climate_var.setEnabled(False)  # активен в climate и batch

        # --- Batch controls (Eco vs Climate batch) ---
        self.periods_edit = QLineEdit("JJA,warm_half_year,MAM,DJF")
        self.lags_edit = QLineEdit("0,1,2")
        self.windows_edit = QLineEdit("1,2,3")

        self.run_batch_btn = QPushButton("Run batch")
        self.run_batch_btn.clicked.connect(self.run_batch)

        self.batch_table = QTableWidget()
        self.batch_table.setColumnCount(8)
        self.batch_table.setHorizontalHeaderLabels([
            "period", "lag", "window", "n", "pearson_r", "spearman_rho", "|r|", "plot"
        ])
        self.batch_table.setSortingEnabled(True)
        self.batch_table.cellClicked.connect(self.on_batch_row_clicked)

        # hidden unless batch-mode is active
        for w in [self.periods_edit, self.lags_edit, self.windows_edit, self.run_batch_btn, self.batch_table]:
            w.setVisible(False)

        self.mode.addItems([
            "Projective cover (classic)",
            "Ellenberg ecospectrum",
            "Climate: Pedya index",
            "Eco vs Climate (batch)"
        ])
        self.mode.setItemData(0, "classic")
        self.mode.setItemData(1, "ecospectrum")
        self.mode.setItemData(2, "climate")
        self.mode.setItemData(3, "eco_vs_climate_batch")

        self.eco_metric = QComboBox()
        self.eco_metric.addItems(["cwm", "sigma", "w_median", "w_min", "w_max"])
        self.eco_metric.setEnabled(False)  # включим только в eco-режиме

        self.run_btn = QPushButton("Run scenario")
        self.output = QLabel("")

        self.run_btn.clicked.connect(self.run)
        self.mode.currentIndexChanged.connect(self.on_mode_changed)

        self.plot_label = QLabel()
        self.plot_label.setAlignment(Qt.AlignCenter)
        self.plot_label.setMinimumHeight(300)  # чтобы было видно даже в маленьком окне
        self.plot_label.setText("Plot will appear here")

        self._last_plot_path = None

        layout.addWidget(QLabel("River"))
        layout.addWidget(self.river)
        layout.addWidget(QLabel("Geomorphology"))
        layout.addWidget(self.geom)
        layout.addWidget(QLabel("Impact type"))
        layout.addWidget(self.impact)
        layout.addWidget(QLabel("Analysis mode"))
        layout.addWidget(self.mode)
        layout.addWidget(QLabel("Ellenberg scale (for ecospectrum mode)"))
        layout.addWidget(self.scale)
        layout.addWidget(QLabel("Period:"))
        layout.addWidget(self.period_combo)
        layout.addWidget(QLabel("Climate variable:"))
        layout.addWidget(self.climate_var)
        layout.addWidget(QLabel("Batch periods (comma):"))
        layout.addWidget(self.periods_edit)
        layout.addWidget(QLabel("Batch lags (comma):"))
        layout.addWidget(self.lags_edit)
        layout.addWidget(QLabel("Batch windows (comma):"))
        layout.addWidget(self.windows_edit)
        layout.addWidget(self.run_batch_btn)

        layout.addWidget(self.batch_table)

        layout.addWidget(QLabel("Eco metric (for ecospectrum mode)"))
        layout.addWidget(self.eco_metric)

        layout.addWidget(self.run_btn)
        layout.addWidget(self.plot_label)

        layout.addWidget(self.output)

        central.setLayout(layout)
        self.setCentralWidget(central)

        # <-- автозаполнение
        self.populate_dropdowns()
        self.on_mode_changed()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self._last_plot_path:
            pixmap = QPixmap(self._last_plot_path)
            if not pixmap.isNull():
                scaled = pixmap.scaled(
                    self.plot_label.size(),
                    Qt.KeepAspectRatio,
                    Qt.SmoothTransformation
                )
                self.plot_label.setPixmap(scaled)

    def _parse_int_list(self, s: str):
        return [int(x.strip()) for x in s.split(",") if x.strip()]

    def _parse_str_list(self, s: str):
        return [x.strip() for x in s.split(",") if x.strip()]

    def on_batch_row_clicked(self, row: int, col: int):
        if not hasattr(self, "_batch_rows") or row >= len(self._batch_rows):
            return

        r = self._batch_rows[row]
        spec = r["spec"]

        # на всякий случай синхронизируем с текущим выбором в UI
        # (если ты батч запускал с одним climate_var, а потом переключил dropdown)
        spec.climate_var = getattr(spec, "climate_var", None) or self.climate_var.currentText()
        climate_var = spec.climate_var

        # второй прогон — уже с plot (одна картинка для выбранной комбинации)
        spec.plot = {
            "title": f"{spec.eco_metric}({spec.trait_scale}) vs {climate_var}({spec.period}) "
                     f"lag={spec.lag} win={spec.window}",
            "out_name": f"ui_scatter_{spec.trait_scale}_{spec.eco_metric}_{climate_var}_{spec.period}"
                        f"_lag{spec.lag}_win{spec.window}",
        }

        df, plot_path = run_scenario(spec)
        self.show_plot(plot_path)

        r_val = df["pearson_r"].iloc[0] if len(df) else "NA"
        rho_val = df["spearman_rho"].iloc[0] if len(df) else "NA"

        self.output.setText(
            "SELECTED\n"
            f"climate_var={climate_var}, period={spec.period}, lag={spec.lag}, window={spec.window}\n"
            f"n={len(df)} r={r_val} rho={rho_val}\n"
            f"plot={plot_path}"
        )

    def run_batch(self):
        # 1) Eco-настройки из UI
        river = self.river.currentText()
        geom = self.geom.currentText()
        impact = self.impact.currentText()

        scale = self.scale.currentText()
        eco_metric = self.eco_metric.currentText()

        filters = {}

        geom_value = self.geom.currentText()
        if geom_value != "All":
            filters["geomorph_level"] = geom_value

        river_value = self.river.currentText()
        if river_value != "All":
            filters["source_file"] = {"contains": river_value}

        impact_value = self.impact.currentText()
        if impact_value != "All":
            filters["impact_type"] = impact_value

        periods = self._parse_str_list(self.periods_edit.text())
        lags = self._parse_int_list(self.lags_edit.text())
        windows = self._parse_int_list(self.windows_edit.text())

        # 2) Прогон
        rows = []
        total = len(periods) * len(lags) * len(windows)
        k = 0

        for period in periods:
            for lag in lags:
                for window in windows:
                    k += 1
                    spec = SimpleNamespace(
                        name=f"ui_batch_{scale}_{eco_metric}_{period}_lag{lag}_win{window}",
                        analysis="eco_vs_climate",
                        filters=filters,
                        trait_scale=scale,
                        eco_metric=eco_metric,
                        period=period,
                        lag=lag,
                        window=window,
                        climate_var = self.climate_var.currentText(),
                        plot=None,  # в батче картинки не строим
                    )
                    try:
                        df, _ = run_scenario(spec)
                        n = len(df)
                        if n > 0:
                            pearson = float(df["pearson_r"].iloc[0])
                            spearman = float(df["spearman_rho"].iloc[0])
                            absr = abs(pearson) if not math.isnan(pearson) else float("nan")
                        else:
                            pearson = float("nan")
                            spearman = float("nan")
                            absr = float("nan")

                        rows.append({
                            "period": period, "lag": lag, "window": window, "n": n,
                            "pearson_r": pearson, "spearman_rho": spearman, "abs_r": absr,
                            "spec": spec,  # сохраним spec для клика по строке
                        })

                    except Exception as e:
                        rows.append({
                            "period": period, "lag": lag, "window": window, "n": 0,
                            "pearson_r": float("nan"), "spearman_rho": float("nan"), "abs_r": float("nan"),
                            "spec": spec,
                            "error": str(e),
                        })

        # 3) Заполняем таблицу
        self._batch_rows = rows  # сохраним для on_batch_row_clicked
        self.batch_table.setRowCount(len(rows))

        for i, r in enumerate(rows):
            def put(col, val):
                item = QTableWidgetItem("" if val is None else str(val))
                self.batch_table.setItem(i, col, item)

            put(0, r["period"])
            put(1, r["lag"])
            put(2, r["window"])
            put(3, r["n"])
            put(4,
                f"{r['pearson_r']:.3f}" if isinstance(r["pearson_r"], float) and not math.isnan(r["pearson_r"]) else "")
            put(5, f"{r['spearman_rho']:.3f}" if isinstance(r["spearman_rho"], float) and not math.isnan(
                r["spearman_rho"]) else "")
            put(6, f"{r['abs_r']:.3f}" if isinstance(r["abs_r"], float) and not math.isnan(r["abs_r"]) else "")
            put(7, "click")  # просто маркер

        self.output.setText(
            f"BATCH DONE\n"
            f"Eco: scale={scale}, metric={eco_metric}\n"
            f"Filters: {filters}\n"
            f"Combos: {total}\n"
            f"Tip: click a row to plot scatter."
        )

    def show_plot(self, plot_path: str):
        self._last_plot_path = plot_path
        if not plot_path:
            self.plot_label.setText("No plot generated")
            return

        pixmap = QPixmap(plot_path)
        if pixmap.isNull():
            self.plot_label.setText(f"Failed to load plot:\n{plot_path}")
            return

        # масштабируем под ширину label, сохраняя пропорции
        scaled = pixmap.scaled(
            self.plot_label.size(),
            Qt.KeepAspectRatio,
            Qt.SmoothTransformation
        )
        self.plot_label.setPixmap(scaled)

    def on_mode_changed(self):
        mode_code = self.mode.currentData()
        is_eco = (mode_code == "ecospectrum")
        is_climate = (mode_code == "climate")
        is_batch = (mode_code == "eco_vs_climate_batch")

        # eco controls
        self.eco_metric.setEnabled(is_eco or is_batch)
        self.scale.setEnabled(is_eco or is_batch)

        # main run buttons
        self.run_btn.setVisible(not is_batch)

        # climate controls
        self.period_combo.setEnabled(is_climate)  # period_combo используется в одиночном climate-режиме
        self.climate_var.setEnabled(is_climate or is_batch)  # NEW: выбор pedya/precip/t_mean

        # batch controls visibility
        for w in [self.periods_edit, self.lags_edit, self.windows_edit, self.run_batch_btn, self.batch_table]:
            w.setVisible(is_batch)

        # site filters:
        # - in climate mode: disable (since climate is regional bbox)
        # - in batch mode: keep enabled (eco filters are needed)
        self.river.setEnabled(not is_climate)
        self.geom.setEnabled(not is_climate)
        self.impact.setEnabled(not is_climate)

    def populate_dropdowns(self):
        try:
            df = load_processed()
        except Exception as e:
            self.output.setText(f"Failed to load processed data:\n{e}")

            self.river.clear()
            self.geom.clear()
            self.impact.clear()

            self.river.addItem("— no data —")
            self.geom.addItem("— no data —")
            self.impact.addItem("— no data —")

            self.run_btn.setEnabled(False)
            return

        # --- Rivers ---
        sf = df["source_file"].astype("string").fillna("")
        rivers = (
            sf.str.extract(r"^([^_\-\s]+)", expand=False)
            .dropna()
            .unique()
            .tolist()
        )
        rivers = sorted(rivers)

        # --- Geomorph levels ---
        geomorphs = (
            df["geomorph_level"]
            .astype("string")
            .dropna()
            .unique()
            .tolist()
        )
        geomorphs = sorted(geomorphs)

        # --- Impact types ---
        impacts = (
            df["impact_type"]
            .astype("string")
            .dropna()
            .unique()
            .tolist()
        )
        impacts = sorted([x for x in impacts if x and x != "nan"])

        # --- Fill dropdowns ---
        self.river.clear()
        self.geom.clear()
        self.impact.clear()

        self.river.addItem("All")
        self.river.addItems(rivers)

        self.geom.addItem("All")
        self.geom.addItems(geomorphs)

        self.impact.addItem("All")
        self.impact.addItems(impacts)

        self.run_btn.setEnabled(bool(rivers) and bool(geomorphs))

        self.output.setText(
            f"Loaded rows: {len(df)} | "
            f"Rivers: {len(rivers)}, "
            f"Geomorph: {len(geomorphs)}, "
            f"Impact: {len(impacts)}"
        )

    def run(self):
        mode_text = self.mode.currentText()
        mode_code = self.mode.currentData()

        msg = f"MODE DEBUG:\ntext='{mode_text}'\ncode='{mode_code}'"
        if hasattr(self.output, "setPlainText"):
            self.output.setPlainText(msg)
        else:
            self.output.setText(msg)

        # 1) Собираем фильтры из UI
        filters = {}

        geom_value = self.geom.currentText()
        if geom_value != "All":
            filters["geomorph_level"] = geom_value

        river_value = self.river.currentText()
        if river_value != "All":
            filters["source_file"] = {"contains": river_value}

        impact_value = self.impact.currentText()
        if impact_value != "All":
            filters["impact_type"] = impact_value

        # 2) Выбираем режим
        mode_code = self.mode.currentData()


        # --- ECO SPECTRUM MODE ---
        if mode_code == "ecospectrum":
            metric_name = self.eco_metric.currentText()  # cwm/sigma/w_median/w_min/w_max

            scale = self.scale.currentText()

            spec = SimpleNamespace(
                name="ui_eco",
                analysis="ecospectrum",
                trait_scale=scale,  # ← ВАЖНО: берём из dropdown
                eco_metric=metric_name,
                filters=filters,
                groupby=["year"],
                metric=None,
                plot={
                    "kind": "line",
                    "x": "year",
                    "y": metric_name,
                    "title": f"Ellenberg {scale} trend: {metric_name}",
                }
            )

            df, plot_path = run_scenario(spec)
            self.show_plot(plot_path)
            self.output.setText(
                f"MODE: ecospectrum (Ellenberg {scale})\n"
                f"Eco metric: {metric_name}\n"
                f"Filters used: {filters}\n"
                f"Rows: {len(df)}\n"
                f"Plot: {plot_path}"
            )
            return

        # --- CLIMATE MODE (Pedya) ---
        if mode_code == "climate":
            period = self.period_combo.currentText()

            spec = SimpleNamespace(
                name=f"ui_climate_pedya_{period}",
                analysis="climate",
                period=period,  # run_scenario читает через getattr
                filters={},  # климат общий; можно оставить пустым
                groupby=["year"],
                metric=None,
                plot={
                    "kind": "line",
                    "x": "year",
                    "y": "pedya",
                    "title": f"Pedya index ({period})",
                }
            )

            df, plot_path = run_scenario(spec)
            self.show_plot(plot_path)
            self.output.setText(
                f"MODE: climate\n"
                f"Index: Pedya\n"
                f"Period: {period}\n"
                f"Rows: {len(df)}\n"
                f"Plot: {plot_path}"
            )
            return

        # --- CLASSIC MODE (как было) ---
        spec = ScenarioSpec(
            name="ui_classic",
            filters=filters,
            groupby=["year"],
            metric={
                "type": "mean",
                "column": "projective_cover",
                "out": "mean_projective_cover",
            },
            plot={
                "kind": "line",
                "x": "year",
                "y": "mean_projective_cover",
                "title": "Mean projective cover trend",
            }
        )

        df, plot_path = run_scenario(spec)
        self.show_plot(plot_path)
        self.output.setText(
            f"MODE: classic\n"
            f"Filters used: {filters}\n"
            f"Rows: {len(df)}\n"
            f"Plot: {plot_path}"
        )



