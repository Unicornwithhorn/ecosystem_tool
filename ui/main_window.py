from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout,
    QPushButton, QLabel, QComboBox
)

from PySide6.QtGui import QPixmap
from PySide6.QtCore import Qt
from core.scenario_runner import run_scenario, ScenarioSpec
from core.analysis_engine import load_processed
from types import SimpleNamespace

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

        self.mode.addItems([
            "Projective cover (classic)",
            "Ellenberg ecospectrum",
        ])
        self.mode.setItemData(0, "classic")
        self.mode.setItemData(1, "ecospectrum")

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
        self.eco_metric.setEnabled(is_eco)
        self.scale.setEnabled(is_eco)

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
            metric_name = self.eco_metric.currentText()

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



