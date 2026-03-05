# ui/panel_tab.py
from __future__ import annotations

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QComboBox, QPushButton,
    QTableWidget, QTableWidgetItem
)

from core.panel_model import run_panel_model


class PanelTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        root = QVBoxLayout(self)

        controls = QHBoxLayout()

        controls.addWidget(QLabel("Scale"))
        self.scale_cb = QComboBox()
        self.scale_cb.addItems(["M", "N", "L", "T", "F", "R"])
        controls.addWidget(self.scale_cb)

        controls.addWidget(QLabel("Metric"))
        self.metric_cb = QComboBox()
        self.metric_cb.addItems(["sigma", "cwm"])
        controls.addWidget(self.metric_cb)

        controls.addWidget(QLabel("Climate"))
        self.climate_cb = QComboBox()
        self.climate_cb.addItems(["t_mean_c", "precip_mm", "pedya"])
        controls.addWidget(self.climate_cb)

        controls.addWidget(QLabel("Period"))
        self.period_cb = QComboBox()
        self.period_cb.addItems(["DJF", "MAM", "JJA", "SON", "warm_half_year", "cold_half_year"])
        controls.addWidget(self.period_cb)

        controls.addWidget(QLabel("Lag"))
        self.lag_cb = QComboBox()
        self.lag_cb.addItems(["0", "1", "2"])
        controls.addWidget(self.lag_cb)

        controls.addWidget(QLabel("Window"))
        self.window_cb = QComboBox()
        self.window_cb.addItems(["1", "2", "3"])
        controls.addWidget(self.window_cb)

        root.addLayout(controls)

        self.run_btn = QPushButton("Run panel model")
        self.run_btn.clicked.connect(self.run_clicked)
        root.addWidget(self.run_btn)

        self.table = QTableWidget()
        root.addWidget(self.table)

    def run_clicked(self):
        spec = {
            "scale": self.scale_cb.currentText(),
            "metric": self.metric_cb.currentText(),
            "climate_var": self.climate_cb.currentText(),
            "period": self.period_cb.currentText(),
            "lag": int(self.lag_cb.currentText()),
            "window": int(self.window_cb.currentText()),
        }

        df = run_panel_model(spec)
        self._show_df(df)

    def _show_df(self, df):
        self.table.clear()
        self.table.setRowCount(len(df))
        self.table.setColumnCount(len(df.columns))
        self.table.setHorizontalHeaderLabels([str(c) for c in df.columns])

        for i in range(len(df)):
            for j, col in enumerate(df.columns):
                val = df.iloc[i, j]
                # красиво печатаем числа
                if isinstance(val, float):
                    text = f"{val:.6g}"
                else:
                    text = str(val)
                self.table.setItem(i, j, QTableWidgetItem(text))

        self.table.resizeColumnsToContents()