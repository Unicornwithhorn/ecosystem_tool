from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QComboBox,
    QSplitter, QScrollArea, QSizePolicy, QHeaderView
)
from core.analysis_engine import load_processed, apply_filters
from PySide6.QtGui import QPixmap
from PySide6.QtCore import Qt
from core.scenario_runner import run_scenario, ScenarioSpec
from core.analysis_engine import load_processed
from PySide6.QtWidgets import QLineEdit, QTableWidget, QTableWidgetItem
from types import SimpleNamespace
from ui.panel_tab import PanelTab
import math
import pandas as pd

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Ecosystem Analysis Tool")

        # -------------------------
        # Create widgets FIRST
        # -------------------------
        self.profile = QComboBox()
        self.geom = QComboBox()
        self.impact = QComboBox()

        self.mode = QComboBox()
        self.scale = QComboBox()
        self.period_combo = QComboBox()
        self.climate_var = QComboBox()
        self.eco_metric = QComboBox()
        self.affor = QComboBox()
        self.lag_combo = QComboBox()
        self.window_combo = QComboBox()

        self.periods_edit = QLineEdit()
        self.lags_edit = QLineEdit()
        self.windows_edit = QLineEdit()

        self.run_btn = QPushButton("Run")
        self.run_batch_btn = QPushButton("Run batch")

        self.batch_table = QTableWidget(0, 12)
        self.batch_table.setHorizontalHeaderLabels([
            "period", "lag", "window", "n_years",
            "pearson_r", "|r|", "p", "p_shift",
            "spearman_rho", "p_s",
            "years", "plot"
        ])


        self.plot_label = QLabel("Plot will appear here")
        self.plot_label.setAlignment(Qt.AlignCenter)

        # ВАЖНО: график не должен "вытеснять" таблицу своим sizeHint от pixmap
        self.plot_label.setMinimumHeight(0)
        self.plot_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Ignored)
        self.batch_table.setMinimumHeight(160)

        self.output = QLabel("")
        self.output.setWordWrap(True)

        self._last_plot_path = ""

        self.run_btn.clicked.connect(self.run)
        self.run_batch_btn.clicked.connect(self.run_batch)
        self.mode.currentIndexChanged.connect(self.on_mode_changed)
        self.batch_table.cellClicked.connect(self.on_batch_row_clicked)

        # modes
        self.mode.addItem("Classic", "classic")
        self.mode.addItem("Ecospectrum", "ecospectrum")
        self.mode.addItem("Climate", "climate")
        self.mode.addItem("Eco vs Climate (batch)", "eco_vs_climate_batch")
        self.mode.addItem("Panel climate", "panel_climate")
        self.mode.addItem("Panel climate (batch)", "panel_climate_batch")


        # ellenberg scales
        for s in ["L", "T", "K", "F", "R", "N", "S", "M"]:
            self.scale.addItem(s)

        # eco metrics
        for m in ["cwm", "sigma", "w_median", "w_min", "w_max"]:
            self.eco_metric.addItem(m)

        # climate var
        self.climate_var.addItems(["pedya", "precip_mm", "t_mean_c"])

        # periods (для climate режима)
        self.period_combo.addItems(["DJF", "MAM", "JJA", "SON", "cold_half_year", "warm_half_year"])

        # defaults for batch edits
        self.periods_edit.setText("JJA,warm_half_year,MAM,DJF")
        self.lags_edit.setText("0,1,2")
        self.windows_edit.setText("1,2,3")


        # afforestation (облесённость)
        self.affor.addItem("All", None)
        self.affor.addItem("Луг (0)", [0])
        self.affor.addItem("Редколесье (1)", [1])
        self.affor.addItem("Лес (2)", [2])
        self.affor.addItem("Луг + редколесье (0,1)", [0, 1])
        self.affor.addItem("Редколесье + лес (1,2)", [1, 2])

        self.lag_combo.addItems(["0", "1", "2"])
        self.window_combo.addItems(["1", "2", "3"])

        central = QWidget()
        root_layout = QHBoxLayout(central)

        splitter = QSplitter(Qt.Horizontal)
        root_layout.addWidget(splitter)

        # -------------------------
        # LEFT PANEL (controls)
        # -------------------------
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(8, 8, 8, 8)
        left_layout.setSpacing(6)

        # ВАЖНО: сюда добавляем ВСЕ элементы меню (как раньше), но теперь в left_layout
        left_layout.addWidget(QLabel("Profile"))
        left_layout.addWidget(self.profile)
        left_layout.addWidget(QLabel("Geomorphology"))
        left_layout.addWidget(self.geom)
        left_layout.addWidget(QLabel("Impact type"))
        left_layout.addWidget(self.impact)
        left_layout.addWidget(QLabel("Облесённость"))
        left_layout.addWidget(self.affor)
        left_layout.addWidget(QLabel("Analysis mode"))
        left_layout.addWidget(self.mode)
        left_layout.addWidget(QLabel("Ellenberg scale (for ecospectrum mode)"))
        left_layout.addWidget(self.scale)
        left_layout.addWidget(QLabel("Period:"))
        left_layout.addWidget(self.period_combo)
        left_layout.addWidget(QLabel("Climate variable:"))
        left_layout.addWidget(self.climate_var)
        left_layout.addWidget(QLabel("Lag"))
        left_layout.addWidget(self.lag_combo)

        left_layout.addWidget(QLabel("Window"))
        left_layout.addWidget(self.window_combo)

        left_layout.addWidget(QLabel("Batch periods (comma):"))
        left_layout.addWidget(self.periods_edit)
        left_layout.addWidget(QLabel("Batch lags (comma):"))
        left_layout.addWidget(self.lags_edit)
        left_layout.addWidget(QLabel("Batch windows (comma):"))
        left_layout.addWidget(self.windows_edit)

        left_layout.addWidget(self.run_batch_btn)

        left_layout.addWidget(QLabel("Eco metric (for ecospectrum mode)"))
        left_layout.addWidget(self.eco_metric)

        left_layout.addWidget(self.run_btn)
        left_layout.addStretch(1)

        # Делаем левую панель прокручиваемой
        left_scroll = QScrollArea()
        left_scroll.setWidgetResizable(True)
        left_scroll.setWidget(left_panel)

        # -------------------------
        # RIGHT PANEL (results)
        # -------------------------
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(8, 8, 8, 8)
        right_layout.setSpacing(8)

        # Таблица должна быть “жирной” и занимать место
        self.batch_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.batch_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.batch_table.horizontalHeader().setStretchLastSection(True)


        # График — тоже растягиваемый
        self.plot_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # Вертикальный сплиттер: таблица сверху, график снизу, лог внизу
        self.right_splitter = QSplitter(Qt.Vertical)
        self.right_splitter.addWidget(self.batch_table)
        self.right_splitter.addWidget(self.plot_label)
        self.right_splitter.addWidget(self.output)

        # Пропорции по умолчанию: таблица видима всегда
        self.right_splitter.setSizes([260, 640, 80])

        right_layout.addWidget(self.right_splitter)

        # Добавляем панели в splitter
        splitter.addWidget(left_scroll)
        splitter.addWidget(right_panel)

        # Пропорции: слева меню уже, справа результаты шире
        splitter.setStretchFactor(0, 1)  # left
        splitter.setStretchFactor(1, 3)  # right

        # Начальная ширина (можно под себя)
        splitter.setSizes([340, 900])

        self.setCentralWidget(central)

        # <-- автозаполнение
        self.populate_dropdowns()
        self.on_mode_changed()

        self.batch_table.setAlternatingRowColors(True)
        self.batch_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.batch_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.batch_table.setSortingEnabled(True)
        self.batch_table.horizontalHeader().setSortIndicatorShown(True)

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

    def _show_df_in_table(self, df: pd.DataFrame):
        self.batch_table.setVisible(True)

        self.batch_table.setSortingEnabled(False)
        self.batch_table.setUpdatesEnabled(False)

        self.batch_table.clearContents()
        self.batch_table.setRowCount(len(df))
        self.batch_table.setColumnCount(len(df.columns))
        self.batch_table.setHorizontalHeaderLabels([str(c) for c in df.columns])

        for i in range(len(df)):
            for j, col in enumerate(df.columns):
                val = df.iloc[i, j]
                if isinstance(val, float):
                    txt = f"{val:.6g}"
                else:
                    txt = "" if val is None else str(val)
                self.batch_table.setItem(i, j, QTableWidgetItem(txt))

        self.batch_table.resizeColumnsToContents()

        self.batch_table.setUpdatesEnabled(True)
        self.batch_table.setSortingEnabled(True)

    def _parse_int_list(self, s: str):
        return [int(x.strip()) for x in s.split(",") if x.strip()]

    def _parse_str_list(self, s: str):
        return [x.strip() for x in s.split(",") if x.strip()]

    def on_batch_row_clicked(self, row: int, col: int):
        if not hasattr(self, "_batch_rows"):
            return

        item0 = self.batch_table.item(row, 0)
        if item0 is None:
            return

        src_i = item0.data(256)
        if src_i is None or src_i >= len(self._batch_rows):
            return

        r = self._batch_rows[src_i]

        alpha = 0.05
        ny = int(r.get("n_years", r.get("n", 0)) or 0)
        pps = r.get("pearson_p_shift", float("nan"))

        bad_p = (not isinstance(pps, float)) or math.isnan(pps) or (pps > alpha)
        low_n = ny < 6

        if low_n or bad_p:
            self.output.setText(
                "BLOCKED\n"
                f"Причина: {'n_years < 6' if low_n else ''} "
                f"{'p_shift > 0.05 / nan' if bad_p else ''}\n"
                f"n_years={ny}, p_shift={pps}"
            )
            return

        spec = r["spec"]

        spec.climate_var = getattr(spec, "climate_var", None) or self.climate_var.currentText()
        climate_var = spec.climate_var

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
        try:
            # 1) Eco-настройки из UI
            river = self.river.currentText()
            geom = self.geom.currentText()
            impact = self.impact.currentText()

            scale = self.scale.currentText()
            eco_metric = self.eco_metric.currentText()

            filters = {}

            aff = self.affor.currentData()
            if aff is not None:
                filters["afforestation"] = {"in": aff}

            geom_value = self.geom.currentText()
            if geom_value != "All":
                filters["geomorph_level"] = geom_value

            profile_value = self.profile.currentText()
            if profile_value != "All":
                filters["source_file"] = profile_value

            impact_value = self.impact.currentText()
            if impact_value != "All":
                filters["impact_type"] = impact_value

            periods = self._parse_str_list(self.periods_edit.text())
            lags = self._parse_int_list(self.lags_edit.text())
            windows = self._parse_int_list(self.windows_edit.text())

            df0 = load_processed()
            df1 = apply_filters(df0, filters)
            self.output.setText(self.output.text() + f"\nAfter filters: rows={len(df1)}")

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

                            # df — это joined по годам, но r/p лежат одинаковыми во всех строках
                            if len(df) == 0:
                                pearson_r = float("nan");
                                spearman_rho = float("nan")
                                pearson_p = float("nan");
                                pearson_p_shift = float("nan")
                                spearman_p = float("nan")
                                n_years = 0;
                                year_min = None;
                                year_max = None
                            else:
                                pearson_r = float(df["pearson_r"].iloc[0])
                                spearman_rho = float(df["spearman_rho"].iloc[0])
                                pearson_p = float(df["pearson_p"].iloc[0])
                                pearson_p_shift = float(df["pearson_p_shift"].iloc[0])
                                spearman_p = float(df["spearman_p"].iloc[0])
                                n_years = int(df["n_years"].iloc[0])
                                year_min = int(df["year_min"].iloc[0]) if pd.notna(df["year_min"].iloc[0]) else None
                                year_max = int(df["year_max"].iloc[0]) if pd.notna(df["year_max"].iloc[0]) else None

                            years_txt = "" if (year_min is None or year_max is None) else f"{year_min}–{year_max}"

                            rows.append({
                                "period": period, "lag": lag, "window": window, "n": n,
                                "pearson_r": pearson, "spearman_rho": spearman, "abs_r": absr,
                                "spec": spec,  # сохраним spec для клика по строке
                                "n_years": n_years,
                                "pearson_p": pearson_p,
                                "pearson_p_shift": pearson_p_shift,
                                "spearman_p": spearman_p,
                                "years": years_txt,
                            })

                        except Exception as e:
                            rows.append({
                                "period": period, "lag": lag, "window": window, "n": 0,
                                "pearson_r": float("nan"), "spearman_rho": float("nan"), "abs_r": float("nan"),
                                "spec": spec,
                                "error": str(e),
                            })

            # 3) Заполняем таблицу
            self._batch_rows = rows
            # гарантируем, что таблица в корректном состоянии
            self.batch_table.setVisible(True)

            self.batch_table.setColumnCount(12)
            self.batch_table.setHorizontalHeaderLabels([
                "period", "lag", "window", "n_years",
                "pearson_r", "|r|", "p", "p_shift",
                "spearman_rho", "p_s",
                "years", "plot"
            ])

            self.batch_table.horizontalHeader().setVisible(True)
            self.batch_table.verticalHeader().setVisible(True)
            self.batch_table.setShowGrid(True)

            # важно: очистить старые items (setRowCount НЕ удаляет содержимое полностью)
            self.batch_table.setSortingEnabled(False)
            self.batch_table.setUpdatesEnabled(False)
            self.batch_table.clearContents()
            self.batch_table.setRowCount(len(rows))

            for i, r in enumerate(rows):
                def put(col, text: str, sort_value=None):
                    item = QTableWidgetItem("" if text is None else str(text))
                    if sort_value is not None:
                        item.setData(0, sort_value)  # DisplayRole used for sorting
                    self.batch_table.setItem(i, col, item)

                # 0 period
                put(0, r.get("period", ""))

                # 1 lag, 2 window
                lag = r.get("lag", None)
                win = r.get("window", None)
                put(1, "" if lag is None else str(lag), sort_value=lag)
                put(2, "" if win is None else str(win), sort_value=win)

                # 3 n_years
                ny = r.get("n_years", r.get("n", None))
                put(3, "" if ny is None else str(ny), sort_value=ny)

                # 4 pearson_r
                pr = r.get("pearson_r", float("nan"))
                put(4, "" if not isinstance(pr, float) or math.isnan(pr) else f"{pr:.3f}", sort_value=pr)

                # 5 |r|
                ar = r.get("abs_r", float("nan"))
                put(5, "" if not isinstance(ar, float) or math.isnan(ar) else f"{ar:.3f}", sort_value=ar)

                # 6 p (pearson)
                pp = r.get("pearson_p", float("nan"))
                put(6, "" if not isinstance(pp, float) or math.isnan(pp) else f"{pp:.3g}", sort_value=pp)

                # 7 p_shift (pearson circular shift)
                pps = r.get("pearson_p_shift", float("nan"))
                put(7, "" if not isinstance(pps, float) or math.isnan(pps) else f"{pps:.3g}", sort_value=pps)

                # 8 spearman_rho
                sr = r.get("spearman_rho", float("nan"))
                put(8, "" if not isinstance(sr, float) or math.isnan(sr) else f"{sr:.3f}", sort_value=sr)

                # 9 p_s (spearman)
                sp = r.get("spearman_p", float("nan"))
                put(9, "" if not isinstance(sp, float) or math.isnan(sp) else f"{sp:.3g}", sort_value=sp)

                # 10 years
                years_txt = r.get("years", "")
                put(10, years_txt)

                # 11 plot
                put(11, "click")

                alpha = 0.05
                ny = int(r.get("n_years", r.get("n", 0)) or 0)
                pps = r.get("pearson_p_shift", float("nan"))

                bad_p = (not isinstance(pps, float)) or math.isnan(pps) or (pps > alpha)
                low_n = ny < 6

                if low_n or bad_p:
                    for c in range(self.batch_table.columnCount()):
                        it = self.batch_table.item(i, c)
                        if it:
                            it.setFlags(it.flags() & ~Qt.ItemIsSelectable)


                # индекс исходной строки для корректного клика после сортировки
                self.batch_table.item(i, 0).setData(256, i)

            self.batch_table.setUpdatesEnabled(True)
            self.batch_table.setSortingEnabled(True)

            # (опционально) сразу сортируем по |r| убыванию
            self.batch_table.sortItems(5, Qt.DescendingOrder)

            # ✅ вернуть нормальные пропорции справа после climate→batch
            if hasattr(self, "right_splitter"):
                self.right_splitter.setSizes([260, 640, 80])

            self.batch_table.viewport().update()
            self.batch_table.repaint()

            self.output.setText(
                f"BATCH DONE\n"
                f"Eco: scale={scale}, metric={eco_metric}\n"
                f"Filters: {filters}\n"
                f"Combos: {total}\n"
                f"Tip: click a row to plot scatter."
            )
        except Exception as e:
            self.output.setText(f"run_batch ERROR:\n{type(e).__name__}: {e}")
            raise

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
        is_panel = (mode_code == "panel_climate")
        is_panel_batch = (mode_code == "panel_climate_batch")
        is_panel_batch = (mode_code == "panel_climate_batch")

        self.lag_combo.setVisible(is_panel)
        self.window_combo.setVisible(is_panel)

        # eco controls (нужны и в panel тоже)
        self.eco_metric.setEnabled(is_eco or is_batch or is_panel or is_panel_batch)
        self.scale.setEnabled(is_eco or is_batch or is_panel or is_panel_batch)

        # main run buttons
        self.run_btn.setVisible(not (is_batch or is_panel_batch))
        self.run_batch_btn.setVisible(is_batch or is_panel_batch)

        # climate controls
        self.period_combo.setEnabled(is_climate or is_panel)  # period нужен в climate и panel
        self.climate_var.setEnabled(is_climate or is_batch or is_panel or is_panel_batch)

        # batch controls visibility (для eco_vs_climate_batch и panel_climate_batch)
        for w in [self.periods_edit, self.lags_edit, self.windows_edit, self.batch_table]:
            w.setVisible(is_batch or is_panel_batch)

        # plot/table visibility в panel single
        if is_panel:
            # self.batch_table.setVisible(False)  # в single panel можно без таблицы батча
            self.plot_label.setVisible(False)
        else:
            self.plot_label.setVisible(True)  # чтобы другие режимы не “ломались”

        # site filters:
        # - in climate mode: disable (since climate is regional bbox)
        # - in batch mode: keep enabled (eco filters are needed)
        self.geom.setEnabled(not is_climate)
        self.impact.setEnabled(not is_climate)
        self.affor.setEnabled(not is_climate)

        if hasattr(self, "right_splitter"):
            mode = self.mode.currentData()

            if mode == "eco_vs_climate_batch":
                # таблица заметная + график + чуть лога
                self.right_splitter.setSizes([260, 640, 80])
            elif mode == "climate":
                # в climate таблица может быть меньше, но не исчезать
                self.right_splitter.setSizes([120, 760, 80])
            else:
                # на всякий случай
                self.right_splitter.setSizes([220, 660, 80])

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

        # --- Profiles ---
        profiles = (
            df["source_file"]
            .astype("string")
            .dropna()
            .unique()
            .tolist()
        )
        profiles = sorted([x for x in profiles if x and x != "nan"])

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
        self.profile.clear()
        self.geom.clear()
        self.impact.clear()

        self.profile.addItem("All")
        self.profile.addItems(profiles)

        self.geom.addItem("All")
        self.geom.addItems(geomorphs)

        self.impact.addItem("All")
        self.impact.addItems(impacts)

        self.run_btn.setEnabled(bool(profiles) and bool(geomorphs))

        self.output.setText(
            f"Loaded rows: {len(df)} | "
            f"Profiles: {len(profiles)}, "
            f"Geomorph: {len(geomorphs)}, "
            f"Impact: {len(impacts)}"
        )

    def run(self):
        mode_text = self.mode.currentText()
        mode_code = self.mode.currentData()

        # msg = f"MODE DEBUG:\ntext='{mode_text}'\ncode='{mode_code}'"
        # if hasattr(self.output, "setPlainText"):
        #     self.output.setPlainText(msg)
        # else:
        #     self.output.setText(msg)

        # 1) Собираем фильтры из UI
        filters = {}

        aff = self.affor.currentData()
        if aff is not None:
            filters["afforestation"] = {"in": aff}

        geom_value = self.geom.currentText()
        if geom_value != "All":
            filters["geomorph_level"] = geom_value

        profile_value = self.profile.currentText()
        if profile_value != "All":
            filters["source_file"] = profile_value

        impact_value = self.impact.currentText()
        if impact_value != "All":
            filters["impact_type"] = impact_value

        msg = (
            "RUN\n"
            f"text='{mode_text}'\n"
            f"code='{mode_code}'\n"
            f"filters={filters}"
        )
        self.output.setText(msg)

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

        if mode_code == "panel_climate":
            try:
                from core.panel_model import run_panel_model

                aff = self.affor.currentData()

                filters = {}

                if aff is not None:
                    filters["afforestation"] = aff

                geom_value = self.geom.currentText()
                if geom_value != "All":
                    filters["geomorph_level"] = geom_value

                impact_value = self.impact.currentText()
                if impact_value != "All":
                    filters["impact_type"] = impact_value

                profile_value = self.profile.currentText()
                if profile_value != "All":
                    filters["source_file"] = profile_value

                spec = {
                    "scale": self.scale.currentText(),
                    "metric": self.eco_metric.currentText(),
                    "climate_var": self.climate_var.currentText(),
                    "period": self.period_combo.currentText(),
                    "lag": int(self.lag_combo.currentText()),
                    "window": int(self.window_combo.currentText()),
                    "filters": filters,
                }

                df = run_panel_model(spec)

                # ВАЖНО: принудительно показать таблицу
                self.batch_table.setVisible(True)
                self._show_df_in_table(df)

                self.output.setText(f"PANEL OK\nn_rows={len(df)}\n{df.to_string(index=False)}")
            except Exception as e:
                import traceback
                self.output.setText("PANEL ERROR:\n" + traceback.format_exc())

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



