import sys
import backend
from datetime import datetime, time
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QFormLayout,
    QGroupBox, QLineEdit, QPushButton, QScrollArea, QCheckBox, QTimeEdit,
    QWizard, QWizardPage, QLabel, QPlainTextEdit, QSizePolicy, QSystemTrayIcon, QMenu
)
from PySide6.QtCore import QThread, QObject, Signal, Slot, Qt, QTime, QTimer
from PySide6.QtGui import QPalette, QColor, QPixmap, QIcon, QAction

# --- Worker thread for running backend tasks ---
class Worker(QObject):
    progress = Signal(str)
    finished = Signal(bool)

    def __init__(self, task_func, *args):
        super().__init__()
        self.task_func = task_func
        self.args = args

    @Slot()
    def run(self):
        try:
            success = self.task_func(*self.args, progress_callback=self.progress.emit)
            self.finished.emit(success)
        except Exception as e:
            self.progress.emit(f"An error occurred: {e}")
            self.finished.emit(False)

# --- Main Application Window ---
class FynixPlayerWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Fynix Library Builder")
        self.setMinimumSize(800, 600)
        self.thread = None
        self.worker = None
        self.category_checkboxes = []
        self.server_editors = {}
        self.last_auto_update_date = None

        # --- System Tray Icon ---
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(QIcon("assets/FLB.png"))
        self.tray_icon.setToolTip("Fynix Library Builder")

        tray_menu = QMenu()
        show_action = tray_menu.addAction("Show")
        show_action.triggered.connect(self.show)
        quit_action = tray_menu.addAction("Quit")
        quit_action.triggered.connect(QApplication.instance().quit)
        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.activated.connect(self.on_tray_icon_activated)
        self.tray_icon.show()

        # Central widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)

        # Left column
        left_column = QVBoxLayout()
        main_layout.addLayout(left_column)

        # Right column
        right_column = QVBoxLayout()
        main_layout.addLayout(right_column)

        # Logo
        logo_label = QLabel()
        pixmap = QPixmap("assets/FLB.png")
        logo_label.setPixmap(pixmap.scaled(128, 128, Qt.KeepAspectRatio, Qt.SmoothTransformation))
        logo_label.setAlignment(Qt.AlignCenter)
        left_column.addWidget(logo_label)

        # Server Editor
        left_column.addWidget(self.build_server_editor())
        
        # Library Actions
        left_column.addWidget(self.build_actions_box())

        # Auto Update
        left_column.addWidget(self.build_auto_update_box())

        # Statistics
        left_column.addWidget(self.build_statistics_box())
        left_column.addStretch()

        # Category Editors
        vod_editor = self.build_category_editor("VOD Categories", "vod", "Select VOD categories to include in your library.")
        series_editor = self.build_category_editor("Series Categories", "series", "Select series categories to include in your library.")
        right_column.addWidget(vod_editor)
        right_column.addWidget(series_editor)

        self.update_statistics_ui() # Initial update
        self.load_and_set_schedule() # Load schedule and start timer

    def closeEvent(self, event):
        event.ignore()
        self.hide()
        self.tray_icon.showMessage(
            "Fynix Library Builder",
            "Application was minimized to the system tray.",
            QSystemTrayIcon.Information,
            2000
        )

    @Slot(QSystemTrayIcon.ActivationReason)
    def on_tray_icon_activated(self, reason):
        if reason == QSystemTrayIcon.Trigger:
            if self.isHidden():
                self.show()
            else:
                self.hide()

    def build_server_editor(self):
        group = QGroupBox("Server Configuration")
        group.setToolTip("Update your IPTV provider connection details here.")
        layout = QVBoxLayout(group)
        
        servers = backend.get_servers()
        if not servers:
            layout.addWidget(QLabel("No servers found."))
        else:
            server = servers[0]
            server_id = server['id']
            
            editors = {}
            editors['name'] = QLineEdit(server['name'])
            editors['url'] = QLineEdit(server['url'])
            editors['username'] = QLineEdit(server['username'])
            editors['password'] = QLineEdit(server['password'])
            editors['password'].setEchoMode(QLineEdit.Password)
            editors['port'] = QLineEdit(str(server['port']))

            form_layout = QFormLayout()
            form_layout.addRow("Server Name:", editors['name'])
            form_layout.addRow("Server URL:", editors['url'])
            form_layout.addRow("Username:", editors['username'])
            form_layout.addRow("Password:", editors['password'])
            form_layout.addRow("Port:", editors['port'])
            layout.addLayout(form_layout)

            self.server_editors[server_id] = editors

        return group

    def build_category_editor(self, title, content_type, description):
        group = QGroupBox(title)
        group.setToolTip(description)
        
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        
        container = QWidget()
        layout = QVBoxLayout(container)
        scroll_area.setWidget(container)

        layout.addWidget(QLabel(description))

        categories = backend.get_categories()
        for cat in categories:
            if cat['content_type'] == content_type:
                check = QCheckBox(cat['category_name'])
                check.setChecked(bool(cat['visible']))
                self.category_checkboxes.append((cat['id'], check))
                layout.addWidget(check)
        
        layout.addStretch()

        main_layout = QVBoxLayout(group)
        main_layout.addWidget(scroll_area)
        return group

    def build_actions_box(self):
        group = QGroupBox("Actions")
        layout = QVBoxLayout(group)

        self.save_button = QPushButton("Save All Changes")
        self.save_button.setToolTip("Saves server, category, and schedule settings.")
        self.save_button.clicked.connect(self.save_all_changes)
        layout.addWidget(self.save_button)

        self.update_library_button = QPushButton("Update Library")
        self.update_library_button.setToolTip("Scans for new content and creates .strm files.")
        self.update_library_button.clicked.connect(self.run_library_update)
        layout.addWidget(self.update_library_button)

        self.clear_cache_button = QPushButton("Clear Cache")
        self.clear_cache_button.setToolTip("Clears all cached metadata. Use if images or text are outdated.")
        self.clear_cache_button.clicked.connect(self.run_clear_cache)
        layout.addWidget(self.clear_cache_button)
        
        self.status_label = QLabel("Ready.")
        layout.addWidget(self.status_label)

        return group

    def build_auto_update_box(self):
        group = QGroupBox("Auto Update")
        layout = QFormLayout(group)

        self.schedule_checkbox = QCheckBox("Enable daily auto-update")
        self.schedule_time_edit = QTimeEdit()
        self.schedule_time_edit.setDisplayFormat("HH:mm")

        layout.addRow(self.schedule_checkbox)
        layout.addRow("Update Time:", self.schedule_time_edit)

        return group

    def build_statistics_box(self):
        group = QGroupBox("Database Statistics")
        layout = QFormLayout(group)

        self.stats_labels = {
            'movies': QLabel("0 / 0"),
            'series': QLabel("0 / 0"),
            'episodes': QLabel("0 / 0"),
        }

        layout.addRow("Visible Movies:", self.stats_labels['movies'])
        layout.addRow("Visible Series:", self.stats_labels['series'])
        layout.addRow("Visible Episodes:", self.stats_labels['episodes'])

        return group

    def update_statistics_ui(self):
        stats = backend.get_database_statistics()
        self.stats_labels['movies'].setText(f"{stats['visible_movies']} / {stats['total_movies']}")
        self.stats_labels['series'].setText(f"{stats['visible_series']} / {stats['total_series']}")
        self.stats_labels['episodes'].setText(f"{stats['visible_episodes']} / {stats['total_episodes']}")

    def save_all_changes(self):
        # Save server details
        for server_id, editors in self.server_editors.items():
            backend.update_server(
                server_id,
                editors['name'].text(),
                editors['url'].text(),
                editors['username'].text(),
                editors['password'].text(),
                editors['port'].text()
            )

        # Save category visibility
        for cat_id, checkbox in self.category_checkboxes:
            backend.update_category_visibility(cat_id, 1 if checkbox.isChecked() else 0)
        
        # Save schedule
        backend.save_schedule(
            self.schedule_checkbox.isChecked(),
            self.schedule_time_edit.time().toString("HH:mm")
        )

        self.statusBar().showMessage("All changes saved successfully!", 5000)
        self.status_label.setText("Changes saved.")
        self.update_statistics_ui()

    def load_and_set_schedule(self):
        schedule = backend.load_schedule()
        self.schedule_checkbox.setChecked(schedule['enabled'])
        self.schedule_time_edit.setTime(QTime.fromString(schedule['time'], "HH:mm"))

        self.schedule_timer = QTimer(self)
        self.schedule_timer.timeout.connect(self.check_schedule)
        self.schedule_timer.start(60000) # Check every 60 seconds

    def check_schedule(self):
        schedule = backend.load_schedule()
        if not schedule['enabled']:
            return

        now = datetime.now()
        scheduled_time = datetime.strptime(schedule['time'], "%H:%M").time()

        # Check if it's time to run and if it hasn't run today
        if now.time() >= scheduled_time and now.date() != self.last_auto_update_date:
            self.last_auto_update_date = now.date()
            self.status_label.setText(f"Starting scheduled update at {now.strftime('%H:%M')}:")
            self.run_library_update()

    def run_task(self, task_func, *args):
        if self.thread is not None and self.thread.isRunning():
            self.statusBar().showMessage("A task is already running.", 3000)
            return

        self.set_buttons_enabled(False)
        self.thread = QThread()
        self.worker = Worker(task_func, *args)
        self.worker.moveToThread(self.thread)

        self.worker.progress.connect(self.update_status)
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.task_finished)
        
        self.thread.start()

    def run_library_update(self):
        self.run_task(backend.run_library_update)

    def run_clear_cache(self):
        self.run_task(backend.run_clear_cache)

    def set_buttons_enabled(self, enabled):
        self.save_button.setEnabled(enabled)
        self.update_library_button.setEnabled(enabled)
        self.clear_cache_button.setEnabled(enabled)

    @Slot(str)
    def update_status(self, message):
        self.status_label.setText(message)

    @Slot(bool)
    def task_finished(self, success):
        self.thread.quit()
        self.thread.wait()
        self.set_buttons_enabled(True)
        if success:
            self.status_label.setText("Task completed successfully.")
            self.statusBar().showMessage("Success!", 3000)
            self.update_statistics_ui()
        else:
            self.status_label.setText("Task failed. Check logs.")
        self.thread = None
        self.worker = None

# --- Initial Setup Wizard ---
class SetupWizard(QWizard):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Fynix Library Builder Setup")
        self.addPage(self.create_welcome_page())
        self.addPage(self.create_server_page())
        self.addPage(self.create_folders_page())
        self.addPage(self.create_confirm_page())

    def create_welcome_page(self):
        page = QWizardPage()
        page.setTitle("Welcome")
        layout = QVBoxLayout(page)
        logo_label = QLabel()
        pixmap = QPixmap("assets/FLB.png")
        logo_label.setPixmap(pixmap.scaled(128, 128, Qt.KeepAspectRatio, Qt.SmoothTransformation))
        logo_label.setAlignment(Qt.AlignCenter)
        welcome_label = QLabel("Welcome to Fynix Library Builder.\n\nThis wizard will guide you through the initial setup.")
        welcome_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(logo_label)
        layout.addWidget(welcome_label)
        return page

    def create_server_page(self):
        page = QWizardPage()
        page.setTitle("Server Details")
        page.setSubTitle("Enter the connection details for your IPTV provider.")
        layout = QVBoxLayout(page)
        self.server_name_entry = QLineEdit()
        self.server_url_entry = QLineEdit()
        self.server_user_entry = QLineEdit()
        self.server_pass_entry = QLineEdit()
        self.server_pass_entry.setEchoMode(QLineEdit.Password)
        self.server_port_entry = QLineEdit("80")
        
        layout.addWidget(QLabel("A friendly name for your server (e.g., 'My Provider'):"))
        layout.addWidget(self.server_name_entry)
        layout.addWidget(QLabel("Server URL (e.g., http://myprovider.com:8080):"))
        layout.addWidget(self.server_url_entry)
        layout.addWidget(QLabel("Username:"))
        layout.addWidget(self.server_user_entry)
        layout.addWidget(QLabel("Password:"))
        layout.addWidget(self.server_pass_entry)
        layout.addWidget(QLabel("Port (usually 80 or 8080):"))
        layout.addWidget(self.server_port_entry)
        return page

    def create_folders_page(self):
        page = QWizardPage()
        page.setTitle("Library Folders")
        page.setSubTitle("Select the folders where your movie and series .strm files will be saved.")
        layout = QVBoxLayout(page)
        self.movie_folder_entry = QLineEdit()
        self.series_folder_entry = QLineEdit()
        # In a real app, these would be folder selection dialogs
        layout.addWidget(QLabel("Movie Library Path:"))
        layout.addWidget(self.movie_folder_entry)
        layout.addWidget(QLabel("Series Library Path:"))
        layout.addWidget(self.series_folder_entry)
        return page

    def create_confirm_page(self):
        page = QWizardPage()
        page.setTitle("Confirmation")
        layout = QVBoxLayout(page)
        layout.addWidget(QLabel("Ready to set up the library. This may take a long time."))
        self.progress_text = QPlainTextEdit()
        self.progress_text.setReadOnly(True)
        layout.addWidget(self.progress_text)
        return page

    def accept(self):
        # Disable finish button to prevent multiple clicks
        self.button(QWizard.FinishButton).setEnabled(False)
        self.button(QWizard.CancelButton).setEnabled(False)

        server_details = (
            self.server_name_entry.text(), self.server_url_entry.text(),
            self.server_user_entry.text(), self.server_pass_entry.text(),
            self.server_port_entry.text()
        )
        movie_path = self.movie_folder_entry.text()
        series_path = self.series_folder_entry.text()

        self.thread = QThread()
        self.worker = Worker(backend.run_initial_setup, server_details, movie_path, series_path)
        self.worker.moveToThread(self.thread)
        self.worker.progress.connect(self.update_progress_text)
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.task_finished)
        self.thread.start()

    @Slot(str)
    def update_progress_text(self, msg):
        self.progress_text.appendPlainText(msg)

    @Slot(bool)
    def task_finished(self, success):
        # Cleanly shut down the thread
        self.thread.quit()
        self.thread.wait()

        if success:
            # Now it's safe to close the wizard
            super().accept()
        else:
            self.progress_text.appendPlainText("\n\nSetup failed. Please check logs and restart the application.")
            # Re-enable buttons on failure
            self.button(QWizard.FinishButton).setEnabled(True)
            self.button(QWizard.CancelButton).setEnabled(True)

def set_dark_theme(app):
    dark_palette = QPalette()
    dark_palette.setColor(QPalette.Window, QColor(53, 53, 53))
    dark_palette.setColor(QPalette.WindowText, QColor(255, 255, 255))
    dark_palette.setColor(QPalette.Base, QColor(25, 25, 25))
    dark_palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
    dark_palette.setColor(QPalette.ToolTipBase, QColor(255, 255, 220))
    dark_palette.setColor(QPalette.ToolTipText, QColor(0, 0, 0))
    dark_palette.setColor(QPalette.Text, QColor(255, 255, 255))
    dark_palette.setColor(QPalette.Button, QColor(53, 53, 53))
    dark_palette.setColor(QPalette.ButtonText, QColor(255, 255, 255))
    dark_palette.setColor(QPalette.BrightText, QColor(255, 0, 0))
    dark_palette.setColor(QPalette.Link, QColor(42, 130, 218))
    dark_palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
    dark_palette.setColor(QPalette.HighlightedText, QColor(0, 0, 0))
    app.setPalette(dark_palette)
    app.setStyleSheet("QToolTip { color: #ffffff; background-color: #2a82da; border: 1px solid white; }")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Fynix Library Builder")
    app.setWindowIcon(QIcon("assets/FLB.png"))
    app.setQuitOnLastWindowClosed(False) # Keep app running in tray
    app.setStyle("Fusion")
    set_dark_theme(app)

    if backend.database_exists():
        window = FynixPlayerWindow()
        window.show()
    else:
        wizard = SetupWizard()
        if wizard.exec():
            # This block will run after the wizard is successfully finished.
            # We can either exit and ask the user to restart, or launch the main window.
            # For a better UX, let's launch the main window.
            window = FynixPlayerWindow()
            window.show()
        else:
            # User cancelled the wizard
            sys.exit(0)

    sys.exit(app.exec())