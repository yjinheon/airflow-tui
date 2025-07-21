"""
Simple Navigation Widget
Simplified navigation for the new DAG management interface
"""

from textual.widgets import Select, Static
from textual.containers import Vertical
from textual.widget import Widget
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.tui.app import AirflowTUI


class SimpleNavigation(Widget):
    """간단한 네비게이션 위젯"""

    DEFAULT_CSS = """
    SimpleNavigation {
        width: 30%;
        height: 100%;
        background: $panel;
        border-right: solid $accent;
        min-width: 25;
        max-width: 50;
    }
    
    #nav-header {
        height: 3;
        background: $primary;
        color: $text;
        padding: 1;
        text-align: center;
    }
    
    #nav-select {
        margin: 1;
        height: 3;
    }
    
    #nav-info {
        margin: 1;
        height: 1fr;
        background: $surface;
        border: solid $accent;
        padding: 1;
    }
    """

    def __init__(self, app_instance: "AirflowTUI"):
        super().__init__()
        self.app_instance = app_instance

    def compose(self):
        """위젯 구성"""
        yield Static("🧭 Navigation", id="nav-header")

        yield Select(
            options=[
                ("📊 Dashboard", "dashboard"),
                ("🔄 DAG Management", "dags_root"),
                ("🔗 Connections", "connections"),
                ("📜 All Logs", "all_logs"),
                ("⚙️ Admin", "admin"),
            ],
            value="dashboard",
            id="nav-select",
        )

        yield Static(
            """[bold]Quick Navigation[/bold]

Select a view from the dropdown above:

• [bold]Dashboard[/bold] - System overview
• [bold]DAG Management[/bold] - Full DAG interface
• [bold]Connections[/bold] - Connection management
• [bold]All Logs[/bold] - System logs
• [bold]Admin[/bold] - Administration

[dim]Use keyboard shortcuts for quick actions:[/dim]
• [bold]T[/bold] - Trigger DAG
• [bold]P[/bold] - Pause/Resume DAG
• [bold]R[/bold] - Refresh
• [bold]Q[/bold] - Quit""",
            id="nav-info",
        )

    def on_select_changed(self, event):
        """네비게이션 선택 변경"""
        if event.control.id == "nav-select":
            selection_data = {"type": event.value}
            self.app_instance.run_worker(
                self.app_instance.handle_navigation_selection(selection_data)
            )

