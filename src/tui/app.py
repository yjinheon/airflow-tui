from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Header, Footer, Static
from textual import work
from typing import Dict, Optional

from src.tui.clients.base import AirflowClient
from src.tui.clients.registry import create_client
from src.tui.widgets.navigation_tree import NavigationTree
from src.tui.widgets.main_content import MainContentArea
from src.tui.widgets.dag_management import DAGManagement
from src.tui.widgets.simple_navigation import SimpleNavigation


class AirflowTUI(App):
    CSS = """
    #main-content-area {
        width: 70%;
        padding: 0;
    }
    
    #content-display {
        height: auto;
        margin: 1;
    }
    
    .panel-title {
        text-style: bold;
        background: $primary;
        color: $text;
        padding: 1;
        text-align: center;
    }
    
    #main-content {
        height: 100%;
        background: $panel;
    }
    
    DAGManagement {
        height: 100%;
        width: 100%;
    }
    
    SimpleNavigation {
        width: 30%;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("d", "toggle_dark", "Dark Mode"),
        ("r", "refresh", "Refresh"),
        ("t", "trigger_dag", "Trigger DAG"),
        ("p", "pause_dag", "Pause/Resume"),
        ("l", "view_logs", "View Logs"),
        ("f", "full_screen_logs", "Full Logs"),
        ("b", "backfill", "Backfill"),
        ("a", "add_connection", "Add Connection"),
        ("ctrl+c", "quit", "Quit"),
    ]

    def __init__(self, client_type: str = "cli", **client_kwargs):
        super().__init__()
        self.client_type = client_type
        self.client_kwargs = client_kwargs
        self.airflow_client: Optional[AirflowClient] = None
        self.nav_tree: Optional[NavigationTree] = None
        self.simple_nav: Optional[SimpleNavigation] = None
        self.main_content: Optional[MainContentArea] = None
        self.dag_management: Optional[DAGManagement] = None
        self.current_selection = {"type": "dashboard"}

        # 클라이언트 생성
        try:
            self.airflow_client = create_client(client_type, **client_kwargs)
        except Exception as e:
            self.exit(f"Failed to create Airflow client: {str(e)}")

    def compose(self) -> ComposeResult:
        yield Header()

        with Horizontal():
            # 좌측 네비게이션 패널
            self.simple_nav = SimpleNavigation(self)
            yield self.simple_nav

            # 메인 컨텐츠 영역
            with Vertical(id="main-content-area"):
                self.main_content = MainContentArea(self)
                yield self.main_content

                # DAG 관리 인터페이스 (기본적으로 숨김)
                self.dag_management = DAGManagement(self)
                self.dag_management.display = False
                yield self.dag_management

        yield Footer()

    def on_mount(self) -> None:
        # 클라이언트 초기화 및 대시보드 표시
        self.run_worker(self.init_application)

        # 자동 새로고침 설정 (30초마다)
        self.set_interval(30, self.auto_refresh)

    async def init_application(self):
        """애플리케이션 초기화"""
        try:
            # 클라이언트 초기화
            if not self.airflow_client.is_initialized:
                self.notify("🔄 Initializing Airflow client...")
                await self.airflow_client.initialize()

            # 대시보드 표시
            self.notify("📊 Loading dashboard...")
            await self.main_content.update_content("dashboard")

            # 성공 메시지
            self.notify("✅ Connected to Airflow successfully!")

        except Exception as e:
            # Error logging and fallback handling
            error_msg = f"❌ Failed to connect to Airflow: {str(e)}"
            self.notify(error_msg)
            print(f"Application initialization error: {str(e)}")

            # Fallback: Display error message in main content
            try:
                content_widget = self.main_content.query_one("#content-display")
                content_widget.renderable = f"[bold red]Connection Error[/bold red]\n\n{str(e)}\n\n[dim]Please check your Airflow installation and try again.[/dim]"
                content_widget.refresh()
            except Exception as fallback_error:
                print(f"Fallback error display failed: {str(fallback_error)}")

    async def handle_navigation_selection(self, data: Dict):
        self.current_selection = data

        # DAG 관리 인터페이스 표시/숨김
        selection_type = data.get("type", "unknown")
        if selection_type == "dags_root":
            # DAGs 루트 선택 시 DAG 관리 인터페이스 표시
            self.main_content.display = False
            self.dag_management.display = True
            self.dag_management.load_dag_list()
            # DAG 테이블에 포커스 설정
            self.call_after_refresh(self.dag_management.focus_dag_table)
        else:
            self.main_content.display = True
            self.dag_management.display = False
            await self.main_content.update_content(data["type"], data)

        if selection_type == "dag":
            dag_id = data.get("dag_id", "")
            self.sub_title = f"Selected: DAG {dag_id}"
        elif selection_type == "task":
            dag_id = data.get("dag_id", "")
            task_id = data.get("task_id", "")
            self.sub_title = f"Selected: Task {dag_id}.{task_id}"
        elif selection_type == "dags_root":
            self.sub_title = "DAG Management"
        else:
            self.sub_title = f"Selected: {selection_type.title()}"

    def handle_dag_selection_from_management(self, dag_id: str, dag_info=None):
        self.current_selection = {"type": "dag", "dag_id": dag_id, "dag_info": dag_info}
        self.sub_title = f"Selected: DAG {dag_id}"

    # 액션 메서드들
    def action_refresh(self):
        self.notify("🔄 Refreshing...")

        # 현재 뷰 새로고침
        if self.current_selection.get("type") == "dags_root" and self.dag_management:
            self.run_worker(self.dag_management.refresh_data)
        elif self.main_content:
            self.run_worker(self.main_content.refresh_current_view)

    def action_trigger_dag(self):
        dag_id = None
        if self.current_selection.get("type") == "dag":
            dag_id = self.current_selection.get("dag_id")
        elif self.current_selection.get("type") == "dags_root" and self.dag_management:
            # DAG 관리 인터페이스에서 선택된 DAG 사용
            dag_id = self.dag_management.selected_dag

        if dag_id:
            self.trigger_dag_async(dag_id)
        else:
            self.notify("❌ Please select a DAG first")

    @work
    async def trigger_dag_async(self, dag_id: str):
        try:
            self.notify(f"🚀 Triggering DAG {dag_id}...")
            success = await self.airflow_client.trigger_dag(dag_id)

            if success:
                self.notify(f"✅ DAG {dag_id} triggered successfully!")
                # 뷰 새로고침
                if (
                    self.current_selection.get("type") == "dags_root"
                    and self.dag_management
                ):
                    await self.dag_management.refresh_data()
                elif self.main_content:
                    await self.main_content.refresh_current_view()
            else:
                self.notify(f"❌ Failed to trigger DAG {dag_id}")

        except Exception as e:
            self.notify(f"❌ Error triggering DAG: {str(e)}")

    def action_pause_dag(self):
        dag_id = None
        if self.current_selection.get("type") == "dag":
            dag_id = self.current_selection.get("dag_id")
        elif self.current_selection.get("type") == "dags_root" and self.dag_management:
            # DAG 관리 인터페이스에서 선택된 DAG 사용
            dag_id = self.dag_management.selected_dag

        if dag_id:
            self.toggle_dag_pause_async(dag_id)
        else:
            self.notify("❌ Please select a DAG first")

    @work
    async def toggle_dag_pause_async(self, dag_id: str):
        try:
            # 현재 상태 확인
            is_paused = await self.airflow_client.is_dag_paused(dag_id)

            if is_paused:
                success = await self.airflow_client.unpause_dag(dag_id)
                action = "resumed"
            else:
                success = await self.airflow_client.pause_dag(dag_id)
                action = "paused"

            if success:
                self.notify(f"✅ DAG {dag_id} {action} successfully!")
                # 뷰 새로고침
                if (
                    self.current_selection.get("type") == "dags_root"
                    and self.dag_management
                ):
                    await self.dag_management.refresh_data()
                elif self.main_content:
                    await self.main_content.refresh_current_view()
            else:
                self.notify(f"❌ Failed to {action.split('d')[0]} DAG {dag_id}")

        except Exception as e:
            self.notify(f"❌ Error toggling DAG pause: {str(e)}")

    def action_view_logs(self):
        if self.current_selection.get("type") == "task":
            dag_id = self.current_selection.get("dag_id")
            task_id = self.current_selection.get("task_id")
            self.notify(f"📜 Viewing logs for {dag_id}.{task_id}")
            # 현재 task 뷰에서 이미 로그를 보여주고 있으므로 알림만
        else:
            self.notify("❌ Please select a task to view logs")

    def action_full_screen_logs(self):
        if self.current_selection.get("type") == "task":
            self.notify("📜 Full screen logs (Coming soon...)")
        else:
            self.notify("❌ Please select a task first")

    def action_backfill(self):
        if self.current_selection.get("type") == "dag":
            dag_id = self.current_selection.get("dag_id")
            self.notify(f"🔄 Backfill for {dag_id} (Coming soon...)")
        else:
            self.notify("❌ Please select a DAG first")

    def action_add_connection(self):
        self.notify("🔗 Add connection (Coming soon...)")

    async def auto_refresh(self):
        """자동 새로고침 (30초마다)"""
        try:
            # 대시보드인 경우에만 자동 새로고침
            if self.current_selection.get("type") == "dashboard" and self.main_content:
                await self.main_content.refresh_current_view()
        except Exception:
            # 자동 새로고침 실패는 조용히 무시
            pass

    def on_app_suspend(self):
        self.notify("⏸️ Application suspended")

    def on_app_resume(self):
        self.notify("▶️ Application resumed")
        # 재개시 새로고침
        self.action_refresh()


def create_app(client_type: str = "cli", **kwargs) -> AirflowTUI:
    """airflow tui app factory function"""
    return AirflowTUI(client_type=client_type, **kwargs)
