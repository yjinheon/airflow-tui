from textual.widgets import Static
from textual.containers import ScrollableContainer
from textual.widget import Widget
from textual.reactive import reactive
from textual import work
from typing import Dict, Optional, TYPE_CHECKING

# 절대경로 imports
from src.tui.clients.base import AirflowClient
from src.tui.models.dag import DAGInfo, DAGRun

if TYPE_CHECKING:
    from src.tui.app import AirflowTUI


class MainContentArea(Widget):
    """Main Content Area Widget"""

    current_view = reactive("dashboard")

    def __init__(self, app_instance: "AirflowTUI"):
        super().__init__()
        self.app_instance = app_instance
        self.current_data = {}

    def compose(self):
        with ScrollableContainer(id="main-content"):
            yield Static("Loading...", id="content-display")

    async def update_content(self, content_type: str, data: Optional[Dict] = None):
        self.current_view = content_type
        self.current_data = data or {}

        # Remove loading display - load actual content directly
        print(f"DEBUG: update_content called with type: {content_type}")

        if content_type == "dashboard":
            await self._render_dashboard(mock=False)
        elif content_type == "dag":
            await self._render_dag_details(data, mock=False)
        elif content_type == "task":
            await self._render_task_details(data, mock=False)
        elif content_type == "connections":
            await self._render_connections()
        elif content_type == "all_logs":
            self._render_all_logs()
        elif content_type == "admin":
            await self._render_admin()
        else:
            self._render_placeholder(content_type)

    async def _render_dashboard(self, mock=False):
        if mock:
            stats = {
                "total_dags": 10,
                "active_dags": 8,
                "paused_dags": 2,
                "airflow_version": "2.5.0",
            }
            failures = [
                {"dag_id": "example_dag_1", "execution_date": "2023-10-27T10:00:00"}
            ]
            successes = [{"dag_id": "example_dag_2", "duration": "0:00:30"}]
            running = [{"dag_id": "example_dag_3", "start_date": "2023-10-27T11:00:00"}]
            dashboard_content = self._format_dashboard_content(
                stats, failures, successes, running
            )
            content_widget = self.query_one("#content-display", Static)
            content_widget.update(dashboard_content)
        else:
            await self._load_dashboard_data()

    async def _load_dashboard_data(self):
        try:
            content_widget = self.query_one("#content-display", Static)
            content_widget.update("📊 Loading dashboard data...")

            print("DEBUG: Starting dashboard data load...")

            # Get system statistics
            print("DEBUG: Getting system stats...")
            stats = await self.app_instance.airflow_client.get_system_stats()
            print(f"DEBUG: Got stats: {stats}")

            # Get recent failures/successes
            print("DEBUG: Getting recent failures...")
            failures = await self.app_instance.airflow_client.get_recent_failures()
            print(f"DEBUG: Got failures: {len(failures)} items")

            print("DEBUG: Getting recent successes...")
            successes = await self.app_instance.airflow_client.get_recent_successes()
            print(f"DEBUG: Got successes: {len(successes)} items")

            print("DEBUG: Getting running dags...")
            running = await self.app_instance.airflow_client.get_running_dags()
            print(f"DEBUG: Got running: {len(running)} items")

            dashboard_content = self._format_dashboard_content(
                stats, failures, successes, running
            )
            content_widget.update(dashboard_content)

            print("DEBUG: Dashboard data loaded successfully!")

        except Exception as e:
            error_msg = f"❌ Error loading dashboard: {str(e)}"
            print(f"DEBUG: Dashboard load error: {str(e)}")
            content_widget = self.query_one("#content-display", Static)
            content_widget.update(f"{error_msg}\n\n[dim]Details: {str(e)}[/dim]")

    def _format_dashboard_content(
        self, stats: Dict, failures: list, successes: list, running: list
    ) -> str:
        content = "[bold blue]📊 Airflow Dashboard[/bold blue]\n\n"

        # System Overview
        content += "[bold]📈 System Overview[/bold]\n"
        content += f"• Total DAGs: {stats.get('total_dags', 'N/A')}\n"
        content += f"• Active DAGs: {stats.get('active_dags', 'N/A')}\n"
        content += f"• Paused DAGs: {stats.get('paused_dags', 'N/A')}\n"
        content += f"• Airflow Version: {stats.get('airflow_version', 'N/A')}\n\n"

        # Recent Failures
        content += "[bold red]🔴 Recent Failures[/bold red]\n"
        if failures:
            for failure in failures[:5]:
                content += f"• {failure['dag_id']} ({failure.get('execution_date', 'Unknown')})\n"
        else:
            content += "• No recent failures 🎉\n"
        content += "\n"

        # Recent Successes
        content += "[bold green]✅ Recent Successes[/bold green]\n"
        if successes:
            for success in successes[:5]:
                duration = success.get("duration", "N/A")
                content += f"• {success['dag_id']} - {duration}\n"
        else:
            content += "• No recent successes\n"
        content += "\n"

        # Running DAGs
        content += "[bold yellow]🟡 Currently Running[/bold yellow]\n"
        if running:
            for run in running:
                start_time = run.get("start_date", "Unknown")
                content += f"• {run['dag_id']} (started: {start_time})\n"
        else:
            content += "• No DAGs currently running\n"

        content += "\n[dim]Last updated: Just now[/dim]"
        return content

    async def _render_dag_details(self, data: Dict, mock=False):
        """DAG details"""
        dag_id = data.get("dag_id", "unknown")
        if mock:
            dag_info = DAGInfo(
                dag_id="example_dag",
                is_paused=False,
                is_active=True,
                owner="airflow",
                description="This is an example DAG.",
                schedule_interval="@daily",
                tags=["example"],
            )
            dag_runs = [
                DAGRun(
                    run_id="manual__2023-10-27T10:00:00+00:00",
                    state="success",
                    execution_date="2023-10-27T10:00:00+00:00",
                    run_type="manual",
                    duration="0:00:15",
                )
            ]
            dag_content = self._format_dag_content(dag_info, dag_runs)
            content_widget = self.query_one("#content-display", Static)
            content_widget.update(dag_content)
        else:
            await self._load_dag_details(dag_id)

    async def _load_dag_details(self, dag_id: str):
        try:
            content_widget = self.query_one("#content-display", Static)
            content_widget.renderable = f"🔄 Loading DAG details for {dag_id}..."
            content_widget.refresh()

            # DAG 정보 가져오기
            dag_info = await self.app_instance.airflow_client.get_dag_details(dag_id)
            dag_runs = await self.app_instance.airflow_client.get_dag_runs(
                dag_id, limit=10
            )

            dag_content = self._format_dag_content(dag_info, dag_runs)
            content_widget.update(dag_content)

        except Exception as e:
            content_widget = self.query_one("#content-display", Static)
            content_widget.renderable = f"❌ Error loading DAG {dag_id}: {str(e)}"
            content_widget.refresh()

    def _format_dag_content(self, dag: DAGInfo, runs: list) -> str:
        """Format DAG content for display"""
        status_icon = (
            "🟢"
            if dag.is_active and not dag.is_paused
            else "⏸️" if dag.is_paused else "🔴"
        )

        content = f"[bold blue]🔄 DAG Details: {dag.dag_id}[/bold blue]\n\n"

        content += "[bold]📋 Overview[/bold]\n"
        content += f"• Status: {status_icon} {'Paused' if dag.is_paused else 'Active' if dag.is_active else 'Inactive'}\n"
        content += f"• Schedule: {dag.schedule_interval or 'Not scheduled'}\n"
        content += f"• Owner: {dag.owner or 'Unknown'}\n"
        content += f"• Description: {dag.description or 'No description'}\n"
        content += f"• Tags: {', '.join(dag.tags) if dag.tags else 'None'}\n\n"

        # toto : get json data

        content += "[bold]📊 Recent Runs[/bold]\n"
        if runs:
            content += "Time      │ State      │ Duration │ Type     │ Run ID\n"
            content += "─" * 60 + "\n"
            for run in runs[:5]:
                state_icon = (
                    "✅"
                    if run.state == "success"
                    else "❌" if run.state == "failed" else "🟡"
                )
                execution_time = (
                    run.execution_date[:16] if run.execution_date else "Unknown"
                )
                duration = run.duration or "N/A"
                content += f"{execution_time} │ {state_icon} {run.state:<8} │ {duration:<8} │ {run.run_type:<8} │ {run.run_id}\n"
        else:
            content += "No recent runs found\n"

        content += "\n[bold]⚡ Actions[/bold]\n"
        content += "[T]rigger  [P]ause/Resume  [B]ackfill  [L]ogs\n"
        content += "\nPress the corresponding key to perform an action"

        return content

    async def _render_task_details(self, data: Dict, mock=False):
        """Task Rendering"""
        dag_id = data.get("dag_id", "unknown")
        task_id = data.get("task_id", "unknown")
        if mock:
            instances = [
                {
                    "execution_date": "2023-10-27T10:00:00+00:00",
                    "state": "success",
                    "duration": "0:00:05",
                }
            ]
            logs = "This is a mock log entry."
            task_content = self._format_task_content(dag_id, task_id, instances, logs)
            content_widget = self.query_one("#content-display", Static)
            content_widget.renderable = task_content
            content_widget.refresh()
        else:
            await self._load_task_details(dag_id, task_id)

    async def _load_task_details(self, dag_id: str, task_id: str):
        try:
            content_widget = self.query_one("#content-display", Static)
            content_widget.renderable = (
                f"📋 Loading task details for {dag_id}.{task_id}..."
            )
            content_widget.refresh()

            # Task 인스턴스 가져오기
            task_instances = await self.app_instance.airflow_client.get_task_instances(
                dag_id, task_id
            )

            # 최신 로그 가져오기
            logs = ""
            if task_instances:
                latest_execution = task_instances[0]["execution_date"]
                logs = await self.app_instance.airflow_client.get_task_logs(
                    dag_id, task_id, latest_execution
                )

            task_content = self._format_task_content(
                dag_id, task_id, task_instances, logs
            )
            content_widget.renderable = task_content
            content_widget.refresh()

        except Exception as e:
            content_widget = self.query_one("#content-display", Static)
            content_widget.renderable = (
                f"❌ Error loading task {dag_id}.{task_id}: {str(e)}"
            )
            content_widget.refresh()

    def _format_task_content(
        self, dag_id: str, task_id: str, instances: list, logs: str
    ) -> str:
        content = f"[bold blue]📋 Task Details: {dag_id}.{task_id}[/bold blue]\n\n"

        content += "[bold]📊 Recent Task Instances[/bold]\n"
        if instances:
            content += "Execution Date  │ State      │ Duration\n"
            content += "─" * 40 + "\n"
            for instance in instances[:5]:
                state_icon = (
                    "✅"
                    if instance["state"] == "success"
                    else "❌" if instance["state"] == "failed" else "🟡"
                )
                exec_date = (
                    instance["execution_date"][:16]
                    if instance["execution_date"]
                    else "Unknown"
                )
                duration = instance.get("duration", "N/A")
                content += (
                    f"{exec_date} │ {state_icon} {instance['state']:<8} │ {duration}\n"
                )
        else:
            content += "No task instances found\n"

        # get latest task logs
        content += "\n[bold]📜 Latest Logs[/bold]\n"
        if logs:

            log_lines = logs.split("\n")
            if len(log_lines) > 15:  # limit log lines
                content += "\n".join(log_lines[-15:])
                content += f"\n\n[dim]... showing last 15 lines of {len(log_lines)} total lines[/dim]"
            else:
                content += logs
        else:
            content += "No logs available\n"

        content += "\n\n[bold]⚡ Actions[/bold]\n"
        content += "Test Task  Full Logs  Retry\n"

        return content

    async def _render_connections(self):
        await self._load_connections()

    async def _load_connections(self):
        try:
            content_widget = self.query_one("#content-display", Static)
            content_widget.renderable = "🔗 Loading connections..."
            content_widget.refresh()

            connections = await self.app_instance.airflow_client.get_connections()

            content = "[bold blue]🔗 Connection Management[/bold blue]\n\n"
            content += "[bold]📋 Connections[/bold]\n"

            if connections:
                content += "Connection ID    │ Type     │ Host           │ Port\n"
                content += "─" * 50 + "\n"
                for conn in connections:
                    host = conn.host or "N/A"
                    port = str(conn.port) if conn.port else "N/A"
                    content += f"{conn.conn_id:<16} │ {conn.conn_type:<8} │ {host:<14} │ {port}\n"
            else:
                content += "No connections found\n"

            content += "\n[bold]⚡ Actions[/bold]\n"
            content += "Add Connection  Test  Delete\n"

            content_widget.renderable = content
            content_widget.refresh()

        except Exception as e:
            content_widget = self.query_one("#content-display", Static)
            content_widget.renderable = f"❌ Error loading connections: {str(e)}"
            content_widget.refresh()

    def _render_all_logs(self):
        content_widget = self.query_one("#content-display", Static)
        content_widget.renderable = """[bold blue]📜 All Logs[/bold blue]\n\nThis feature will show aggregated logs from all DAGs and tasks.\n\n[dim]Coming soon...[/dim]"""
        content_widget.refresh()

    async def _render_admin(self):
        await self._load_admin_info()

    async def _load_admin_info(self):
        try:
            content_widget = self.query_one("#content-display", Static)
            content_widget.renderable = "⚙️ Loading admin info..."
            content_widget.refresh()

            health_check = await self.app_instance.airflow_client.check_health()
            stats = await self.app_instance.airflow_client.get_system_stats()

            content = "[bold blue]⚙️ Administration[/bold blue]\n\n"
            content += "[bold]🏥 Health Check[/bold]\n"
            content += f"• Database: {'🟢 Connected' if health_check else '🔴 Error'}\n"
            content += (
                f"• Airflow Version: {stats.get('airflow_version', 'Unknown')}\n\n"
            )

            content += "[bold]📊 System Stats[/bold]\n"
            content += f"• Total DAGs: {stats.get('total_dags', 'N/A')}\n"
            content += f"• Active DAGs: {stats.get('active_dags', 'N/A')}\n"
            content += f"• Paused DAGs: {stats.get('paused_dags', 'N/A')}\n"

            content_widget.renderable = content
            content_widget.refresh()

        except Exception as e:
            content_widget = self.query_one("#content-display", Static)
            content_widget.renderable = f"❌ Error loading admin info: {str(e)}"
            content_widget.refresh()

    def _render_placeholder(self, content_type: str):
        content_widget = self.query_one("#content-display", Static)
        content_widget.renderable = (
            f"[bold]View: {content_type}[/bold]\n\nThis view is under development."
        )
        content_widget.refresh()

    async def refresh_current_view(self):
        await self.update_content(self.current_view, self.current_data)
