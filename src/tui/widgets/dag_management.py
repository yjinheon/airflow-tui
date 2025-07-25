"""
DAG Management Widget
Advanced DAG interface with Select dropdown, TabbedContent, and detailed views
"""

from textual.widgets import (
    Select,
    TabbedContent,
    TabPane,
    DataTable,
    Static,
    Button,
    Input,
    RichLog,
)
from textual.worker import Worker
from textual.containers import Vertical, Horizontal, Container
from textual.widget import Widget
from textual.reactive import reactive
from textual import work
from typing import Dict, List, Optional, TYPE_CHECKING
from rich.text import Text
import json

# 절대경로 imports
from src.tui.clients.base import AirflowClient
from src.tui.models.dag import DAGInfo, DAGRun

if TYPE_CHECKING:
    from src.tui.app import AirflowTUI


class DAGManagement(Widget):
    """DAG 관리 위젯"""

    DEFAULT_CSS = """
    DAGManagement {
        height: 100%;
        width: 100%;
    }
    
    #dag-header {
        height: 3;
        background: $primary;
        color: $text;
        padding: 1;
        text-align: center;
    }
    
    #dag-selector {
        height: 3;
        padding: 0 1;
    }
    
    #dag-list-container {
        height: 1fr;
        border: solid $accent;
        margin: 1;
    }
    
    #dag-details-container {
        height: 1fr;
        border: solid $accent;
        margin: 1;
    }
    
    #action-buttons {
        height: 3;
        padding: 0 1;
    }
    
    #filters {
        height: 3;
        padding: 0 1;
    }
    
    .dag-table {
        height: 100%;
        width: 100%;
        scrollbar-gutter: stable;
    }
    
    .dag-table:focus {
        border: thick $accent;
    }
    
    .status-active {
        color: $success;
    }
    
    .status-paused {
        color: $warning;
    }
    
    .status-failed {
        color: $error;
    }
    
    .status-running {
        color: $accent;
    }
    
    .action-button {
        margin: 0 1;
    }
    
    .filter-input {
        width: 1fr;
        margin: 0 1;
    }
    """

    selected_dag = reactive(None)
    filter_status = reactive("all")
    search_query = reactive("")

    def __init__(self, app_instance: "AirflowTUI"):
        super().__init__()
        self.app_instance = app_instance
        self.dag_list: List[DAGInfo] = []
        self.selected_dag_info: Optional[DAGInfo] = None
        self.dag_runs: List[DAGRun] = []

    def compose(self):
        """위젯 구성"""
        yield Static("🔄 DAG Management", id="dag-header")

        # DAG List Container
        with Container(id="dag-list-container"):
            yield Static("DAG List", classes="panel-title")
            dag_table = DataTable(id="dag-table", classes="dag-table")
            dag_table.add_columns(
                "●", "DAG_ID", "Status", "Schedule", "Last Run", "Next Run"
            )
            dag_table.cursor_type = "row"
            dag_table.can_focus = True
            yield dag_table

        # Show Selected Dag Info
        yield Static(
            "Selected DAG: (Use ↑↓ to navigate, SPACE/ENTER/S to select)",
            id="selected-dag-info",
        )

        # DAG Details
        with Container(id="dag-details-container"):
            with TabbedContent(id="dag-tabs", initial="overview-tab"):
                with TabPane("Overview", id="overview-tab"):
                    yield Static(
                        "Select a DAG from the list above to view its overview",
                        id="overview-content",
                    )

                with TabPane("Runs", id="runs-tab"):
                    runs_table = DataTable(id="runs-table")
                    runs_table.add_columns(
                        "Run After",
                        "State",
                        "Type",
                        "Start Date",
                        "End Date",
                        "Duration",
                    )
                    yield runs_table

                with TabPane("Tasks", id="tasks-tab"):
                    tasks_table = DataTable(id="tasks-table")
                    tasks_table.add_columns(
                        "Task ID",
                        "Operator",
                        "Pool",
                        "Retries",
                        "Timeout",
                        "Depends On",
                    )
                    yield tasks_table

                with TabPane("Graph", id="graph-tab"):
                    yield Static(
                        "DAG Graph View\n\n(Graph visualization coming soon...)",
                        id="graph-content",
                    )

                with TabPane("Code", id="code-tab"):
                    yield RichLog(id="code-content", highlight=True)

                with TabPane("Details", id="details-tab"):
                    yield Static("DAG Details", id="details-content")

                with TabPane("Plan", id="plan-tab"):
                    yield RichLog(id="plan-content", highlight=True)

        # Action Buttons
        with Horizontal(id="action-buttons"):
            yield Button("Trigger Now", id="trigger-btn", classes="action-button")
            yield Button("Pause/Resume", id="pause-btn", classes="action-button")
            yield Button("Backfill", id="backfill-btn", classes="action-button")
            yield Button("Clear", id="clear-btn", classes="action-button")
            yield Button("Delete", id="delete-btn", classes="action-button")
            yield Button("Refresh", id="refresh-btn", classes="action-button")

        # 필터 및 검색
        with Horizontal(id="filters"):
            yield Select(
                options=[
                    ("All", "all"),
                    ("Active", "active"),
                    ("Paused", "paused"),
                    ("Failed", "failed"),
                    ("Running", "running"),
                ],
                value="all",
                id="filter-select",
            )
            yield Input(
                placeholder="Search DAGs...", id="search-input", classes="filter-input"
            )

    def on_mount(self):
        """위젯 마운트 시 초기화"""
        self.load_dag_list()

        # DAG 테이블에 포커스 설정
        self.call_after_refresh(self.focus_dag_table)

    def focus_dag_table(self):
        """DAG 테이블에 포커스 설정"""
        try:
            dag_table = self.query_one("#dag-table", DataTable)
            dag_table.focus()
        except Exception:
            pass

    @work
    async def load_dag_list(self):
        """DAG 목록 로드"""
        try:
            if not self.app_instance.airflow_client.is_initialized:
                await self.app_instance.airflow_client.initialize()

            self.dag_list = await self.app_instance.airflow_client.get_dags()
            self.app_instance.notify(f"Loaded {len(self.dag_list)} DAGs")

            # 실제 DAG가 없을 때 예시 DAG 생성
            if not self.dag_list:
                self.dag_list = self.create_example_dags()
                self.app_instance.notify("Using example DAGs for demonstration")

            self.update_dag_table()

        except Exception as e:
            self.app_instance.notify(f"❌ Failed to load DAGs: {str(e)}")
            # 에러 발생 시에도 예시 DAG 생성
            self.dag_list = self.create_example_dags()
            self.app_instance.notify("Using example DAGs due to connection error")
            self.update_dag_table()

    def create_example_dags(self):
        """예시 DAG 생성"""
        from src.tui.models.dag import DAGInfo, TaskInfo

        example_dags = [
            DAGInfo(
                dag_id="daily_etl",
                is_paused=False,
                is_active=True,
                owner="data-team",
                description="Daily ETL pipeline for data processing",
                schedule_interval="0 6 * * *",
                tags=["etl", "daily", "production"],
                concurrency=16,
                last_updated="2025-07-17T14:30:00",
                fileloc="/opt/airflow/dags/daily_etl.py",
                tasks=[
                    TaskInfo(
                        task_id="extract",
                        dag_id="daily_etl",
                        operator="PythonOperator",
                        state="success",
                        start_date="2025-07-18T06:00:00",
                        end_date="2025-07-18T06:02:00",
                    ),
                    TaskInfo(
                        task_id="transform",
                        dag_id="daily_etl",
                        operator="PythonOperator",
                        state="success",
                        start_date="2025-07-18T06:02:00",
                        end_date="2025-07-18T06:04:00",
                    ),
                    TaskInfo(
                        task_id="load",
                        dag_id="daily_etl",
                        operator="PostgresOperator",
                        state="success",
                        start_date="2025-07-18T06:04:00",
                        end_date="2025-07-18T06:06:00",
                    ),
                ],
            )
        ]

        return example_dags

    def update_dag_table(self):
        """DAG 테이블 업데이트"""
        dag_table = self.query_one("#dag-table", DataTable)
        dag_table.clear()

        filtered_dags = [dag for dag in self.dag_list if self.should_show_dag(dag)]
        self.app_instance.notify(f"Showing {len(filtered_dags)} DAGs in table")

        if not filtered_dags:
            # 표시할 DAG가 없을 때 안내 메시지 추가
            dag_table.add_row(
                "ℹ️",
                "No DAGs found",
                "Check your Airflow installation",
                "or filters",
                "-",
                "-",
                key="no_dags",
            )
        else:
            for dag in filtered_dags:
                # 상태 아이콘
                if dag.is_paused:
                    status_icon = "⏸️"
                    status_text = "⏸️ Paused"
                    status_class = "status-paused"
                elif not dag.is_active:
                    status_icon = "❌"
                    status_text = "🔴 Inactive"
                    status_class = "status-failed"
                else:
                    status_icon = "✅"
                    status_text = "🟢 Active"
                    status_class = "status-active"

                # 스케줄 정보
                # schedule = dag.schedule_interval.value or "Manual"
                schedule = self.parse_schedule(dag.schedule_interval)

                last_run = "2025-07-18 06:00"
                next_run = "2025-07-19 06:00" if not dag.is_paused else "-"

                dag_table.add_row(
                    status_icon,
                    dag.dag_id,
                    status_text,
                    schedule,
                    last_run,
                    next_run,
                    key=dag.dag_id,
                )

    def parse_schedule(self, schedule_interval):
        # Handle None or empty values
        if not schedule_interval:
            return "Manual"

        # Handle string values - convert JSON string to dict if needed
        if isinstance(schedule_interval, str):
            if schedule_interval.lower() in ["none", "null", ""]:
                return "Manual"
            # Try to parse JSON string to dict
            try:
                import json

                schedule_interval = json.loads(schedule_interval)
            except (json.JSONDecodeError, ValueError):
                # If not JSON, treat as regular string (cron expression)
                return schedule_interval

        # Handle dictionary values (from JSON response)
        if isinstance(schedule_interval, dict):
            if schedule_interval.get("__type") == "CronExpression":
                return schedule_interval.get("value", "Manual")
            elif schedule_interval.get("__type") == "TimeDelta":
                unit_names = {
                    "weeks": "week",
                    "days": "day",
                    "hours": "hour",
                    "minutes": "minute",
                    "seconds": "second",
                }
                parts = [
                    f"{schedule_interval[unit]} {name}{'s' if int(schedule_interval[unit]) > 1 else ''}"
                    for unit, name in unit_names.items()
                    if unit in schedule_interval and int(schedule_interval[unit]) > 0
                ]
                return "every " + " ".join(parts) if parts else "immediately"
            else:
                return str(schedule_interval.get("value", "Manual"))

        # Fallback for any other type
        return str(schedule_interval) if schedule_interval else "Manual"

    def should_show_dag(self, dag: DAGInfo) -> bool:
        """DAG 필터링 조건 확인"""
        # 상태 필터
        if self.filter_status == "active" and (dag.is_paused or not dag.is_active):
            return False
        elif self.filter_status == "paused" and not dag.is_paused:
            return False
        elif self.filter_status == "failed" and dag.is_active:
            return False

        if self.search_query and self.search_query.lower() not in dag.dag_id.lower():
            return False

        return True

    def on_data_table_row_selected(self, event):
        if event.control.id == "dag-table":
            # RowKey가 아닌 실제 DAG ID 컬럼 값을 가져오기
            dag_table = event.control
            try:
                # 선택된 행의 DAG_ID 컬럼(인덱스 1) 값 추출
                row_data = dag_table.get_row_at(event.cursor_row)
                dag_id = str(row_data[1])  # "DAG_ID" 컬럼
                self.select_dag_row(dag_id)
            except (IndexError, AttributeError):
                # 실패시 fallback으로 row_key 사용
                self.select_dag_row(str(event.row_key))

    def on_data_table_row_highlighted(self, event):
        if event.control.id == "dag-table":
            try:
                # 실제 DAG ID 컬럼(인덱스 1) 값 추출
                dag_table = event.control
                row_data = dag_table.get_row_at(event.cursor_row)
                dag_id = str(row_data[1])  # "DAG_ID" 컬럼
            except (IndexError, AttributeError):
                # 실패시 fallback으로 row_key 사용
                dag_id = str(event.row_key)

            if dag_id and dag_id != "no_dags":
                dag_info = next(
                    (dag for dag in self.dag_list if dag.dag_id == dag_id), None
                )
                if dag_info:
                    # 상태 표시줄에 미리보기 정보 표시
                    self.app_instance.sub_title = (
                        f"Highlighted: {dag_id} ({dag_info.owner})"
                    )

    def select_dag_row(self, dag_id):
        if dag_id == "no_dags":
            return

        self.selected_dag = dag_id
        self.selected_dag_info = next(
            (dag for dag in self.dag_list if dag.dag_id == dag_id), None
        )

        selected_info = self.query_one("#selected-dag-info", Static)
        if self.selected_dag_info:
            status = (
                "Paused"
                if self.selected_dag_info.is_paused
                else "Active" if self.selected_dag_info.is_active else "Inactive"
            )
            selected_info.update(
                f"Selected DAG: {dag_id} ({status}) - {self.selected_dag_info.description or 'No description'}"
            )
        else:
            selected_info.update(f"Selected DAG: {dag_id} (Loading...)")

        # 상세 정보 로드 (워커를 통해 비동기 실행)
        self.load_dag_details(dag_id)

        # 앱의 current_selection 업데이트
        self.app_instance.handle_dag_selection_from_management(
            dag_id, self.selected_dag_info
        )

        # notify
        self.app_instance.notify(f"✅ Selected DAG: {dag_id}")

        # update subtitle of app
        self.app_instance.sub_title = f"Selected DAG: {dag_id}"

    def on_key(self, event):
        if self.query_one("#dag-table", DataTable).has_focus:
            if event.key == "space" or event.key == "enter":
                dag_table = self.query_one("#dag-table", DataTable)
                if dag_table.cursor_row is not None:
                    try:
                        row_data = dag_table.get_row_at(dag_table.cursor_row)
                        dag_id = str(row_data[1])  # "DAG_ID" 컬럼
                        self.select_dag_row(dag_id)
                        event.prevent_default()
                    except Exception:
                        pass
            elif event.key == "s":
                # 's' 키로도 선택 가능
                dag_table = self.query_one("#dag-table", DataTable)
                if dag_table.cursor_row is not None:
                    try:
                        row_data = dag_table.get_row_at(dag_table.cursor_row)
                        dag_id = str(row_data[1])  # "DAG_ID" 컬럼
                        self.select_dag_row(dag_id)
                        event.prevent_default()
                    except Exception:
                        pass

    @work
    async def load_dag_details(self, dag_id: str):

        try:
            # Overview 탭 업데이트
            await self.update_overview_tab(dag_id)

            # Runs 탭 업데이트
            await self.update_runs_tab(dag_id)

            # Tasks 탭 업데이트
            await self.update_tasks_tab(dag_id)

            # Code 탭 업데이트
            await self.update_code_tab(dag_id)

            # Details 탭 업데이트
            await self.update_details_tab(dag_id)

            # Plan 탭 업데이트
            await self.update_plan_tab(dag_id)

            # Refresh the entire tab content area
            try:
                tab_content = self.query_one("#dag-tabs")
                tab_content.refresh()
            except Exception:
                pass  # Silent fail on refresh

        except Exception as e:
            self.app_instance.notify(f"❌ Failed to load DAG details: {str(e)}")

    async def update_overview_tab(self, dag_id: str):
        """Overview 탭 내용 업데이트"""
        # Try to get DAG info from the list if not already set
        if not self.selected_dag_info:
            self.selected_dag_info = next(
                (dag for dag in self.dag_list if dag.dag_id == dag_id), None
            )

        if not self.selected_dag_info:
            try:
                overview_content = self.query_one("#overview-content", Static)
                loading_content = f"[bold yellow]Loading DAG information for {dag_id}...[/bold yellow]\n\nPlease wait while we fetch the DAG details.\n\n[dim]If this persists, the DAG may not be available.[/dim]"
                overview_content.update(loading_content)
                overview_content.refresh()
            except Exception:
                pass
            return

        dag = self.selected_dag_info

        if dag.is_paused:
            status_icon = "⏸️"
            status_text = "Paused"
        elif not dag.is_active:
            status_icon = "🔴"
            status_text = "Inactive"
        else:
            status_icon = "🟢"
            status_text = "Active"

        content = f"""[bold]{status_icon} DAG Overview: {dag_id}[/bold]

[bold]Basic Information:[/bold]
• [bold]Status:[/bold] {status_icon} {status_text}
• [bold]Description:[/bold] {dag.description or 'No description provided'}
• [bold]Owner:[/bold] {dag.owner or 'Unknown'}
• [bold]Tags:[/bold] {', '.join([str(tag) for tag in dag.tags]) if dag.tags else 'None'}

[bold]Schedule & Configuration:[/bold]
• [bold]Schedule:[/bold] {dag.schedule_interval or 'Manual/None'}
• [bold]Max Active Runs:[/bold] {dag.max_active_runs or 'Unlimited'}
• [bold]Concurrency:[/bold] {dag.concurrency or 'Default'}
• [bold]Catchup:[/bold] {dag.catchup if hasattr(dag, 'catchup') else 'Unknown'}

[bold]File Information:[/bold]
• [bold]File Location:[/bold] {dag.fileloc or 'Unknown'}
• [bold]Last Updated:[/bold] {dag.last_updated or 'Unknown'}

[bold]Tasks:[/bold]
• [bold]Total Tasks:[/bold] {len(dag.tasks) if dag.tasks else 0}
• [bold]Task IDs:[/bold] {', '.join([str(t.task_id) for t in dag.tasks[:5]]) if dag.tasks else 'None'}{' ...' if dag.tasks and len(dag.tasks) > 5 else ''}

[bold]Recent Statistics:[/bold]
• Total Runs: Loading...    • Success Rate: Loading...    
• Avg Duration: Loading...  • Last Success: Loading...
• Last Failure: Loading...

[dim]Use the buttons below or keyboard shortcuts to manage this DAG[/dim]"""

        try:
            overview_content = self.query_one("#overview-content", Static)
            overview_content.update(content)
            overview_content.refresh()
        except Exception:
            # Fallback: try alternative update methods
            try:
                overview_content = self.query_one("#overview-content", Static)
                overview_content.renderable = content
                overview_content.refresh()
            except Exception:
                pass  # Silent fail

    async def update_runs_tab(self, dag_id: str):
        try:
            runs = await self.app_instance.airflow_client.get_dag_runs(dag_id, limit=20)

            runs_table = self.query_one("#runs-table", DataTable)
            runs_table.clear()

            for run in runs:
                state_icon = self.get_run_state_icon(run.state)
                runs_table.add_row(
                    run.execution_date[:19] if run.execution_date else "Unknown",
                    f"{state_icon} {run.state}",
                    run.run_type,
                    run.execution_date[:19] if run.execution_date else "Unknown",
                    (
                        getattr(run, "end_date", "Unknown")[:19]
                        if getattr(run, "end_date", None)
                        else "Unknown"
                    ),
                    run.duration or "N/A",
                )

        except Exception as e:
            runs_table = self.query_one("#runs-table", DataTable)
            runs_table.clear()
            runs_table.add_row(
                "Error", f"Failed to load runs: {str(e)}", "", "", "", ""
            )

    async def update_tasks_tab(self, dag_id: str):
        """Tasks tab content update with DAG structure"""
        try:
            # Get DAG structure using airflow dags show command
            dag_structure = await self.app_instance.airflow_client.get_dag_structure(
                dag_id
            )

            # Replace the tasks table with a RichLog to show the structure
            tasks_container = self.query_one("#tasks-tab")
            # Remove existing table
            try:
                old_table = self.query_one("#tasks-table", DataTable)
                old_table.remove()
            except:
                pass

            # Add RichLog to show DAG structure
            from textual.widgets import RichLog

            structure_log = RichLog(id="dag-structure-log", highlight=True)
            structure_log.write(dag_structure)
            tasks_container.mount(structure_log)

        except Exception as e:
            # Fallback to original implementation if airflow dags show fails
            if not self.selected_dag_info or not self.selected_dag_info.tasks:
                return

            tasks_table = self.query_one("#tasks-table", DataTable)
            tasks_table.clear()

            for task in self.selected_dag_info.tasks:
                tasks_table.add_row(
                    task.task_id,
                    task.operator or "Unknown",
                    task.pool or "default",
                    str(task.retries) if task.retries else "0",
                    task.timeout or "-",
                    (
                        ", ".join([str(dep) for dep in task.depends_on])
                        if task.depends_on
                        else "-"
                    ),
                )

    async def update_code_tab(self, dag_id: str):
        try:
            code_content = self.query_one("#code-content", RichLog)
            code_content.clear()

            # Get the DAG file location using CLI client
            try:
                dag_file_path = await self.app_instance.airflow_client.get_dag_loc(dag_id)
                if dag_file_path:
                    # Read the actual DAG file content
                    import aiofiles
                    try:
                        async with aiofiles.open(dag_file_path, 'r', encoding='utf-8') as f:
                            actual_code = await f.read()
                        code_content.write(actual_code)
                        return
                    except FileNotFoundError:
                        code_content.write(f"# DAG file not found at: {dag_file_path}\n# File may have been moved or deleted.\n\n")
                    except PermissionError:
                        code_content.write(f"# Permission denied accessing: {dag_file_path}\n# Check file permissions.\n\n")
                    except Exception as file_error:
                        code_content.write(f"# Error reading DAG file: {str(file_error)}\n# Path: {dag_file_path}\n\n")
                        
            except Exception as location_error:
                code_content.write(f"# Error getting DAG location: {str(location_error)}\n\n")

            # Fallback to sample code if we can't read the actual file
            sample_code = f"""# DAG: {dag_id}
# Note: This is sample code as the actual DAG file could not be loaded
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.http import HttpSensor
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email import EmailOperator

default_args = {{
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}}

dag = DAG(
    '{dag_id}',
    default_args=default_args,
    description='Sync user data from external API',
    schedule_interval=timedelta(minutes=15),
    catchup=True,
    max_active_runs=1,
    tags=['sync', 'users', 'hourly'],
)

# Tasks
start = DummyOperator(task_id='start', dag=dag)

extract_users = HttpSensor(
    task_id='extract_users',
    http_conn_id='api_conn',
    endpoint='users',
    dag=dag,
    pool='api',
    retries=3,
    timeout=timedelta(minutes=5),
)

# Task dependencies
start >> extract_users
"""

            code_content.write(sample_code)

        except Exception as e:
            code_content = self.query_one("#code-content", RichLog)
            code_content.clear()
            code_content.write(f"Error loading code: {str(e)}")

    async def update_details_tab(self, dag_id: str):
        """Details 탭 내용 업데이트"""
        if not self.selected_dag_info:
            return

        dag = self.selected_dag_info

        content = f"""[bold]DAG Details: {dag_id}[/bold]

[bold]Basic Information:[/bold]
• DAG ID: {dag.dag_id}
• File Path: {dag.fileloc or 'Unknown'}
• Owner: {dag.owner or 'Unknown'}
• Description: {dag.description or 'No description'}
• Tags: {', '.join([str(tag) for tag in dag.tags]) if dag.tags else 'None'}

[bold]Schedule & Execution:[/bold]
• Schedule Interval: {dag.schedule_interval or 'None'}
• Is Paused: {dag.is_paused}
• Is Active: {dag.is_active}
• Max Active Runs: {dag.max_active_runs or 'Unlimited'}
• Concurrency: {dag.concurrency or 'Default'}

[bold]Dates:[/bold]
• Start Date: {dag.start_date or 'Unknown'}
• End Date: {dag.end_date or 'None'}
• Last Updated: {dag.last_updated or 'Unknown'}

[bold]Tasks:[/bold]
• Total Tasks: {len(dag.tasks) if dag.tasks else 0}
• Task IDs: {', '.join([str(t.task_id) for t in dag.tasks]) if dag.tasks else 'None'}"""

        details_content = self.query_one("#details-content", Static)
        details_content.update(content)

    async def update_plan_tab(self, dag_id: str):
        """Plan tab content update with DAG runs plan"""
        try:
            # Get DAG runs plan using airflow dags list-runs -d <DAG_ID> --plan command
            plan_output = await self.app_instance.airflow_client.get_dag_list_runs(
                dag_id
            )

            plan_content = self.query_one("#plan-content", RichLog)
            plan_content.clear()
            plan_content.write(plan_output)

        except Exception as e:
            plan_content = self.query_one("#plan-content", RichLog)
            plan_content.clear()
            plan_content.write(f"Error loading plan: {str(e)}")

    def get_run_state_icon(self, state: str) -> str:
        """실행 상태 아이콘 반환"""
        state_icons = {
            "success": "✅",
            "failed": "❌",
            "running": "🟡",
            "queued": "🟠",
            "up_for_retry": "🔄",
            "upstream_failed": "⚠️",
            "skipped": "⏭️",
        }
        return state_icons.get(state.lower(), "🔹")

    def on_button_pressed(self, event):
        """버튼 클릭 처리"""
        if not self.selected_dag:
            self.app_instance.notify("❌ Please select a DAG first")
            return

        if event.button.id == "trigger-btn":
            self.trigger_dag()
        elif event.button.id == "pause-btn":
            self.toggle_pause_dag()
        elif event.button.id == "refresh-btn":
            self.load_dag_list()
        elif event.button.id == "backfill-btn":
            self.app_instance.notify("🔄 Backfill feature coming soon...")
        elif event.button.id == "clear-btn":
            self.app_instance.notify("🧹 Clear feature coming soon...")
        elif event.button.id == "delete-btn":
            self.app_instance.notify("🗑️ Delete feature coming soon...")

    @work
    async def trigger_dag(self):
        """DAG 트리거"""
        try:
            success = await self.app_instance.airflow_client.trigger_dag(
                self.selected_dag
            )
            if success:
                self.app_instance.notify(
                    f"✅ DAG {self.selected_dag} triggered successfully!"
                )
                await self.load_dag_details(self.selected_dag)
            else:
                self.app_instance.notify(
                    f"❌ Failed to trigger DAG {self.selected_dag}"
                )
        except Exception as e:
            self.app_instance.notify(f"❌ Error triggering DAG: {str(e)}")

    @work
    async def toggle_pause_dag(self):
        """DAG 일시정지/재개"""
        try:
            is_paused = await self.app_instance.airflow_client.is_dag_paused(
                self.selected_dag
            )

            if is_paused:
                success = await self.app_instance.airflow_client.unpause_dag(
                    self.selected_dag
                )
                action = "resumed"
            else:
                success = await self.app_instance.airflow_client.pause_dag(
                    self.selected_dag
                )
                action = "paused"

            if success:
                self.app_instance.notify(
                    f"✅ DAG {self.selected_dag} {action} successfully!"
                )

                # Update the selected DAG info to reflect the new status
                if self.selected_dag_info:
                    self.selected_dag_info.is_paused = not is_paused
                    self.selected_dag_info.is_active = (
                        is_paused  # If it was paused, now it's active
                    )

                # Refresh the table and details
                await self.load_dag_list()
                # Update the selection display
                if self.selected_dag_info:
                    status = (
                        "Paused"
                        if self.selected_dag_info.is_paused
                        else (
                            "Active" if self.selected_dag_info.is_active else "Inactive"
                        )
                    )
                    selected_info = self.query_one("#selected-dag-info", Static)
                    selected_info.update(
                        f"Selected DAG: {self.selected_dag} ({status}) - {self.selected_dag_info.description or 'No description'}"
                    )

                # Reload the details with updated status
                await self.load_dag_details(self.selected_dag)
            else:
                self.app_instance.notify(
                    f"❌ Failed to {action.split('d')[0]} DAG {self.selected_dag}"
                )

        except Exception as e:
            self.app_instance.notify(f"❌ Error toggling DAG pause: {str(e)}")

    def on_select_changed(self, event):
        if event.control.id == "filter-select":
            self.filter_status = event.value
            self.update_dag_table()

    def on_input_changed(self, event):
        if event.control.id == "search-input":
            self.search_query = event.value
            self.update_dag_table()

    async def refresh_data(self):
        """refresh widget data"""
        await self.load_dag_list()
        if self.selected_dag:
            await self.load_dag_details(self.selected_dag)
