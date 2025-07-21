"""
Navigation Tree Widget
좌측 패널의 트리 네비게이션
"""

from textual.widgets import Tree
from textual import work
from typing import TYPE_CHECKING

# 절대경로 imports
from src.tui.clients.base import AirflowClient
from src.tui.models.dag import DAGInfo

if TYPE_CHECKING:
    from src.tui.app import AirflowTUI


class NavigationTree(Tree):

    def __init__(self, app_instance: "AirflowTUI"):
        super().__init__("Navigation")
        self.app_instance = app_instance
        self.root.expand()
        self.dags_node = None

    def on_mount(self):
        """init when the widget is mounted"""
        self._build_tree_structure()
        self.load_dags()

    def _build_tree_structure(self):
        # Dashboard
        dashboard_node = self.root.add("📊 Dashboard", data={"type": "dashboard"})

        # DAGs
        self.dags_node = self.root.add("🔄 DAGs", data={"type": "dags_root"})

        # Connections
        connections_node = self.root.add("🔗 Connections", data={"type": "connections"})

        # All Logs
        logs_node = self.root.add("📜 All Logs", data={"type": "all_logs"})

        # Admin
        admin_node = self.root.add("⚙️ Admin", data={"type": "admin"})

    @work
    async def load_dags(self):
        try:
            if not self.app_instance.airflow_client.is_initialized:
                await self.app_instance.airflow_client.initialize()

            dags = await self.app_instance.airflow_client.get_dags()

            # breakpoint()

            # 기존 DAG 노드들 제거
            if self.dags_node:
                self.dags_node.remove_children()

            # DAG 추가
            for dag in dags:
                status_icon = self._get_dag_status_icon(dag)

                dag_node = self.dags_node.add(
                    f"{status_icon} {dag.dag_id}",
                    data={"type": "dag", "dag_id": dag.dag_id},
                )

                # Task 추가
                if dag.tasks:
                    for task in dag.tasks:
                        task_icon = self._get_task_status_icon(task.state)

                        dag_node.add_leaf(
                            f"{task_icon} {task.task_id}",
                            data={
                                "type": "task",
                                "dag_id": dag.dag_id,
                                "task_id": task.task_id,
                            },
                        )

            # DAGs 노드 확장
            self.dags_node.expand()

        except Exception as e:
            # 에러 시 알림
            self.app_instance.notify(f"❌ Failed to load DAGs: {str(e)}")

    def _get_dag_status_icon(self, dag: DAGInfo) -> str:
        if dag.is_paused:
            return "⏸️"
        elif not dag.is_active:
            return "🔴"
        else:
            return "🟢"

    def _get_task_status_icon(self, state: str) -> str:
        state_icons = {
            "failed": "❌",
            "success": "✅",
            "running": "🟡",
            "queued": "🟠",
            "up_for_retry": "🔄",
            "upstream_failed": "⚠️",
            "skipped": "⏭️",
            "unknown": "🔹",
        }
        return state_icons.get(state.lower(), "🔹")

    async def on_tree_node_selected(self, event):
        node = event.node
        if node.data:
            await self.app_instance.handle_navigation_selection(node.data)

    @work
    async def refresh_dags(self):
        try:
            if not self.app_instance.airflow_client.is_initialized:
                await self.app_instance.airflow_client.initialize()

            dags = await self.app_instance.airflow_client.get_dags()

            # 기존 DAG 노드들 제거
            if self.dags_node:
                self.dags_node.remove_children()

            # DAG 추가
            for dag in dags:
                status_icon = self._get_dag_status_icon(dag)

                dag_node = self.dags_node.add(
                    f"{status_icon} {dag.dag_id}",
                    data={"type": "dag", "dag_id": dag.dag_id},
                )

                # Task 추가
                if dag.tasks:
                    for task in dag.tasks:
                        task_icon = self._get_task_status_icon(task.state)

                        dag_node.add_leaf(
                            f"{task_icon} {task.task_id}",
                            data={
                                "type": "task",
                                "dag_id": dag.dag_id,
                                "task_id": task.task_id,
                            },
                        )

            # DAGs 노드 확장
            self.dags_node.expand()

        except Exception as e:
            # 에러 시 알림
            self.app_instance.notify(f"❌ Failed to refresh DAGs: {str(e)}")

    async def refresh_dags(self):
        """DAG 목록 새로고침"""
        await self.load_dags()

    def expand_all_dags(self):
        """모든 DAG 노드 확장"""
        if self.dags_node:
            for child in self.dags_node.children:
                child.expand()

    def collapse_all_dags(self):
        """모든 DAG 노드 축소"""
        if self.dags_node:
            for child in self.dags_node.children:
                child.collapse()
