from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from src.tui.models.connection import Connection
from src.tui.models.dag import DAGInfo
from src.tui.models.dag import DAGRun
from src.tui.models.dag import TaskInfo


class AirflowClient(ABC):
    def __init__(self):
        self._initialized = False

    async def initialize(self):
        self._initialized = True

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    # DAG realdted commands
    @abstractmethod
    async def get_dags(self) -> List[DAGInfo]:
        """get all dags"""
        pass

    @abstractmethod
    async def get_dag_details(self, dag_id: str) -> DAGInfo:
        """get all details of a dag"""
        pass

    @abstractmethod
    async def get_dag_runs(self, dag_id: str, limit: int = 10) -> List[DAGRun]:
        """get dags run history"""
        pass

    @abstractmethod
    async def get_dag_runs_with_plan(self, dag_id: str, limit: int = 10) -> str:
        """DAG runs with plan (airflow dags list-runs -d <DAG_ID> --plan)"""
        pass

    @abstractmethod
    async def trigger_dag(self, dag_id: str, conf: Optional[Dict] = None) -> bool:
        """trigger a DAG run"""
        pass

    @abstractmethod
    async def pause_dag(self, dag_id: str) -> bool:
        pass

    @abstractmethod
    async def unpause_dag(self, dag_id: str) -> bool:
        pass

    @abstractmethod
    async def backfill_dag(self, dag_id: str, start_date: str, end_date: str) -> bool:
        pass

    # Task
    @abstractmethod
    async def get_dag_tasks(self, dag_id: str) -> List[TaskInfo]:
        pass

    @abstractmethod
    async def get_dag_structure(self, dag_id: str) -> str:
        """DAG structure with task dependencies (airflow dags show)"""
        pass

    @abstractmethod
    async def get_task_instances(
        self, dag_id: str, task_id: str, limit: int = 10
    ) -> List[Dict]:
        """get task instance history"""
        pass

    @abstractmethod
    async def get_task_logs(
        self, dag_id: str, task_id: str, execution_date: str
    ) -> str:
        pass

    @abstractmethod
    async def test_task(self, dag_id: str, task_id: str, execution_date: str) -> bool:
        pass

    @abstractmethod
    async def get_connections(self) -> List[Connection]:
        pass

    @abstractmethod
    async def add_connection(self, connection: Connection) -> bool:
        pass

    @abstractmethod
    async def delete_connection(self, conn_id: str) -> bool:
        pass

    @abstractmethod
    async def test_connection(self, conn_id: str) -> bool:
        pass

    @abstractmethod
    async def check_health(self) -> bool:
        pass

    @abstractmethod
    async def get_system_stats(self) -> Dict[str, Any]:
        pass

    async def is_dag_paused(self, dag_id: str) -> bool:
        try:
            dag = await self.get_dag_details(dag_id)
            return dag.is_paused
        except Exception:
            return False

    async def get_recent_failures(self, limit: int = 5) -> List[Dict]:
        """get recent failed DAG runs"""
        try:
            dags = await self.get_dags()
            failures = []

            for dag in dags:
                runs = await self.get_dag_runs(dag.dag_id, limit=10)
                for run in runs:
                    if run.state == "failed":
                        failures.append(
                            {
                                "dag_id": dag.dag_id,
                                "run_id": run.run_id,
                                "execution_date": run.execution_date,
                                "duration": run.duration,
                            }
                        )
                        if len(failures) >= limit:
                            break
                if len(failures) >= limit:
                    break

            return failures[:limit]
        except Exception:
            return []

    async def get_recent_successes(self, limit: int = 5) -> List[Dict]:
        """get recent successful DAG runs"""
        try:
            dags = await self.get_dags()
            successes = []

            for dag in dags:
                runs = await self.get_dag_runs(dag.dag_id, limit=10)
                for run in runs:
                    if run.state == "success":
                        successes.append(
                            {
                                "dag_id": dag.dag_id,
                                "run_id": run.run_id,
                                "execution_date": run.execution_date,
                                "duration": run.duration,
                            }
                        )
                        if len(successes) >= limit:
                            break
                if len(successes) >= limit:
                    break

            return successes[:limit]
        except Exception:
            return []

    async def get_running_dags(self) -> List[Dict]:
        try:
            dags = await self.get_dags()
            running = []

            for dag in dags:
                runs = await self.get_dag_runs(dag.dag_id, limit=5)
                for run in runs:
                    if run.state == "running":
                        running.append(
                            {
                                "dag_id": dag.dag_id,
                                "run_id": run.run_id,
                                "execution_date": run.execution_date,
                                "start_date": run.start_date,
                            }
                        )
                        break  # single run per dag

            return running
        except Exception:
            return []


class ClientFactory:
    """Client Factory to create AirflowClient instances"""

    _clients = {}

    @classmethod
    def register_client(cls, name: str, client_class):
        cls._clients[name] = client_class

    @classmethod
    def create_client(cls, client_type: str, **kwargs) -> AirflowClient:
        if client_type not in cls._clients:
            raise ValueError(f"Unknown client type: {client_type}")

        return cls._clients[client_type](**kwargs)

    @classmethod
    def get_available_clients(cls) -> List[str]:
        return list(cls._clients.keys())


class AirflowClientError(Exception):
    pass


class AirflowConnectionError(AirflowClientError):
    pass


class AirflowCommandError(AirflowClientError):
    pass


class AirflowDataError(AirflowClientError):
    pass
