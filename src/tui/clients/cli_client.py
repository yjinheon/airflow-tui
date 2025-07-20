"""
Airflow CLI Based Client
"""

import subprocess
import json
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
import re

from src.tui.clients.base import (
    AirflowClient,
    AirflowClientError,
    AirflowCommandError,
    AirflowDataError,
    TaskInfo,
)
from src.tui.models.dag import DAGInfo, DAGRun
from src.tui.models.connection import Connection


class AirflowCLIClient(AirflowClient):

    def __init__(self, airflow_home: Optional[str] = None):
        super().__init__()
        self.airflow_home = airflow_home
        self.base_env = {}
        if airflow_home:
            self.base_env["AIRFLOW_HOME"] = airflow_home

    async def initialize(self):
        try:
            await self._run_command(["airflow", "version"], parse_json=False)
            await super().initialize()
        except Exception as e:
            raise AirflowConnectionError(f"Cannot connect to Airflow CLI: {str(e)}")

    async def _run_command(self, cmd: List[str], parse_json: bool = True) -> Any:
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={**self.base_env} if self.base_env else None,
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_msg = stderr.decode().strip()
                raise AirflowCommandError(f"Command failed: {error_msg}")

            output = stdout.decode().strip()

            if parse_json and output:
                try:
                    return json.loads(output)
                except json.JSONDecodeError:
                    # JSON 파싱 실패시 원본 텍스트 반환
                    return output

            return output

        except Exception as e:
            if isinstance(e, AirflowCommandError):
                raise
            # Add error details for debugging
            print(f"CLI command failed: {cmd}, Error: {str(e)}")
            raise AirflowClientError(f"CLI command error: {str(e)}")

    # DAG commands
    async def get_dags(self) -> List[DAGInfo]:
        """airflow dags list --output json"""
        try:
            result = await self._run_command(
                ["airflow", "dags", "list", "--output", "json"]
            )

            dags = []
            for dag_data in result:
                dags.append(
                    DAGInfo(
                        dag_id=dag_data["dag_id"],
                        is_active=not dag_data.get("paused", True),
                        is_paused=dag_data.get("paused", False),
                        description=dag_data.get("description", ""),
                        owner=(
                            dag_data.get("owners", [""])[0]
                            if dag_data.get("owners")
                            else ""
                        ),
                        schedule_interval=dag_data.get("schedule_interval"),
                        start_date=dag_data.get("start_date"),
                        tags=dag_data.get("tags", []),
                        tasks=[],  # 빈 리스트로 초기화하여 성능 향상
                    )
                )

            return dags

        except Exception as e:
            # Fallback to simple list if JSON fails
            try:
                output = await self._run_command(
                    ["airflow", "dags", "list"], parse_json=False
                )
                return self._parse_dag_list_text(output)
            except Exception as fallback_error:
                print(
                    f"Error: Both JSON and text parsing failed: {str(fallback_error)}"
                )
                raise AirflowDataError(f"Failed to get DAGs: {str(e)}")

    async def get_dag_details(self, dag_id: str) -> DAGInfo:
        """airflow dags details {dag_id} --output json"""
        try:
            # try json first
            result = await self._run_command(
                ["airflow", "dags", "details", dag_id, "--output", "json"]
            )

            # pares json
            if isinstance(result, dict):
                dag_data = result
            elif isinstance(result, list) and len(result) > 0:
                dag_data = result[0]
            else:
                raise AirflowDataError("Invalid JSON response from dags details")

            # get tasks
            try:
                tasks = await self.get_dag_tasks(dag_id)
            except Exception as e:
                # Add error log and return empty list
                print(f"Warning: Failed to get tasks for DAG {dag_id}: {str(e)}")
                tasks = []

            dag_info = DAGInfo(
                dag_id=dag_data.get("dag_id", dag_id),
                is_active=not dag_data.get("is_paused", True),
                is_paused=dag_data.get("is_paused", False),
                description=dag_data.get("description", ""),
                owner=self._extract_owner(dag_data.get("owners", [])),
                schedule_interval=dag_data.get("schedule_interval"),
                start_date=dag_data.get("start_date"),
                end_date=dag_data.get("end_date"),
                last_run=dag_data.get("last_parsed_time"),
                tags=dag_data.get("tags", []),
                max_active_runs=dag_data.get("max_active_runs", 1),
                catchup=dag_data.get("catchup", True),
                fileloc=dag_data.get("fileloc"),
                tasks=tasks,
            )

            return dag_info

        except Exception as e:
            if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                raise ValueError(f"DAG {dag_id} not found")

            try:
                text_result = await self._run_command(
                    ["airflow", "dags", "details", dag_id], parse_json=False
                )

                return await self._parse_dag_details_text(text_result, dag_id)

            except Exception as fallback_error:
                try:
                    dags = await self.get_dags()
                    for dag in dags:
                        if dag.dag_id == dag_id:
                            return dag
                    raise ValueError(f"DAG {dag_id} not found")
                except:
                    raise AirflowDataError(
                        f"Failed to get DAG details for {dag_id}: {str(e)}"
                    )

    def _extract_owner(self, owners_data) -> str:
        """owners 데이터에서 첫 번째 owner 추출"""
        if isinstance(owners_data, list) and owners_data:
            return owners_data[0]
        elif isinstance(owners_data, str):
            return owners_data
        return ""

    async def _parse_dag_details_text(self, output: str, dag_id: str) -> DAGInfo:
        lines = output.split("\n")

        # init dag_info
        dag_info = {
            "dag_id": dag_id,
            "is_paused": False,
            "is_active": True,
            "description": "",
            "owner": "",
            "schedule_interval": None,
            "start_date": None,
            "tags": [],
            "fileloc": None,
        }

        for line in lines:
            line = line.strip()
            if ":" in line:
                key, value = line.split(":", 1)
                key = key.strip().lower()
                value = value.strip()

                if "paused" in key:
                    dag_info["is_paused"] = value.lower() in ["true", "yes", "1"]
                    dag_info["is_active"] = not dag_info["is_paused"]
                elif "description" in key:
                    dag_info["description"] = value
                elif "owner" in key:
                    dag_info["owner"] = value
                elif "schedule" in key:
                    dag_info["schedule_interval"] = value if value != "None" else None
                elif "start_date" in key:
                    dag_info["start_date"] = value if value != "None" else None
                elif "fileloc" in key or "file" in key:
                    dag_info["fileloc"] = value

        # get tasks
        try:
            tasks = await self.get_dag_tasks(dag_id)
        except Exception as e:
            print(f"Warning: Failed to get tasks for DAG {dag_id}: {str(e)}")
            tasks = []

        return DAGInfo(
            dag_id=dag_info["dag_id"],
            is_active=dag_info["is_active"],
            is_paused=dag_info["is_paused"],
            description=dag_info["description"],
            owner=dag_info["owner"],
            schedule_interval=dag_info["schedule_interval"],
            start_date=dag_info["start_date"],
            tags=dag_info["tags"],
            fileloc=dag_info["fileloc"],
            tasks=tasks,
        )

    async def get_dag_tasks(self, dag_id: str) -> List[TaskInfo]:
        """airflow tasks list {dag_id} --output json"""
        try:
            result = await self._run_command(
                ["airflow", "tasks", "list", dag_id, "--output", "json"]
            )

            tasks = []
            for task_data in result:
                tasks.append(
                    TaskInfo(
                        task_id=task_data["task_id"],
                        dag_id=dag_id,
                        state="unknown",  # CLI에서는 task state 별도 조회 필요
                        operator=task_data.get("operator", ""),
                        pool=task_data.get("pool", "default"),
                        retries=task_data.get("retries", 0),
                        depends_on=task_data.get("depends_on_past", []),
                    )
                )

            return tasks

        except Exception:
            # Fallback to text parsing
            try:
                output = await self._run_command(
                    ["airflow", "tasks", "list", dag_id], parse_json=False
                )
                return self._parse_task_list_text(output, dag_id)
            except:
                return []

    async def get_dag_structure(self, dag_id: str) -> str:
        """airflow dags show {dag_id}"""
        try:
            result = await self._run_command(
                ["airflow", "dags", "show", dag_id], parse_json=False
            )
            return result
        except Exception as e:
            return f"Error retrieving DAG structure: {str(e)}"

    async def get_dag_runs(self, dag_id: str, limit: int = 10) -> List[DAGRun]:
        """airflow dags list-runs {dag_id} --output json --limit {limit}"""
        try:
            result = await self._run_command(
                [
                    "airflow",
                    "dags",
                    "list-runs",
                    dag_id,
                    "--output",
                    "json",
                    "--limit",
                    str(limit),
                ]
            )

            runs = []
            for run_data in result:
                runs.append(
                    DAGRun(
                        dag_id=dag_id,
                        run_id=run_data["run_id"],
                        state=run_data["state"],
                        execution_date=run_data["execution_date"],
                        start_date=run_data.get("start_date"),
                        end_date=run_data.get("end_date"),
                        duration=self._calculate_duration(
                            run_data.get("start_date"), run_data.get("end_date")
                        ),
                        run_type=run_data.get("run_type", "scheduled"),
                    )
                )

            return runs

        except Exception:
            return []

    async def get_dag_runs_with_plan(self, dag_id: str, limit: int = 10) -> str:
        """airflow dags list-runs -d {dag_id} --plan"""
        try:
            result = await self._run_command(
                ["airflow", "dags", "list-runs", "-d", dag_id, "--plan"],
                parse_json=False,
            )
            return result
        except Exception as e:
            return f"Error retrieving DAG runs plan: {str(e)}"

    async def get_task_instances(
        self, dag_id: str, task_id: str, limit: int = 10
    ) -> List[Dict]:
        """airflow tasks states-for-dag-run {dag_id} {execution_date}"""
        try:
            # get recent DAG runs
            dag_runs = await self.get_dag_runs(dag_id, limit)

            instances = []
            for run in dag_runs[:limit]:
                try:
                    state_result = await self._run_command(
                        [
                            "airflow",
                            "tasks",
                            "state",
                            dag_id,
                            task_id,
                            run.execution_date,
                        ],
                        parse_json=False,
                    )

                    instances.append(
                        {
                            "execution_date": run.execution_date,
                            "state": state_result.strip(),
                            "start_date": run.start_date,
                            "end_date": run.end_date,
                            "duration": run.duration,
                        }
                    )
                except:
                    continue

            return instances

        except Exception:
            return []

    async def get_task_logs(
        self, dag_id: str, task_id: str, execution_date: str
    ) -> str:
        """airflow tasks logs {dag_id} {task_id} {execution_date}"""
        try:
            result = await self._run_command(
                ["airflow", "tasks", "logs", dag_id, task_id, execution_date],
                parse_json=False,
            )

            return result

        except Exception as e:
            return f"Error retrieving logs: {str(e)}"

    async def trigger_dag(self, dag_id: str, conf: Optional[Dict] = None) -> bool:
        """airflow dags trigger {dag_id} [--conf {conf}]"""
        try:
            cmd = ["airflow", "dags", "trigger", dag_id]

            if conf:
                cmd.extend(["--conf", json.dumps(conf)])

            await self._run_command(cmd, parse_json=False)
            return True

        except Exception:
            return False

    async def pause_dag(self, dag_id: str) -> bool:
        """airflow dags pause {dag_id}"""
        try:
            await self._run_command(
                ["airflow", "dags", "pause", dag_id], parse_json=False
            )
            return True
        except:
            return False

    async def unpause_dag(self, dag_id: str) -> bool:
        """airflow dags unpause {dag_id}"""
        try:
            await self._run_command(
                ["airflow", "dags", "unpause", dag_id], parse_json=False
            )
            return True
        except:
            return False

    async def test_task(self, dag_id: str, task_id: str, execution_date: str) -> bool:
        """airflow tasks test {dag_id} {task_id} {execution_date}"""
        try:
            await self._run_command(
                ["airflow", "tasks", "test", dag_id, task_id, execution_date],
                parse_json=False,
            )
            return True
        except:
            return False

    async def backfill_dag(self, dag_id: str, start_date: str, end_date: str) -> bool:
        """airflow dags backfill {dag_id} --start-date {start_date} --end-date {end_date}"""
        try:
            await self._run_command(
                [
                    "airflow",
                    "dags",
                    "backfill",
                    dag_id,
                    "--start-date",
                    start_date,
                    "--end-date",
                    end_date,
                    "--yes",  # 자동 확인
                ],
                parse_json=False,
            )
            return True
        except:
            return False

    # Connection
    async def get_connections(self) -> List[Connection]:
        """airflow connections list --output json"""
        try:
            result = await self._run_command(
                ["airflow", "connections", "list", "--output", "json"]
            )

            connections = []
            for conn_data in result:
                connections.append(
                    Connection(
                        conn_id=conn_data["conn_id"],
                        conn_type=conn_data["conn_type"],
                        host=conn_data.get("host"),
                        port=conn_data.get("port"),
                        login=conn_data.get("login"),
                        schema=conn_data.get("schema"),
                    )
                )

            return connections

        except Exception:
            # Fallback to text parsing
            try:
                output = await self._run_command(
                    ["airflow", "connections", "list"], parse_json=False
                )
                return self._parse_connections_text(output)
            except:
                return []

    async def add_connection(self, connection: Connection) -> bool:
        """airflow connections add {conn_id} --conn-type {type} ..."""
        try:
            cmd = [
                "airflow",
                "connections",
                "add",
                connection.conn_id,
                "--conn-type",
                connection.conn_type,
            ]

            if connection.host:
                cmd.extend(["--conn-host", connection.host])
            if connection.port:
                cmd.extend(["--conn-port", str(connection.port)])
            if connection.login:
                cmd.extend(["--conn-login", connection.login])
            if connection.password:
                cmd.extend(["--conn-password", connection.password])
            if connection.schema:
                cmd.extend(["--conn-schema", connection.schema])

            await self._run_command(cmd, parse_json=False)
            return True

        except:
            return False

    async def delete_connection(self, conn_id: str) -> bool:
        """airflow connections delete {conn_id}"""
        try:
            await self._run_command(
                ["airflow", "connections", "delete", conn_id], parse_json=False
            )
            return True
        except:
            return False

    async def test_connection(self, conn_id: str) -> bool:
        """airflow connections test {conn_id}"""
        try:
            await self._run_command(
                ["airflow", "connections", "test", conn_id], parse_json=False
            )
            return True
        except:
            return False

    async def check_health(self) -> bool:
        """airflow db check"""
        try:
            await self._run_command(["airflow", "db", "check"], parse_json=False)
            return True
        except:
            return False

    async def get_system_stats(self) -> Dict[str, Any]:
        try:
            dags = await self.get_dags()

            active_count = sum(1 for dag in dags if dag.is_active and not dag.is_paused)
            paused_count = sum(1 for dag in dags if dag.is_paused)
            total_count = len(dags)

            return {
                "total_dags": total_count,
                "active_dags": active_count,
                "paused_dags": paused_count,
                "airflow_version": await self._get_airflow_version(),
            }
        except:
            return {}

    async def _get_airflow_version(self) -> str:
        try:
            result = await self._run_command(["airflow", "version"], parse_json=False)
            return result.strip()
        except:
            return "unknown"

    # Helper methods for text parsing
    def _parse_dag_list_text(self, output: str) -> List[DAGInfo]:
        """DAG 목록 텍스트 파싱"""
        lines = output.split("\n")
        dags = []

        for line in lines:
            if "|" in line and not line.startswith("dag_id"):
                parts = [p.strip() for p in line.split("|")]
                if len(parts) >= 3:
                    dags.append(
                        DAGInfo(
                            dag_id=parts[0],
                            is_active=parts[2].lower() != "true",  # paused 컬럼
                            is_paused=parts[2].lower() == "true",
                            description="",
                            owner=parts[1] if len(parts) > 1 else "",
                        )
                    )

        return dags

    def _parse_task_list_text(self, output: str, dag_id: str) -> List[TaskInfo]:
        lines = output.split("\n")
        tasks = []

        for line in lines:
            line = line.strip()
            if line and not line.startswith("---") and line != dag_id:
                tasks.append(
                    TaskInfo(
                        task_id=line, dag_id=dag_id, state="unknown", operator="Unknown"
                    )
                )

        return tasks

    def _parse_connections_text(self, output: str) -> List[Connection]:
        lines = output.split("\n")
        connections = []

        for line in lines:
            if "|" in line and not line.startswith("conn_id"):
                parts = [p.strip() for p in line.split("|")]
                if len(parts) >= 2:
                    connections.append(
                        Connection(
                            conn_id=parts[0],
                            conn_type=parts[1],
                            host=parts[2] if len(parts) > 2 else None,
                        )
                    )

        return connections

    def _calculate_duration(self, start_date: str, end_date: str) -> str:
        if not start_date or not end_date:
            return ""

        try:
            start = datetime.fromisoformat(
                start_date.replace("T", " ").replace("Z", "")
            )
            end = datetime.fromisoformat(end_date.replace("T", " ").replace("Z", ""))
            duration = end - start

            minutes = duration.total_seconds() // 60
            seconds = duration.total_seconds() % 60

            return f"{int(minutes)}m {int(seconds)}s"
        except:
            return ""


# 팩토리에 CLI 클라이언트 등록을 위한 import 에러 방지
from src.tui.clients.base import AirflowConnectionError
