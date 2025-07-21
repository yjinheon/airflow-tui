from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime


@dataclass
class TaskInfo:
    task_id: str
    dag_id: str
    state: str
    start_date: Optional[str]
    end_date: Optional[str]
    duration: Optional[str]
    operator: str = ""
    pool: str = "default"
    retries: int = 0
    timeout: Optional[str] = None
    depends_on: List[str] = field(default_factory=list)


@dataclass
class DAGRun:
    dag_id: str
    run_id: str
    state: str
    execution_date: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    duration: Optional[str] = None
    run_type: str = "scheduled"
    external_trigger: bool = False
    conf: Optional[dict] = None

    def __post_init__(self):
        # duration이 없으면 계산 시도
        if not self.duration and self.start_date and self.end_date:
            self.duration = self._calculate_duration()

    def _calculate_duration(self) -> str:
        try:
            start = datetime.fromisoformat(
                self.start_date.replace("T", " ").replace("Z", "")
            )
            end = datetime.fromisoformat(
                self.end_date.replace("T", " ").replace("Z", "")
            )
            duration = end - start

            minutes = int(duration.total_seconds() // 60)
            seconds = int(duration.total_seconds() % 60)

            return f"{minutes}m {seconds}s"
        except (ValueError, TypeError):
            return ""

    @property
    def is_success(self) -> bool:
        """성공 여부"""
        return self.state.lower() == "success"

    @property
    def is_failed(self) -> bool:
        """실패 여부"""
        return self.state.lower() == "failed"

    @property
    def is_running(self) -> bool:
        """실행 중 여부"""
        return self.state.lower() == "running"


@dataclass
class DAGInfo:
    dag_id: str
    is_active: bool
    is_paused: bool
    description: str = ""
    owner: str = ""
    schedule_interval: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    last_run: Optional[str] = None
    next_run: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    max_active_runs: int = 1
    catchup: bool = True
    concurrency: int = 16
    last_updated: Optional[str] = None
    fileloc: Optional[str] = None
    tasks: Optional[List] = field(
        default_factory=list
    )  # TaskInfo 리스트, 순환참조 방지용

    def __post_init__(self):
        if self.tags is None:
            self.tags = []

        if self.tasks is None:
            self.tasks = []

    @property
    def status(self) -> str:
        if self.is_paused:
            return "paused"
        elif not self.is_active:
            return "inactive"
        else:
            return "active"

    @property
    def status_icon(self) -> str:
        if self.is_paused:
            return "⏸️"
        elif not self.is_active:
            return "🔴"
        else:
            return "🟢"

    @property
    def task_count(self) -> int:
        return len(self.tasks) if self.tasks else 0

    def get_task_by_id(self, task_id: str):
        if not self.tasks:
            return None

        for task in self.tasks:
            if task.task_id == task_id:
                return task
        return None

    def get_failed_tasks(self) -> List:
        if not self.tasks:
            return []

        return [task for task in self.tasks if task.state == "failed"]

    def get_running_tasks(self) -> List:
        if not self.tasks:
            return []

        return [task for task in self.tasks if task.state == "running"]


@dataclass
class DAGCode:
    dag_id: str
    fileloc: str
    source_code: str
    file_token: Optional[str] = None

    @property
    def filename(self) -> str:
        """파일명만 추출"""
        import os

        return os.path.basename(self.fileloc) if self.fileloc else ""

    @property
    def line_count(self) -> int:
        """라인 수"""
        return len(self.source_code.split("\n")) if self.source_code else 0


@dataclass
class DAGStats:
    dag_id: str
    total_runs: int = 0
    success_runs: int = 0
    failed_runs: int = 0
    running_runs: int = 0
    avg_duration_seconds: float = 0.0
    last_success_date: Optional[str] = None
    last_failure_date: Optional[str] = None

    @property
    def success_rate(self) -> float:
        if self.total_runs == 0:
            return 0.0
        return self.success_runs / self.total_runs

    @property
    def success_rate_percent(self) -> str:
        return f"{self.success_rate * 100:.1f}%"

    @property
    def avg_duration_formatted(self) -> str:
        if self.avg_duration_seconds <= 0:
            return "N/A"

        minutes = int(self.avg_duration_seconds // 60)
        seconds = int(self.avg_duration_seconds % 60)

        if minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
