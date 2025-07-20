from src.tui.clients.base import AirflowClient, ClientFactory
from src.tui.clients.cli_client import AirflowCLIClient

from src.tui.clients import registry

# 클라이언트 등록은 registry에서 자동으로 처리

__all__ = ["AirflowClient", "ClientFactory", "AirflowCLIClient"]
