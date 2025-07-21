from dataclasses import dataclass
from typing import Optional, Dict, Any
import json


@dataclass
class Connection:
    """Airflow Connection 정보"""

    conn_id: str
    conn_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None
    extra: Optional[str] = None
    description: Optional[str] = None
    is_encrypted: bool = False
    is_extra_encrypted: bool = False

    def __post_init__(self):
        """초기화 후 처리"""
        # port가 문자열이면 정수로 변환 시도
        if isinstance(self.port, str):
            try:
                self.port = int(self.port) if self.port else None
            except ValueError:
                self.port = None

    @property
    def uri(self) -> str:
        """Connection URI 생성"""
        uri_parts = [self.conn_type, "://"]

        if self.login:
            uri_parts.append(self.login)
            if self.password:
                uri_parts.append(f":{self.password}")
            uri_parts.append("@")

        if self.host:
            uri_parts.append(self.host)

        if self.port:
            uri_parts.append(f":{self.port}")

        if self.schema:
            uri_parts.append(f"/{self.schema}")

        return "".join(uri_parts)

    @property
    def display_password(self) -> str:
        """표시용 마스킹된 패스워드"""
        if not self.password:
            return ""
        return "*" * len(self.password)

    @property
    def extra_dict(self) -> Dict[str, Any]:
        """Extra 필드를 딕셔너리로 파싱"""
        if not self.extra:
            return {}

        try:
            return json.loads(self.extra)
        except (json.JSONDecodeError, TypeError):
            return {}

    @extra_dict.setter
    def extra_dict(self, value: Dict[str, Any]):
        """Extra 딕셔너리를 JSON 문자열로 저장"""
        if value:
            self.extra = json.dumps(value)
        else:
            self.extra = None

    @property
    def connection_summary(self) -> str:
        """Connection 요약 정보"""
        parts = [f"Type: {self.conn_type}"]

        if self.host:
            host_info = self.host
            if self.port:
                host_info += f":{self.port}"
            parts.append(f"Host: {host_info}")

        if self.login:
            parts.append(f"User: {self.login}")

        if self.schema:
            parts.append(f"Schema: {self.schema}")

        return " | ".join(parts)

    @classmethod
    def from_uri(cls, conn_id: str, uri: str) -> "Connection":
        """URI에서 Connection 객체 생성"""
        from urllib.parse import urlparse

        parsed = urlparse(uri)

        return cls(
            conn_id=conn_id,
            conn_type=parsed.scheme,
            host=parsed.hostname,
            port=parsed.port,
            login=parsed.username,
            password=parsed.password,
            schema=parsed.path.lstrip("/") if parsed.path else None,
        )

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            "conn_id": self.conn_id,
            "conn_type": self.conn_type,
            "host": self.host,
            "port": self.port,
            "login": self.login,
            "password": self.password,
            "schema": self.schema,
            "extra": self.extra,
            "description": self.description,
            "is_encrypted": self.is_encrypted,
            "is_extra_encrypted": self.is_extra_encrypted,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Connection":
        """딕셔너리에서 Connection 객체 생성"""
        return cls(**data)

    def test_connection(self) -> bool:
        """Connection 테스트 (실제 구현은 클라이언트에서)"""
        # 기본적인 필수 필드 검증
        if not self.conn_id or not self.conn_type:
            return False

        # 타입별 기본 검증
        if self.conn_type in ["postgres", "mysql", "sqlite"]:
            return bool(self.host or self.schema)
        elif self.conn_type == "http":
            return bool(self.host)
        elif self.conn_type == "ftp":
            return bool(self.host)

        return True

    def is_valid(self) -> bool:
        """Connection 유효성 검사"""
        if not self.conn_id or not self.conn_type:
            return False

        # conn_id에 특수문자 확인
        import re

        if not re.match(r"^[a-zA-Z0-9_-]+$", self.conn_id):
            return False

        return True

    def get_validation_errors(self) -> list:
        """유효성 검사 오류 목록"""
        errors = []

        if not self.conn_id:
            errors.append("Connection ID is required")
        elif not self.conn_id.replace("_", "").replace("-", "").isalnum():
            errors.append(
                "Connection ID can only contain letters, numbers, hyphens, and underscores"
            )

        if not self.conn_type:
            errors.append("Connection type is required")

        # 타입별 특정 검증
        if self.conn_type in ["postgres", "mysql"] and not self.host:
            errors.append(f"{self.conn_type} connections require a host")

        if self.port and (self.port < 1 or self.port > 65535):
            errors.append("Port must be between 1 and 65535")

        return errors


@dataclass
class ConnectionTest:
    """Connection 테스트 결과"""

    conn_id: str
    success: bool
    message: str
    test_duration: Optional[float] = None
    error_details: Optional[str] = None

    @property
    def status_icon(self) -> str:
        """상태 아이콘"""
        return "✅" if self.success else "❌"

    @property
    def duration_formatted(self) -> str:
        """테스트 소요시간 포맷팅"""
        if not self.test_duration:
            return "N/A"

        if self.test_duration < 1:
            return f"{self.test_duration * 1000:.0f}ms"
        else:
            return f"{self.test_duration:.2f}s"
