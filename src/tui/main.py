import sys
import os
import argparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


from src.tui.app import create_app
from src.tui.clients.registry import get_registered_clients, create_client


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Airflow TUI - Terminal User Interface for Apache Airflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  airflow-tui                                    # Use CLI client with default settings
        """,
    )

    available_clients = get_registered_clients()
    parser.add_argument(
        "--client-type",
        choices=available_clients,
        default="cli",
        help=f"Airflow client type to use (default: cli). Available: {', '.join(available_clients)}",
    )

    parser.add_argument(
        "--airflow-home", type=str, help="Airflow home directory (for CLI client)"
    )

    parser.add_argument("--airflow-config", type=str, help="Path to airflow.cfg file")

    parser.add_argument(
        "--api-url", type=str, help="Airflow REST API URL (for REST client)"
    )

    parser.add_argument(
        "--api-username", type=str, help="Airflow API username (for REST client)"
    )

    parser.add_argument(
        "--api-password", type=str, help="Airflow API password (for REST client)"
    )

    parser.add_argument("--debug", action="store_true", help="Enable debug mode")

    parser.add_argument("--version", action="version", version="Airflow TUI 0.1.0")

    return parser.parse_args()


def setup_environment(args):
    # Airflow Home 설정
    if args.airflow_home:
        os.environ["AIRFLOW_HOME"] = args.airflow_home

    # Airflow Config 설정
    if args.airflow_config:
        os.environ["AIRFLOW_CONFIG"] = args.airflow_config

    # 디버그 모드
    if args.debug:
        os.environ["TEXTUAL_DEBUG"] = "1"


def validate_environment(args):
    if args.client_type == "cli":
        # Airflow CLI 사용 가능한지 확인
        import shutil

        if not shutil.which("airflow"):
            print("❌ Error: Airflow CLI not found in PATH")
            print("Please install Apache Airflow or add it to your PATH")
            sys.exit(1)

    elif args.client_type == "rest":
        # REST API 설정 확인
        if not args.api_url:
            print("❌ Error: --api-url is required for REST client")
            sys.exit(1)


def create_client_kwargs(args):
    kwargs = {}

    if args.client_type == "cli":
        if args.airflow_home:
            kwargs["airflow_home"] = args.airflow_home

    elif args.client_type == "rest":
        kwargs.update(
            {
                "api_url": args.api_url,
                "username": args.api_username,
                "password": args.api_password,
            }
        )

    return kwargs


def main():
    try:
        args = parse_arguments()
        setup_environment(args)
        validate_environment(args)
        client_kwargs = create_client_kwargs(args)
        print(f"Starting Airflow TUI with {args.client_type} client...")
        app = create_app(client_type=args.client_type, **client_kwargs)
        app.run()

    except KeyboardInterrupt:
        print("\n Goodbye!")
        sys.exit(0)

    except Exception as e:
        print(f"Fatal error: {str(e)}")
        if args.debug:
            import traceback

            traceback.print_exc()
        sys.exit(1)


def cli_main():
    main()


if __name__ == "__main__":
    main()
