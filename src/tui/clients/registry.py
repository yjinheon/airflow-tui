from src.tui.clients.base import ClientFactory
from src.tui.clients.cli_client import AirflowCLIClient


def register_clients():

    ClientFactory.register_client("cli", AirflowCLIClient)

    # todo
    # ClientFactory.register_client('rest', RESTAirflowClient)
    # ClientFactory.register_client('mock', MockAirflowClient)


def get_registered_clients():
    return ClientFactory.get_available_clients()


def create_client(client_type: str, **kwargs):
    return ClientFactory.create_client(client_type, **kwargs)


register_clients()
