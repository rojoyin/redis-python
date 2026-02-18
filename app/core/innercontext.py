from dataclasses import dataclass
from socket import socket

from app.core.configstorage import ConfigStorage
from app.core.datastorage import DataStorage


@dataclass
class InnerContext:
    store: DataStorage
    connection: socket
    config_store: ConfigStorage
    connection_to_main_server: socket | None
