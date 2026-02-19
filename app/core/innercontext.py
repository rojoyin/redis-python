from dataclasses import dataclass
from socket import socket

from app.core.configstorage import ConfigStorage
from app.core.datastorage import DataStorage
from app.replication.client import ReplicationClient


@dataclass
class InnerContext:
    store: DataStorage
    connection_to_client: socket
    config_store: ConfigStorage
    replication_client: ReplicationClient | None
