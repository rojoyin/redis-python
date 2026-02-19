import socket
from concurrent.futures import ThreadPoolExecutor

from app.commands.registry import registry
from app.core.configstorage import ConfigStorage
from app.core.datastorage import DataStorage
from app.core.innercontext import InnerContext
from app.protocol import resp
from app.replication.client import ReplicationClient


def handle_client_connection(
    connection_to_client: socket.socket,
    store: DataStorage,
    config_store: ConfigStorage,
    replication_client: ReplicationClient | None,
):
    try:
        remote_name = connection_to_client.getpeername()
        print(f"New connection created, remote: {remote_name}")
        context = InnerContext(
            connection_to_client=connection_to_client,
            store=store,
            config_store=config_store,
            replication_client=replication_client
        )

        while True:
            command, args = resp.read_command(context.connection_to_client)
            command_handler = registry.get_command_handler(command)
            result = command_handler(args, context)
            connection_to_client.sendall(resp.encode_response(result))
            print(f"Replying to remote: {remote_name}")

    except Exception as e:
        print(f"Thread exception: {e}")
    finally:
        connection_to_client.close()


def run(*, store: DataStorage, config_store: ConfigStorage) -> None:
    host = config_store.get("host")
    port = config_store.get("port")
    server_socket = socket.create_server((host, port), reuse_port=True)
    main_server_data = config_store.get("replicaof")

    replication_client = None
    if main_server_data:
        replication_client = ReplicationClient(main_server_data)
        replication_client.start()

    with ThreadPoolExecutor() as executor:
        while True:
            connection_to_client, _ = server_socket.accept()
            executor.submit(
                handle_client_connection,
                connection_to_client,
                store,
                config_store,
                replication_client,
            )
