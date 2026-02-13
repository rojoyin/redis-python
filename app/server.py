import socket
from concurrent.futures import ThreadPoolExecutor

from app.commands.registry import registry
from app.core.configstorage import ConfigStorage
from app.core.datastorage import DataStorage
from app.core.innercontext import InnerContext
from app.protocol import resp


def handle_connection(connection: socket.socket, store: DataStorage, config_store: ConfigStorage):
    try:
        remote_name = connection.getpeername()
        print(f"New connection created, remote: {remote_name}")
        context = InnerContext(connection=connection, store=store, config_store=config_store)

        while True:
            command, args = resp.read_command(context.connection)
            command_handler = registry.get_command_handler(command)
            result = command_handler(args, context)
            connection.sendall(resp.encode_response(result))
            print(f"Replying to remote: {remote_name}")

    except Exception as e:
        print(f"Thread exception: {e}")
    finally:
        connection.close()


def run(*, store: DataStorage, config_store: ConfigStorage) -> None:
    host = config_store.get(b"host")
    port = config_store.get(b"port")
    server_socket = socket.create_server((host, port), reuse_port=True)

    with ThreadPoolExecutor() as executor:
        while True:
            connection, _ = server_socket.accept()
            executor.submit(handle_connection, connection, store, config_store)
