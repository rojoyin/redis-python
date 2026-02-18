import socket
from concurrent.futures import ThreadPoolExecutor

from app.commands.registry import registry
from app.core.configstorage import ConfigStorage
from app.core.datastorage import DataStorage
from app.core.innercontext import InnerContext
from app.protocol import resp


def handle_connection(
    connection: socket.socket,
    store: DataStorage,
    config_store: ConfigStorage,
    connection_to_main_server: socket.socket | None,
):
    try:
        remote_name = connection.getpeername()
        print(f"New connection created, remote: {remote_name}")
        context = InnerContext(
            connection=connection,
            store=store,
            config_store=config_store,
            connection_to_main_server=connection_to_main_server
        )

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
    main_server_data = config_store.get(b"replicaof")
    connection_to_main_server = None

    if main_server_data:
        main_server_address, main_server_port = main_server_data.split(b" ")
        connection_to_main_server = socket.create_connection(
            (
                main_server_address.decode("utf-8"),
                int(main_server_port.decode("utf-8"))
            )
        )

        ping_command = "*1\r\n$4\r\nPING\r\n"
        connection_to_main_server.sendall(ping_command.encode())


    with ThreadPoolExecutor() as executor:
        while True:
            connection_to_client, _ = server_socket.accept()
            executor.submit(handle_connection, connection_to_client, store, config_store, connection_to_main_server)
