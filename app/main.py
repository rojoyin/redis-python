import socket
from concurrent.futures import ThreadPoolExecutor

from app.commands import load_commands
from app.commands.registry import registry
from app.core.datastorage import DataStorage
from app.core.innercontext import InnerContext
from app.protocol import resp

storage = DataStorage()

def handle_connection(connection: socket.socket):
    try:
        remote_name = connection.getpeername()
        print(f"New connection created, remote: {remote_name}")
        context = InnerContext(connection=connection, store=storage)
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



def main():
    print("Logs from your program will appear here!")
    load_commands()
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    with ThreadPoolExecutor() as executor:
        while True:
            connection, _ = server_socket.accept()
            executor.submit(handle_connection, connection)


if __name__ == "__main__":
    main()
