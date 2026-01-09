import socket  # noqa: F401

from concurrent.futures import ThreadPoolExecutor


def handle_connection(connection: socket):
    try:
        remote_name = connection.getpeername()
        print(f"New connection created, remote: {remote_name}")
        while True:
            data = connection.recv(1024)

            if not data:
                break

            print(f"Replying to remote: {remote_name}")
            connection.sendall(b"+PONG\r\n")
    except Exception:
        raise
    finally:
        connection.close()



def main():
    print("Logs from your program will appear here!")
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    with ThreadPoolExecutor() as executor:
        while True:
            connection, _ = server_socket.accept()
            executor.submit(handle_connection, connection)


if __name__ == "__main__":
    main()
