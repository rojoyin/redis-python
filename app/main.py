import socket  # noqa: F401
import threading

from concurrent.futures import ThreadPoolExecutor

storage = {}

def handle_connection(connection: socket.socket):
    try:
        remote_name = connection.getpeername()
        print(f"New connection created, remote: {remote_name}")
        while True:
            data = connection.recv(1024)

            if not data:
                break

            semi_parsed = data.split(b"\r\n", maxsplit=2)
            command_size_spec = semi_parsed.pop(0)
            parsed_command_size = int(command_size_spec[1:])
            reminder = semi_parsed.pop()
            tokenized_reminder = reminder.split(b"\r\n", maxsplit=1)
            main_command = tokenized_reminder.pop(0)
            parsed_command = [main_command]
            raw_arguments = tokenized_reminder.pop(0)

            while len(parsed_command)<parsed_command_size:
                semi_parsed_arg = raw_arguments.split(b"\r\n", maxsplit=1)
                arg_size = int(semi_parsed_arg.pop(0)[1:])
                arg_value = semi_parsed_arg[0][:arg_size]
                parsed_command.append(arg_value)
                raw_arguments = semi_parsed_arg[0][arg_size+2:]

            command = parsed_command[0]
            if command == b"PING":
                connection.sendall(b"+PONG\r\n")
            elif command == b"ECHO":
                response = f"${len(parsed_command[1])}\r\n{parsed_command[1].decode('utf-8')}\r\n"
                connection.sendall(response.encode())
            elif command == b"COMMAND":
                connection.sendall(b"*0\r\n")
            elif command == b"SET":
                var_name, var_value = parsed_command[1], parsed_command[2]
                parsed_command = [v.lower() for v in parsed_command]

                parse_as_milliseconds = False

                if len(parsed_command) > 3:
                    if b"px" in parsed_command:
                        ttl_flag_index = parsed_command.index(b"px")
                        parse_as_milliseconds = True
                    elif b"ex" in parsed_command:
                        ttl_flag_index = parsed_command.index(b"ex")
                    else:
                        raise Exception(f"Unsupported option for {command}")

                    val = int(parsed_command[ttl_flag_index + 1])
                    ttl_value = val if not parse_as_milliseconds else val / 1000
                    threading.Timer(ttl_value, storage.pop, args=[var_name]).start()

                storage[var_name] = var_value
                connection.sendall(b"+OK\r\n")
            elif command == b"GET":
                _, var_name = parsed_command
                value = storage.get(var_name)

                if value is not None:
                    response = f"${len(value)}\r\n{value.decode('utf-8')}\r\n"
                else:
                    response = "$-1\r\n"

                connection.sendall(response.encode())
            else:
                return

            print(f"Replying to remote: {remote_name}")
    except Exception as e:
        print(f"Thread exception: {e}")
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
