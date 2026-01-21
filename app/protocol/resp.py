import socket

def read_command(connection: socket.socket) -> tuple[bytes, list[bytes]]:
    raw = connection.recv(1024)

    if not raw:
        raise ConnectionError("Client disconnected")

    semi_parsed = raw.split(b"\r\n", maxsplit=2)
    command_size_spec = semi_parsed.pop(0)
    parsed_command_size = int(command_size_spec[1:])
    reminder = semi_parsed.pop()
    tokenized_reminder = reminder.split(b"\r\n", maxsplit=1)
    main_command = tokenized_reminder.pop(0)
    parsed_command = [main_command]
    raw_arguments = tokenized_reminder.pop(0)

    while len(parsed_command) < parsed_command_size:
        semi_parsed_arg = raw_arguments.split(b"\r\n", maxsplit=1)
        arg_size = int(semi_parsed_arg.pop(0)[1:])
        arg_value = semi_parsed_arg[0][:arg_size]
        parsed_command.append(arg_value)
        raw_arguments = semi_parsed_arg[0][arg_size + 2:]

    command = parsed_command[0]
    args = parsed_command[1:]
    return command, args

