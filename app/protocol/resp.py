import socket

from app.protocol.types import RespValue, Array, SimpleString, BulkString

CRLF = b"\r\n"

def read_command(connection: socket.socket) -> tuple[bytes, list[bytes]]:
    raw = connection.recv(1024)

    if not raw:
        raise ConnectionError("Client disconnected")

    semi_parsed = raw.split(CRLF, maxsplit=2)
    command_size_spec = semi_parsed.pop(0)
    parsed_command_size = int(command_size_spec[1:])
    reminder = semi_parsed.pop()
    tokenized_reminder = reminder.split(CRLF, maxsplit=1)
    main_command = tokenized_reminder.pop(0)
    parsed_command = [main_command]
    raw_arguments = tokenized_reminder.pop(0)

    while len(parsed_command) < parsed_command_size:
        semi_parsed_arg = raw_arguments.split(CRLF, maxsplit=1)
        arg_size = int(semi_parsed_arg.pop(0)[1:])
        arg_value = semi_parsed_arg[0][:arg_size]
        parsed_command.append(arg_value)
        raw_arguments = semi_parsed_arg[0][arg_size + 2:]

    command = parsed_command[0]
    args = parsed_command[1:]
    return command, args


def encode_response(result: RespValue) -> bytes:
    if isinstance(result, Array):
        parts: list[bytes] = [b"*" + str(len(result.items)).encode("ascii") + CRLF]

        for item in result.items:
            parts.append(encode_response(item))

        return b"".join(parts)
    elif isinstance(result, SimpleString):
        return b"+" + result.text.encode("utf-8") + CRLF
    elif isinstance(result, BulkString):
        return b"$" + str(len(result.data)).encode("utf-8") + CRLF + result.data + CRLF

    return b""
