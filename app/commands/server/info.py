from dataclasses import dataclass

from app.commands.contract import CommandHandler
from app.core.innercontext import InnerContext
from app.protocol.types import RespValue, BulkString
from app.commands.registry import registry


@dataclass
class InfoPayload:
    subcommand: bytes | None = None


@registry.register(b"INFO")
class Handler(CommandHandler):

    def parse(self, args: list[bytes]) -> object:
        return InfoPayload(subcommand=args[0]) if args else InfoPayload()

    def execute(self, parsed: InfoPayload, context: InnerContext) -> RespValue:
        role = context.config_store.get(b"role")
        server_replication_info = [b"role:" + role]

        if role == b"master":
            replid = context.config_store.get(b"master_replid")
            offset = context.config_store.get(b"master_repl_offset")
            server_replication_info.extend([
                b"master_replid:" + replid,
                b"master_repl_offset:" + offset
            ])

        server_info = [b"\n".join(server_replication_info)]

        if parsed.subcommand == b"replication":
            return BulkString(b"\n".join(server_replication_info))

        return BulkString(b"\n".join(server_info))
