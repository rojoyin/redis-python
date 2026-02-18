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
        role = context.config_store.get("role")
        server_replication_info = ["role:" + role]

        if role == "master":
            replid = context.config_store.get("master_replid")
            offset = context.config_store.get("master_repl_offset")
            server_replication_info.extend([
                f"master_replid:{replid}",
                f"master_repl_offset:{offset}"
            ])

        server_info = ["\n".join(server_replication_info)]

        if parsed.subcommand == b"replication":
            return BulkString("\n".join(server_replication_info).encode("utf-8"))

        return BulkString("\n".join(server_info).encode("utf-8"))
