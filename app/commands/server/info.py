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
        node_role = b"role:master"

        main_node_data = context.config_store.get(b"replicaof")
        if main_node_data:
            node_role = b"role:slave"
            main_node_hostname, main_node_port = main_node_data.split(b' ')
            print(f"Main node name: {main_node_hostname}, port: {main_node_port}")

        server_info = []
        
        server_replication_info = [node_role]

        server_info.append(b"\n".join(server_replication_info))

        if parsed.subcommand == b"replication":
            return BulkString(b"\n".join(server_replication_info))

        return BulkString(b"\n".join(server_info))
