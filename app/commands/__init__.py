def load_commands() -> None:
    from app.commands.connection import ping, echo
    from app.commands.server import command
    from app.commands.string import set, get

