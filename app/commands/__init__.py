def load_commands() -> None:
    from app.commands.connection import ping, echo
    from app.commands.server import command, config
    from app.commands.string import set, get

