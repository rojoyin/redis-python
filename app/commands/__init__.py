def load_commands() -> None:
    from app.commands.connection import ping, echo
    from app.commands.server import command, config, info
    from app.commands.string import set, get
    from app.commands.generic import keys
