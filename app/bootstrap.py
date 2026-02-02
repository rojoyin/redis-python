import argparse

from app.core.configstorage import ConfigStorage


def parse_server_args(argv: list[str] | None  = None) -> argparse.Namespace:
    server_arg_parser = argparse.ArgumentParser()
    server_arg_parser.add_argument("--dir", type=str)
    server_arg_parser.add_argument("--dbfilename", type=str)
    return server_arg_parser.parse_args(argv)

def configure_server(config_storage: ConfigStorage, args: argparse.Namespace) -> None:
    if args.dir is not None:
        print(f"Set {args.dir=} configuration")
        config_storage.set(b"dir", args.dir.encode("utf-8"))
    if args.dbfilename is not None:
        print(f"Set {args.dbfilename=} configuration")
        config_storage.set(b"dbfilename", args.dbfilename.encode("utf-8"))
