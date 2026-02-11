import argparse
from pathlib import Path

from app.core.configstorage import ConfigStorage
from app.core.datastorage import DataStorage
from app.core.rdb_parser import RDBParser


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


def load_rdb_data(store: DataStorage, config_store: ConfigStorage, rdb_parser: RDBParser) -> None:
    dir_path = config_store.get(b"dir")
    dbfilename = config_store.get(b"dbfilename")

    if not dir_path or not dbfilename:
        return

    rdb_filepath = Path(dir_path.decode()) / dbfilename.decode()

    if not rdb_filepath.exists():
        print(f"RDB file not found: {rdb_filepath}, skipping load")
        return

    parsed_data = rdb_parser.parse_file(rdb_filepath)
    for key, value in parsed_data.items():
        store.set(key, value)
