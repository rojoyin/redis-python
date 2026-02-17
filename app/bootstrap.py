import argparse
import random
import string
from pathlib import Path

from app.core.configstorage import ConfigStorage
from app.core.datastorage import DataStorage
from app.core.rdb_parser import RDBParser


def parse_server_args(argv: list[str] | None  = None) -> argparse.Namespace:
    server_arg_parser = argparse.ArgumentParser()
    server_arg_parser.add_argument("--dir", type=str)
    server_arg_parser.add_argument("--dbfilename", type=str)
    server_arg_parser.add_argument("--host", type=str, default="localhost")
    server_arg_parser.add_argument("--port", type=int, default=6379)
    server_arg_parser.add_argument("--replicaof", type=str)
    return server_arg_parser.parse_args(argv)


def _generate_replid(size: int = 40) -> bytes:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size)).encode(encoding="ascii")


def configure_server(config_storage: ConfigStorage, args: argparse.Namespace) -> None:
    if args.dir is not None:
        print(f"Set {args.dir=} configuration")
        config_storage.set(b"dir", args.dir.encode("utf-8"))
    if args.dbfilename is not None:
        print(f"Set {args.dbfilename=} configuration")
        config_storage.set(b"dbfilename", args.dbfilename.encode("utf-8"))
    if args.replicaof is not None:
        print(f"Set {args.replicaof=} configuration")
        config_storage.set(b"replicaof", args.replicaof.encode("utf-8"))
        config_storage.set(b"role", b"slave")
    if args.replicaof is None:
        master_replid = _generate_replid(size=40)
        config_storage.set(b"role", b"master")
        config_storage.set(b"master_replid", master_replid)
        config_storage.set(b"master_repl_offset", b"0")
        print(f"Set {master_replid=}")

    print(f"Set {args.host=} configuration")
    config_storage.set(b"host", args.host)

    print(f"Set {args.port=} configuration")
    config_storage.set(b"port", args.port)


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

    for entry in parsed_data:
        store.set(entry.key, entry.value, entry.expire_ts)
