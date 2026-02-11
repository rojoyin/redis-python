from app.bootstrap import parse_server_args, configure_server, load_rdb_data
from app.commands import load_commands
from app.core.configstorage import ConfigStorage
from app.core.datastorage import DataStorage
from app.core.rdb_parser import RDBParser
from app.server import run


def main():
    print("Logs from your program will appear here!")
    load_commands()

    store = DataStorage()
    config_store = ConfigStorage()
    rdb_parser = RDBParser()
    args = parse_server_args()

    configure_server(config_store, args)
    load_rdb_data(store, config_store, rdb_parser)

    run(host="localhost", port=6379, store=store, config_store=config_store)


if __name__ == "__main__":
    main()
