from app.bootstrap import parse_server_args, configure_server
from app.commands import load_commands
from app.core.configstorage import ConfigStorage
from app.core.datastorage import DataStorage
from app.server import run


def main():
    print("Logs from your program will appear here!")
    load_commands()

    store = DataStorage()
    config_store = ConfigStorage()

    args = parse_server_args()
    configure_server(config_store, args)

    run(host="localhost", port=6379, store=store, config_store=config_store)



if __name__ == "__main__":
    main()
