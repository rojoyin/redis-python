import socket
import threading
from dataclasses import dataclass

from app.protocol import resp
from app.protocol.types import Array, BulkString


@dataclass(slots=True)
class MainHost:
    hostname: str
    port: int


class ReplicationClient:

    def __init__(self, main_server_data: str):
        self._main_host = self._get_main_host(main_server_data)
        self._thread: threading.Thread | None = None
        self._is_stop_requested = threading.Event()
        self._connection_to_main_server: socket.socket | None = None
        self._connection_established = threading.Event()
        self._connection_lock = threading.Lock()


    @staticmethod
    def _get_main_host(main_server_data: str) -> MainHost:
        main_server_address, main_server_port = main_server_data.split(" ")
        return MainHost(main_server_address, int(main_server_port))


    def _connect(self) -> None:
        if self._is_stop_requested.is_set():
            return

        with self._connection_lock:
            self._connection_to_main_server = socket.create_connection(
                (self._main_host.hostname, self._main_host.port)
            )


    def _run(self) -> None:
        try:
            self._connect()
            self._handshake()

            while not self._is_stop_requested:
                pass

        except Exception:
            self._connection_established.clear()
            self._close_connection_to_main_server()


    def _send_message_to_server(self, message: str) -> None:
        with self._connection_lock:
            self._connection_to_main_server.sendall(
                resp.encode_response(
                    Array([BulkString(message)])
                )
            )


    def _handshake(self) -> None:
        self._send_message_to_server("PING")


    def _close_connection_to_main_server(self):
        with self._connection_lock:
            if self._connection_to_main_server is None:
                return
            try:
                self._connection_to_main_server.close()
            finally:
                self._connection_to_main_server = None
        self._connection_established.clear()



    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(target=self._run, name="replication-client", daemon=True)
        self._thread.start()
        self._is_stop_requested.clear()


    def stop(self) -> None:
        self._is_stop_requested.set()
        self._close_connection_to_main_server()
