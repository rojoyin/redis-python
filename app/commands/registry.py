from collections import defaultdict
from typing import Callable, Type

from app.commands.contract import CommandHandler


class _CommandRegistry:

    def __init__(self):
        self._handlers: dict[bytes, CommandHandler | None] = defaultdict(lambda: None)

    def get_command_handler(self, command_name: bytes) -> CommandHandler:
        command_handler_class = self._handlers[command_name]
        return command_handler_class


    def register(self, command_name: bytes) -> Callable:
        def wrapper(handler_class: Type[CommandHandler]) -> Type[CommandHandler]:
            if not issubclass(handler_class, CommandHandler):
                raise TypeError(f"Handler {handler_class} is not of type CommandHandler")

            if self._handlers[command_name]:
                raise KeyError(f"Handler for {command_name} already registered")

            self._handlers[command_name] = handler_class()
            return handler_class

        return wrapper


registry = _CommandRegistry()
