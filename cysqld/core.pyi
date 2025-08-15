from typing import Generator, Any

__all__: tuple[str]

class Parser:
    def __init__(self, path: str): ...
    def parse(self) -> Generator[tuple[str, dict[str, Any]], None, None]: ...