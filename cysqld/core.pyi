from typing import Generator, Any, Optional, Callable

__all__: tuple[str]

class Parser:
    def __init__(self, path: str, decode_callback: Optional[Callable[[bytes], str]] = None): ...
    def parse(self) -> Generator[tuple[str, dict[str, Any]], None, None]: ...