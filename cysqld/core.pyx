from io import BufferedReader, SEEK_CUR
from typing import Optional, Callable
from cpython.bytes cimport PyBytes_GET_SIZE

__all__ = ("Parser", )

cdef class Parser:
    cdef object _reader
    cdef str path
    cdef dict tables
    cdef object decode_callback

    def __cinit__(self, path: str, decode_callback: Optional[Callable[[bytes], str]] = None):
        self.path = path
        self.reader = None
        self.tables = {}
        self.decode_callback = bytes.decode if decode_callback is None else decode_callback

    @property
    def reader(self) -> Optional[BufferedReader]:
        return self._reader

    @reader.setter
    def reader(self, value: BufferedReader):
        self._reader = value

    cdef str _decode(self, b: bytes):
        try:
            return self.decode_callback(b)
        except Exception as err:
            err.add_note(f"Issues while decoding bytes: {b=}")
            raise err

    cdef object _tell(self):
        return self.reader.tell()

    cdef int _consume_whitespace(self):
        cdef bytes char
        cdef int consumed_length = 0
        while True:
            char = self.reader.read(1)
            if not char.isspace():
                break
            consumed_length += 1
        self.reader.seek(-1, SEEK_CUR)
        return consumed_length

    cdef bytes _consume_until(self, bytes char_end):
        cdef bytearray out = bytearray()
        cdef bytes read_char
        while True:
            read_char = self.reader.read(1)
            if read_char == char_end:
                break
            else:
                out.extend(read_char)
        return bytes(out)

    cdef bint _consume_attempt(self, bytes term):
        cdef unsigned char b
        cdef Py_ssize_t i, n = PyBytes_GET_SIZE(term)
        for i in range(n):
            b = self.reader.read(1)[0]
            if b != term[i]:
                self.reader.seek(int(i) - 1, SEEK_CUR)
                return False
        return True

    cdef bytes _parse_string(self, bytes encloser, bint consumed_first):
        if not consumed_first:
            assert self._consume_attempt(encloser)
        cdef bytearray out = bytearray()
        cdef bytes ch, nxt
        while True:
            ch = self.reader.read(1)
            if ch == b'':
                raise ValueError("unterminated string")
            if ch == encloser:
                break
            if ch != b'\\':
                out.extend(ch)
                continue
            # handle backslash escapes
            nxt = self.reader.read(1)
            if nxt == b'':
                raise ValueError("dangling backslash")
            if nxt == b'\\': out.append(0x5C)   # \
            elif nxt == b"'": out.append(0x27) # '
            elif nxt == b'"': out.append(0x22) # "
            elif nxt == b'n': out.append(0x0A) # LF
            elif nxt == b'r': out.append(0x0D) # CR
            elif nxt == b't': out.append(0x09) # TAB
            elif nxt == b'b': out.append(0x08) # BS
            elif nxt == b'0': out.append(0x00) # NUL
            elif nxt == b'Z': out.append(0x1A) # Ctrl-Z
            elif nxt == encloser:
                out.extend(encloser) # escaped quote
            else:
                out.append(nxt[0])
        return bytes(out)

    cdef object _parse_int(self):
        cdef bytes result = b""
        cdef bytes ch
        if self._consume_attempt(b"-"):
            result += b"-"
        while True:
            ch = self.reader.read(1)
            if ch.isdigit():
                result += ch
            else:
                break
        self.reader.seek(-1, SEEK_CUR)
        return int(result)

    cdef list _parse_array(self, bint is_fields):
        cdef list result = []
        cdef bytes char
        cdef object value
        cdef bint should_break
        while True:
            self._consume_whitespace()
            char = self.reader.read(1)
            self.reader.seek(-1, SEEK_CUR)
            if char == b"`":
                value = self._parse_string(b"`", consumed_first=False).decode()
            elif is_fields is False and char == b"'":
                value = self._decode(self._parse_string(b"'", consumed_first=False))
            elif is_fields is False and (char.isdigit() or char == b"-"):
                value = self._parse_int()
            elif is_fields is False and char == b"N":
                assert self._consume_attempt(b"NULL")
                value = None
            elif char == b")": # Empty array
                break
            else:
                raise AssertionError(f"{char=}, {is_fields=}")
            self._consume_whitespace()
            if self._consume_attempt(b")"):
                should_break = True
            elif self._consume_attempt(b","):
                should_break = False
            else:
                raise AssertionError
            result.append(value)
            if should_break is True:
                break
        return result

    def _parse_insert_into(self, object start_pos):
        self.reader.seek(start_pos)
        assert self._consume_attempt(b"INSERT")
        assert self._consume_whitespace() > 0
        assert self._consume_attempt(b"INTO")
        assert self._consume_whitespace() > 0
        cdef bytes raw_table = self._parse_string(encloser=b"`", consumed_first=False)
        assert PyBytes_GET_SIZE(raw_table) > 0
        cdef str table_name = raw_table.decode()
        assert self._consume_whitespace() > 0
        cdef tuple spec
        if self._consume_attempt(b"(") is True:
            spec = tuple(self._parse_array(is_fields=True))
            assert self._consume_whitespace()
        else:
            spec = self.tables[table_name]
        assert self._consume_attempt(b"VALUES")
        assert self._consume_whitespace()
        cdef list raw_vals
        while True:
            assert self._consume_attempt(b"(")
            raw_vals = self._parse_array(is_fields=False)
            yield (table_name, dict(zip(spec, raw_vals)))
            self._consume_whitespace()
            if self._consume_attempt(b";") is True:
                break
            assert self._consume_attempt(b",")
            self._consume_whitespace()
    
    cdef void _parse_create_table(self, object start_pos):
        self.reader.seek(start_pos)
        assert self._consume_attempt(b"CREATE")
        assert self._consume_whitespace() > 0
        assert self._consume_attempt(b"TABLE")
        assert self._consume_whitespace() > 0
        if self._consume_attempt(b"IF NOT EXISTS"):
            assert self._consume_whitespace() > 0
        cdef bytes raw_table = self._parse_string(encloser=b"`", consumed_first=False)
        assert self._consume_whitespace() > 0
        assert self._consume_attempt(b"(")
        cdef list fields = []
        cdef bint is_field_def
        cdef bytes skipped_bytes
        while True:
            assert self._consume_whitespace() > 0
            if self._consume_attempt(b"`"):
                fields.append(self._parse_string(encloser=b"`", consumed_first=True))
                is_field_def = True
            else:
                is_field_def = False
            skipped_bytes = self._consume_safe_until(terms=(b",\n", b"\n)"), is_field_def=is_field_def)
            if skipped_bytes.endswith(b")"):
                break
            assert PyBytes_GET_SIZE(skipped_bytes) > 0
        self.tables[raw_table.decode()] = tuple(map(bytes.decode, fields))

    cdef bytes _consume_safe_until(self, tuple terms, bint is_field_def):
        cdef bytes result = b""
        cdef bytes char
        while True:
            char = self.reader.read(1)
            result += char
            if char == b"(":
                self._parse_array(is_fields=is_field_def is False)
            elif char in b"'`":
                self._parse_string(encloser=char, consumed_first=True)
            elif result.endswith(terms):
                break
        return result

    def _parse(self):
        cdef bytes line
        cdef object start_pos
        try:
            while True:
                start_pos = self._tell()
                line = self.reader.readline()
                if PyBytes_GET_SIZE(line) == 0:
                    break
                elif line.startswith(b"INSERT "):
                    yield from self._parse_insert_into(start_pos=start_pos)
                elif line.startswith(b"CREATE "):
                    self._parse_create_table(start_pos=start_pos)
        except Exception as err:
            err.add_note(f"File position: {self._tell()}, next 20 chars: {repr(self.reader.read(0x20))}")
            raise err

    def parse(self):
        with open(self.path, "rb") as read:
            with BufferedReader(read, buffer_size=0x8000) as reader:
                self.reader = reader
                yield from self._parse()
            self.reader = None