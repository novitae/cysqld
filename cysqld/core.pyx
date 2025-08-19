from io import SEEK_CUR
from typing import Optional, Callable
from cpython.bytes cimport PyBytes_GET_SIZE
# Optimized buffer access for mmap hot path
from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release, Py_buffer
from libc.string cimport memchr, memcmp
import os

__all__ = ("Parser", )

# ----- small inline predicates (no Python objects) -----
cdef inline bint _is_space(unsigned char b):
    return b == 0x20 or b in (0x09, 0x0A, 0x0D, 0x0B, 0x0C)

cdef inline bint _is_digit(unsigned char b):
    return 48 <= b <= 57

# ==============================================
# mmap-only parser. No BufferedReader anywhere.
# ==============================================
cdef class Parser:
    cdef str path
    cdef dict tables
    cdef object decode_callback

    # mmap hot path state
    cdef object _mm                 # Python mmap object
    cdef Py_ssize_t _pos            # current offset in file
    cdef Py_ssize_t _len            # file length
    cdef const unsigned char* _data # raw pointer to bytes (via Py_buffer)
    cdef Py_buffer _view            # holds acquired buffer; must release
    cdef Py_ssize_t _chunk_size     # logical chunk size hint for callers
    cdef Py_ssize_t _win_size      # bytes mapped in current window
    cdef Py_ssize_t _win_base      # file offset of window start (page-aligned)
    cdef int _page_size            # OS allocation granularity
    cdef int _fd                   # adopted file descriptor during parse()

    def __cinit__(self, path: str, decode_callback: Optional[Callable[[bytes], str]] = None):
        self.path = path
        self.tables = {}
        self.decode_callback = bytes.decode if decode_callback is None else decode_callback
        # mmap state
        self._mm = None
        self._pos = 0
        self._len = 0
        self._data = NULL
        self._view.buf = NULL
        self._view.len = 0
        # kept only as documentation for higher-level readers
        self._chunk_size = 0x8000  # 32 KiB
        self._win_size = 0
        self._win_base = 0
        self._page_size = 4096
        self._fd = -1

    # -------- public zero-copy read API over mmap --------
    def read(self, Py_ssize_t n):
        """Return a zero-copy memoryview over next at most n bytes and advance."""
        if n <= 0 or self._pos >= self._len:
            return memoryview(b"")
        if not (self._win_base <= self._pos < self._win_base + self._win_size):
            self._map_window(self._pos)
        cdef Py_ssize_t end = self._pos + n
        cdef Py_ssize_t win_end = self._win_base + self._win_size
        if end > win_end:
            end = win_end
        if end > self._len:
            end = self._len
        mv = memoryview(self._mm)[self._pos - self._win_base:end - self._win_base]
        self._pos = end
        return mv

    def peek(self):
        """Return a zero-copy 1-byte memoryview at current cursor without advancing."""
        if self._pos >= self._len:
            return memoryview(b"")
        if not (self._win_base <= self._pos < self._win_base + self._win_size):
            self._map_window(self._pos)
        if self._win_size == 0:
            return memoryview(b"")
        return memoryview(self._mm)[self._pos - self._win_base:self._pos - self._win_base + 1]

    # -------- tiny helpers operating on integers/pointers --------
    cdef inline object _tell(self):
        return self._pos

    cdef inline void _seek(self, Py_ssize_t offset, int whence=SEEK_CUR):
        if whence == SEEK_CUR:
            self._pos += offset
        elif whence == 0:  # SEEK_SET
            self._pos = offset
        else:              # SEEK_END
            self._pos = self._len + offset
        if self._pos < 0:
            self._pos = 0
        elif self._pos > self._len:
            self._pos = self._len

    cdef inline void _map_window(self, Py_ssize_t want_abs_pos):
        """Map a window covering want_abs_pos. Zero-copy within window.
        We align mapping to OS page size and map up to _chunk_size bytes.
        """
        cdef Py_ssize_t base = want_abs_pos - (want_abs_pos % self._page_size)
        cdef Py_ssize_t offset_in_win = want_abs_pos - base
        cdef Py_ssize_t remain = self._len - base
        cdef Py_ssize_t length = self._chunk_size
        if length > remain:
            length = remain
        if length < 0:
            length = 0
        # Drop previous buffer view and mapping, if any
        if self._view.buf != NULL:
            PyBuffer_Release(&self._view)
            self._view.buf = NULL
            self._view.len = 0
        if self._mm is not None:
            self._mm.close()
            self._mm = None
        if length == 0:
            self._data = NULL
            self._win_size = 0
            self._win_base = base
            return
        # Create a new mapping for [base, base+length)
        import mmap
        self._mm = mmap.mmap(self._fd, length, access=mmap.ACCESS_READ, offset=base)
        PyObject_GetBuffer(self._mm, &self._view, 0)  # PyBUF_SIMPLE
        self._data = <const unsigned char*> self._view.buf
        self._win_size = length
        self._win_base = base
        # Adjust absolute cursor if it fell before base due to alignment
        self._pos = base + offset_in_win

    cdef inline bint _ensure_window(self, Py_ssize_t need):
        if self._pos >= self._len:
            return False
        if not (self._win_base <= self._pos < self._win_base + self._win_size):
            self._map_window(self._pos)
        cdef Py_ssize_t within = (self._win_base + self._win_size) - self._pos
        if within >= need or self._pos + need <= self._len:
            return True
        return True  # caller may call again as we slide

    cdef inline int _peek_byte_int(self):
        if self._pos >= self._len:
            return -1
        if not (self._win_base <= self._pos < self._win_base + self._win_size):
            self._map_window(self._pos)
            if self._win_size == 0:
                return -1
        return <int>self._data[self._pos - self._win_base]

    cdef inline int _read_byte_int(self):
        cdef int b = self._peek_byte_int()
        if b >= 0:
            self._pos += 1
        return b

    cdef inline bint _starts_with_at(self, Py_ssize_t pos, bytes term):
        cdef Py_ssize_t n, off, avail, rest_pos, rest_len
        cdef const unsigned char* tptr
        n = PyBytes_GET_SIZE(term)
        tptr = <const unsigned char*> term
        if pos + n > self._len:
            return False
        # Make sure [pos, pos+n) is inside current window; remap if needed
        if not (self._win_base <= pos and pos + n <= self._win_base + self._win_size):
            self._map_window(pos)
            if not (self._win_base <= pos and pos + n <= self._win_base + self._win_size):
                # term spans windows; compare the available prefix and fall back byte-wise
                off = pos - self._win_base
                avail = self._win_size - off
                if avail <= 0:
                    return False
                if memcmp(<const void*>(self._data + off), <const void*>(tptr), <size_t>avail) != 0:
                    return False
                # Slide to cover the rest
                rest_pos = pos + avail
                rest_len = n - avail
                self._map_window(rest_pos)
                if not (self._win_base <= rest_pos and rest_pos + rest_len <= self._win_base + self._win_size):
                    return False
                return memcmp(<const void*>(self._data + (rest_pos - self._win_base)), <const void*>(tptr + avail), <size_t>rest_len) == 0
        # Fast path: fully inside window
        return memcmp(<const void*>(self._data + (pos - self._win_base)), <const void*>tptr, <size_t>n) == 0

    cdef object _readline_view(self):
        """Return a memoryview of the next line including '\n' if present."""
        cdef Py_ssize_t off, remain, local_end, abs_end
        cdef const unsigned char* p
        cdef const void* hit
        if self._pos >= self._len:
            return memoryview(b"")
        # Ensure we're in a valid window
        if not (self._win_base <= self._pos < self._win_base + self._win_size):
            self._map_window(self._pos)
            if self._win_size == 0:
                return memoryview(b"")
        while True:
            off = self._pos - self._win_base
            remain = self._win_size - off
            if remain <= 0:
                # Remap starting at current absolute position
                self._map_window(self._pos)
                if self._win_size == 0:
                    return memoryview(b"")
                continue
            p = self._data + off
            hit = memchr(<const void*>p, 0x0A, <size_t>remain)
            if hit != NULL:
                local_end = <Py_ssize_t>((<const unsigned char*>hit) - self._data) + 1
                abs_end = self._win_base + local_end
                mv = memoryview(self._mm)[off:local_end]
                self._pos = abs_end
                return mv
            # No newline in this window: consume to window end and continue
            self._pos = self._win_base + self._win_size
            # Slide window to new position
            self._map_window(self._pos)
            if self._win_size == 0:
                return memoryview(b"")

    cdef str _decode(self, b: bytes):
        try:
            return self.decode_callback(b)
        except Exception as err:
            err.add_note(f"Issues while decoding bytes: {b=}")
            raise err

    # -------- parser primitives using mmap cursor --------
    cdef int _consume_whitespace(self):
        cdef int consumed = 0
        cdef int b
        while True:
            b = self._read_byte_int()
            if b < 0:
                break
            if not _is_space(<unsigned char>b):
                self._seek(-1, SEEK_CUR)
                break
            consumed += 1
        return consumed

    cdef bytes _consume_until(self, bytes char_end):
        cdef bytearray out = bytearray()
        cdef int b
        cdef unsigned char endb = <unsigned char>char_end[0]
        while True:
            b = self._read_byte_int()
            if b < 0:
                break
            if <unsigned char>b == endb:
                break
            out.append(<unsigned char>b)
        return bytes(out)

    cdef bint _consume_attempt(self, bytes term):
        cdef Py_ssize_t i, n = PyBytes_GET_SIZE(term)
        cdef int b
        for i in range(n):
            b = self._read_byte_int()
            if b < 0 or b != term[i]:
                self._seek(-(i + 1), SEEK_CUR)
                return False
        return True

    cdef bytes _parse_string(self, bytes encloser, bint consumed_first):
        cdef unsigned char quote = <unsigned char>encloser[0]
        cdef bytearray out = bytearray()
        cdef int b, nxt
        if not consumed_first:
            assert self._consume_attempt(encloser)
        while True:
            b = self._read_byte_int()
            if b < 0:
                raise ValueError("unterminated string")
            if <unsigned char>b == quote:
                break
            if <unsigned char>b != 0x5C:  # '\\'
                out.append(<unsigned char>b)
                continue
            nxt = self._read_byte_int()
            if nxt < 0:
                raise ValueError("dangling backslash")
            if nxt == 0x5C: out.append(0x5C)
            elif nxt == 0x27: out.append(0x27)
            elif nxt == 0x22: out.append(0x22)
            elif nxt == 0x6E: out.append(0x0A)
            elif nxt == 0x72: out.append(0x0D)
            elif nxt == 0x74: out.append(0x09)
            elif nxt == 0x62: out.append(0x08)
            elif nxt == 0x30: out.append(0x00)
            elif nxt == 0x5A: out.append(0x1A)
            elif <unsigned char>nxt == quote:
                out.append(quote)
            else:
                out.append(<unsigned char>nxt)
        return bytes(out)

    cdef object _parse_int(self):
        cdef long value = 0
        cdef bint neg = self._consume_attempt(b"-")
        cdef int b
        cdef bint have_digit = False
        while True:
            b = self._peek_byte_int()
            if b < 0 or not _is_digit(<unsigned char>b):
                break
            self._read_byte_int()
            value = value * 10 + (b - 48)
            have_digit = True
        if not have_digit:
            raise ValueError("expected integer")
        if neg:
            value = -value
        return value

    cdef list _parse_array(self, bint is_fields):
        cdef list result = []
        cdef int b
        cdef object value
        cdef bint should_break
        while True:
            self._consume_whitespace()
            b = self._read_byte_int()  # lookahead then push back
            if b < 0:
                raise ValueError("unexpected EOF in array")
            self._seek(-1, SEEK_CUR)
            if is_fields is True and b == 0x60:  # '`'
                value = self._parse_string(b"`", consumed_first=False).decode()
            elif is_fields is False and b == 0x27:  # "'"
                value = self._decode(self._parse_string(b"'", consumed_first=False))
            elif is_fields is False and (_is_digit(<unsigned char>b) or b == 0x2D):
                value = self._parse_int()
            elif is_fields is False and b == 0x4E:  # 'N'
                assert self._consume_attempt(b"NULL")
                value = None
            elif b == 0x29:  # ')'
                break
            else:
                raise AssertionError(f"char=0x{b:02x}, is_fields={is_fields}")
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
        self._seek(start_pos, 0)
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
        self._seek(start_pos, 0)
        assert self._consume_attempt(b"CREATE")
        assert self._consume_whitespace() > 0
        assert self._consume_attempt(b"TABLE")
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
            skipped_bytes = self._consume_safe_until(term=b",", is_field_def=is_field_def)
            if skipped_bytes.endswith(b")"):
                break
            assert PyBytes_GET_SIZE(skipped_bytes) > 0
        self.tables[raw_table.decode()] = tuple(map(bytes.decode, fields))

    cdef bytes _consume_safe_until(self, bytes term, bint is_field_def):
        cdef bytearray out = bytearray()
        cdef int b
        cdef unsigned char t = <unsigned char>term[0]
        while True:
            b = self._read_byte_int()
            if b < 0:
                break
            out.append(<unsigned char>b)
            if b == 0x28:  # '('
                self._parse_array(is_fields=is_field_def is False)
            elif b == 0x27 or b == 0x60:  # '\'' or '`'
                self._parse_string(encloser=bytes([b]), consumed_first=True)
            elif b == t or b == 0x29:  # term or ')'
                break
        return bytes(out)

    def _parse(self):
        cdef object start_pos
        cdef object line_view
        cdef Py_ssize_t here
        cdef Py_ssize_t off2, local_end2
        try:
            while True:
                start_pos = self._tell()
                line_view = self._readline_view()
                line_view = None  # Drop memoryview reference before any parsing that may remap window
                if self._pos == start_pos:
                    break
                if self._starts_with_at(<Py_ssize_t>start_pos, b"INSERT "):
                    self._seek(start_pos, 0)
                    yield from self._parse_insert_into(start_pos=start_pos)
                elif self._starts_with_at(<Py_ssize_t>start_pos, b"CREATE "):
                    self._seek(start_pos, 0)
                    self._parse_create_table(start_pos=start_pos)
        except Exception as err:
            here = <Py_ssize_t>self._tell()
            # Show a preview from the current window if available
            if not (self._win_base <= here < self._win_base + self._win_size):
                self._map_window(here)
            off2 = here - self._win_base if self._win_size else 0
            local_end2 = off2 + 0x20
            if self._win_size and local_end2 > self._win_size:
                local_end2 = self._win_size
            nxt = bytes(memoryview(self._mm)[off2:local_end2]) if self._win_size else b''
            err.add_note(f"File position: {here}, next 20 chars: {repr(nxt)}")
            raise err

    def parse(self):
        import mmap
        import fcntl
        st = os.stat(self.path)
        self._len = st.st_size
        if self._len == 0:
            return
        # Prefer ALLOCATIONGRANULARITY when available; fall back to 4096
        try:
            self._page_size = mmap.ALLOCATIONGRANULARITY
        except AttributeError:
            self._page_size = mmap.PAGESIZE if hasattr(mmap, 'PAGESIZE') else 4096
        # Choose window size as max(chunk, one page). Caller can tweak _chunk_size.
        if self._chunk_size < self._page_size:
            self._chunk_size = self._page_size
        # Open FD and keep for sliding remaps
        with open(self.path, 'rb', buffering=0) as f:
            self._fd = f.fileno()
            # Map initial window at position 0
            self._pos = 0
            self._map_window(0)
            try:
                for item in self._parse():
                    yield item
            finally:
                # Release current window buffers and reset state
                if self._view.buf != NULL:
                    PyBuffer_Release(&self._view)
                    self._view.buf = NULL
                    self._view.len = 0
                if self._mm is not None:
                    self._mm.close()
                    self._mm = None
                self._data = NULL
                self._win_size = 0
                self._win_base = 0
                self._fd = -1
                self._len = 0
                self._pos = 0