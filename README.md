# cysqld
A simple and lightweight cython parser for sql dumps. Features:
- [x] Is in streaming mode, doesn't load the sql file at once to parse it (use of `io.BufferedReader`). Allows to process files from any sizes.
- [x] Yields the data from all the tables.
- [x] Automatic detection of tables key to yield data as dict.

If you have any issues, plz open issue with the traceback and (if possible) the file, to reproduce.

Install:
```
pip install git+https://github.com/novitae/cysqld
```
Usage:
```py
from cysqld import Parser

PATH = "blabla.sql"

for (table_name, item) in Parser(PATH).parse():
    # `item` is the dict of values for each lines
    # `table_name` is the name of database at `INSERT INTO \`table_name\` ...`

# In case of custom encoding, you will have to use the `decode_callback`
# argument in `Parser()`, as shown below:
def decode_callback(b: bytes):
    return b \
        .decode("utf-8", errors="surrogatepass") \
        .encode("utf-16", "surrogatepass") \
        .decode("utf-16", "replace")

for _ in Parser(PATH, decode_callback=decode_callback).parse():
    pass
```
Downsides (TODO):
- Doesn't support floats value (i don't even know if it's a thing in sql).
