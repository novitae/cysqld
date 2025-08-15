# cysqld
A simple and lightweight cython parser for sql dumps. Features:
- [x] Is in streaming mode, doesn't load the sql file at once to parse it (use of `io.BufferedReader`).
- [x] Yields the data from all the tables.
- [x] Automatic detection of tables key to yield data as dict.

If you have any issues, plz open issue with the traceback and (if possible) the file, to reproduce.

![](https://en.meming.world/images/en/b/be/But_It%27s_Honest_Work.jpg)

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
```
Downsides (TODO):
- Doesn't support other string encoding than utf8.
- Doesn't support floats value (i don't even know if it's a thing in sql).
