# SQLiteCompress.jl

SQLite functions to compress/decompress data in the database.

They are created in just a few lines of code, thanks to `SQLite.jl` UDF interface and to `TranscodingStreams.jl`.

Basic usage:

```julia
using SQLiteCompress
import SQLite
using DBInterface: execute
using CodecZstd

db = SQLite.DB(":memory:")
register_compression!(db, (ZstdCompressor, ZstdDecompressor))
execute(db, """select decompress(compress("hello"))""")
```

Performs sanity and consistency check of the compressor-decompressor pair when registering.

Tests cover both happy paths and errors, for a range of codecs and for different `SQLite.jl`-compatible packages.
