using SQLiteCompress
using Test
using Tables
import SQLite, SQLCipher
using DBInterface: execute
using CodecZstd, CodecZlib, CodecBzip2, CodecLz4, CodecXz


@testset "zstd detailed" begin
    db = SQLite.DB(":memory:")

    @test_throws SQLite.SQLiteException execute(db, """select decompress(compress("hello")) as x""")
    @test_throws ArgumentError register_compression!(db, (ZstdDecompressor, ZstdCompressor))
    @test_throws ArgumentError register_compression!(db, (ZstdCompressor, ZstdCompressor))
    @test_throws ArgumentError register_compression!(db, (ZstdCompressor, ZlibDecompressor))
    @test_throws SQLite.SQLiteException execute(db, """select decompress(compress("hello")) as x""")

    register_compression!(db, (ZstdCompressor, ZstdDecompressor))
    @test only(rowtable(execute(db, """select decompress(compress("hello")) as x"""))).x == "hello"
    @test only(rowtable(execute(db, """select compress("hello") as x"""))).x == transcode(ZstdCompressor, "hello")
    @test only(rowtable(execute(db, """select decompress(compress(decompress(compress("hello")))) == decompress(compress("hello")) as x"""))).x == 1
    
    execute(db, "create table tbl ( x text not null check (typeof(x) = 'text'), y blob not null check (typeof(y) = 'blob') )")
    execute(db, "insert into tbl (x, y) values (?, ?)", ("hello", transcode(ZstdCompressor, "hello")))
    longstrs = [join(rand('A':'Z', 1000)) for _ in 1:2]
    execute(db, "insert into tbl (x, y) values (?, ?)", (longstrs[1], transcode(ZstdCompressor, longstrs[1])))
    execute(db, "insert into tbl (x, y) values (?, ?)", (longstrs[2], transcode(ZstdCompressor, longstrs[2])))
    
    res = rowtable(execute(db, "select x, y, decompress(y) as dy, compress(x) as cx, typeof(decompress(y)) as tdy from tbl"))
    @test all([r.tdy == "text" for r in res])
    @test all([r.x == r.dy for r in res])
    @test all([r.cx == r.y for r in res])
    @test all(r.ok == 1 for r in rowtable(execute(db, "select y == compress(x) as ok from tbl")))
    @test all(r.ok == 1 for r in rowtable(execute(db, "select quote(decompress(y)) == quote(x) as ok from tbl")))

    # SQLite.jl seems to return a nul-terminated string from UDF
    # it doesn't compare equal in SQL, can be checked with hex()
    @test_broken only(rowtable(execute(db, """select decompress(compress("hello")) == "hello" as x"""))).x == 1
    @test_broken all(r.ok == 1 for r in rowtable(execute(db, "select decompress(y) == x as ok from tbl")))
end


@testset "all simple" begin
    @testset for cdecomp in [
            (ZstdCompressor, ZstdDecompressor),
            (ZlibCompressor, ZlibDecompressor),
            (GzipCompressor, GzipDecompressor),
            (DeflateCompressor, DeflateDecompressor),
            (Bzip2Compressor, Bzip2Decompressor),
            (LZ4FastCompressor, LZ4SafeDecompressor),
            (LZ4HCCompressor, LZ4SafeDecompressor),
            (XzCompressor, XzDecompressor),
        ]
        db = SQLite.DB(":memory:")
        @test_throws SQLite.SQLiteException execute(db, """select decompress(compress("hello")) as x""")
        @test_throws ArgumentError register_compression!(db, reverse(cdecomp))
        register_compression!(db, cdecomp)
        @test only(rowtable(execute(db, """select decompress(compress("hello")) as x"""))).x == "hello"
    end
end


@testset "sqlcipher" begin
    @testset for cdecomp in [
            (ZstdCompressor, ZstdDecompressor),
            (ZlibCompressor, ZlibDecompressor),
            (GzipCompressor, GzipDecompressor),
            (DeflateCompressor, DeflateDecompressor),
            (Bzip2Compressor, Bzip2Decompressor),
            (LZ4FastCompressor, LZ4SafeDecompressor),
            (LZ4HCCompressor, LZ4SafeDecompressor),
            (XzCompressor, XzDecompressor),
        ]
        db = SQLCipher.DB(":memory:")
        @test_throws SQLCipher.SQLiteException execute(db, """select decompress(compress("hello")) as x""")
        @test_throws ArgumentError register_compression!(db, reverse(cdecomp))
        register_compression!(db, cdecomp)
        @test only(rowtable(execute(db, """select decompress(compress("hello")) as x"""))).x == "hello"
    end
end


import CompatHelperLocal as CHL
CHL.@check()
