module SQLiteCompress

export register_compression!

using TranscodingStreams
import SQLite
using DBInterface: execute


""" `register_compression!(db::DB, (comp, decomp); do_codec_checks=true)`

Register `compress(TEXT)::BLOB` and `decompress(BLOB)::TEXT` SQL functions in the `db` SQLite database.
They apply the Julia function `TranscodingStreams.transcode` with provided compressor `comp` and decompressor `decomp`.

- `db`: SQLite database from `SQLite.jl` or compatible
- `(comp, decomp)`: `TranscodingStreams` codecs, e.g. `(ZstdCompressor, ZstdDecompressor)`
- `do_codec_checks`: whether to perform sanity checks on provided codecs
"""
function register_compression!(db, (comp, decomp); do_codec_checks=true)
    modul = first(methods(typeof(db))).module  # e.g., SQLite
    do_codec_checks && _check_comp_decomp(comp, decomp)

    modul.register(db, function compress(x::String)
        transcode(comp, x)
    end)
    modul.register(db, function decompress(x)::String
        String(transcode(decomp, x))
    end)
end


function _check_comp_decomp(comp, decomp)
    str_orig = join(repeat(rand('A':'Z', 100), inner=20))
    arr_comp = try
        transcode(comp, str_orig)
    catch e
        throw(ArgumentError("Cannot compress a string with provided compressor `$comp`. Got $e."))
    end
    str_decomp = try
        String(transcode(decomp, arr_comp))
    catch e
        throw(ArgumentError("Cannot decompress a compressed array with provided decompressor `$decomp`. Got $e."))
    end
    str_orig == str_decomp || throw(ArgumentError("Decompressed string is not equal to the original."))
    length(arr_comp) < length(str_orig) || throw(ArgumentError("Data compressed with `$comp` is not smaller than the original: $(length(arr_comp)) vs $(length(str_orig))."))

    db = SQLite.DB(":memory:")
    SQLite.@register db function compress(x::String)
        transcode(comp, x)
    end
    SQLite.@register db function decompress(x)::String
        String(transcode(decomp, x))
    end
    if first(execute(db, """select decompress(compress("hello")) as x""")).x != "hello"
        throw(ArgumentError("Provided [de]compressor doesn't seem to work with SQLite"))
    end
end

end
