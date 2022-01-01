### A Pluto.jl notebook ###
# v0.15.1

using Markdown
using InteractiveUtils

# ╔═╡ 98282574-4bfd-465e-abb0-161b2985c41c
begin
	using Altair
	alt.data_transformers.disable_max_rows()
	using Random
	using DataPipes
	using Utils
	using RectiGrids
	import Faker
	using SplitApplyCombine
end

# ╔═╡ ee12692a-64fc-4819-9d93-b64818c0f486
begin
	using SQLiteCompress
	using Tables
	import SQLite
	using DBInterface: execute
	using CodecZstd, CodecZlib, CodecBzip2, CodecLz4, CodecXz
end

# ╔═╡ 0e579386-ff75-4ac7-b35c-83f3986427e5
md"""
# Results
"""

# ╔═╡ 04fc6b07-4d06-4484-8332-6ee2baae4724
md"""
# Computation
"""

# ╔═╡ a047dd62-718c-4a30-b830-c62cf55e2005
codecs = [
	:Zstd => (ZstdCompressor, ZstdDecompressor),
	:Zlib => (ZlibCompressor, ZlibDecompressor),
	:Gzip => (GzipCompressor, GzipDecompressor),
	:Deflate => (DeflateCompressor, DeflateDecompressor),
	# segfault sometimes:
	:LZ4Fast => (LZ4FastCompressor, LZ4SafeDecompressor),
	:LZ4HC => (LZ4HCCompressor, LZ4SafeDecompressor),
	# much slower:
	# :Bzip2 => (Bzip2Compressor, Bzip2Decompressor),
	# :Xz => (XzCompressor, XzDecompressor),
]

# ╔═╡ c7c19b9d-17d7-4619-a727-503336d18cec
G = grid(codec=codecs, kind=[:random, :lipsum])

# ╔═╡ fc895725-d150-4b03-9253-b03de205efcb
df = mapreduce(vcat, G) do g
	db = SQLite.DB(":memory:")
	execute(db, "create table tbl ( c blob )")
	register_compression!(db, g.codec[2])
	@timed first(execute(db, """select compress("x") as c""")).c
	mapreduce(vcat, unique(round.(Int, logrange(1, 100_000, length=50)))) do len
		str = if g.kind == :random
			randstring(len)
		elseif g.kind == :lipsum
			Faker.text(number_chars=len)
		end
		comp_size = length(first(execute(db, """select compress("$str") as c""")).c)
		
		execute(db, "delete from tbl")
		N = 300
		ctimed = @timed SQLite.transaction(db) do
			for _ in 1:N
				execute(db, """insert into tbl select compress("$str")""")
			end
		end
		dtimed = @timed rowtable(execute(db, """select decompress(c) as d from tbl"""))
		res = (;
			codec=g.codec[1], g.kind,
			string_length=length(str),
			comp_size,
			ctime=ctimed.time / N,
			cbytes=ctimed.bytes / N,
			dtime=dtimed.time / N,
			dbytes=dtimed.bytes / N,
		)
		
		res
	end
end

# ╔═╡ 4ad13a18-49d0-4007-a4b7-a6c7821d0a20
base_ch = let
	ch = altChart(df |> @f(mutate(
				comp_ratio=_.comp_size / _.string_length,
				comp_time_μs=1e6*_.ctime,
				decomp_time_μs=1e6*_.dtime,
				comp_kbytes=_.cbytes / 1e3,
				decomp_kbytes=_.dbytes / 1e3,
				comp_mem_ratio=_.cbytes / _.string_length,
				decomp_mem_ratio=_.dbytes / _.string_length,
			)))
	ch = ch.encode(
		alt.X(:string_length, scale=alt.Scale(type=:log, nice=false)),
		alt.Color(:codec),
	).mark_line()
end

# ╔═╡ 733a58e2-43b9-430f-8274-a1014bb139a1
let
	ch = base_ch.encode(alt.Y(:comp_ratio, scale=alt.Scale(domain=[0, 1.3], clamp=true)), alt.Column(:kind))
	altVLSpec(ch)
end

# ╔═╡ 4c990900-57ee-4958-85b4-c91d2a44ccce
let
	ch = base_ch.encode(alt.Y(:comp_time_μs, scale=alt.Scale(type=:log, nice=false, domain=[10, 8000], clamp=true)), alt.Column(:kind))
	altVLSpec(ch)
end

# ╔═╡ da481a77-e59b-47d0-9d85-42f2d486ccb1
let
	ch = base_ch.encode(alt.Y(:decomp_time_μs, scale=alt.Scale(type=:log, nice=false, domain=[4, 700], clamp=true)), alt.Column(:kind))
	altVLSpec(ch)
end

# ╔═╡ 2f8e71d7-32de-4900-b81b-0ddb50d4289a
let
	ch = base_ch.transform_filter(alt.datum.kind == "lipsum")
	ch1 = ch.encode(alt.Y(:comp_kbytes, scale=alt.Scale(type=:log, domain=[1, 900], nice=false, clamp=true))) | ch.encode(alt.Y(:comp_mem_ratio, scale=alt.Scale(domain=[1, 20], clamp=true)))
	ch2 = ch.encode(alt.Y(:decomp_kbytes, scale=alt.Scale(type=:log, domain=[1, 900], nice=false, clamp=true))) | ch.encode(alt.Y(:decomp_mem_ratio, scale=alt.Scale(domain=[1, 20], clamp=true)))
	ch = ch1 & ch2
	altVLSpec(ch)
end

# ╔═╡ f1c58ee8-c8f7-47a1-badd-2410979d8235


# ╔═╡ 00000000-0000-0000-0000-000000000001
PLUTO_PROJECT_TOML_CONTENTS = """
[deps]
Altair = "b5d8985d-ff0a-46fa-83e6-c6893fdbcf16"
CodecBzip2 = "523fee87-0ab8-5b00-afb7-3ecf72e48cfd"
CodecLz4 = "5ba52731-8f18-5e0d-9241-30f10d1ec561"
CodecXz = "ba30903b-d9e8-5048-a5ec-d1f5b0d4b47b"
CodecZlib = "944b1d66-785c-5afd-91f1-9de20f533193"
CodecZstd = "6b39b394-51ab-5f42-8807-6242bab2b4c2"
DBInterface = "a10d1c49-ce27-4219-8d33-6db1a4562965"
DataPipes = "02685ad9-2d12-40c3-9f73-c6aeda6a7ff5"
Faker = "0efc519c-db33-5916-ab87-703215c3906f"
Random = "9a3f8284-a2c9-5f02-9a11-845980a1fd5c"
RectiGrids = "8ac6971d-971d-971d-971d-971d5ab1a71a"
SQLite = "0aa819cd-b072-5ff4-a722-6bc24af294d9"
SQLiteCompress = "ca66c970-58dd-40fd-84ba-8333ff914a26"
SplitApplyCombine = "03a91e81-4c3e-53e1-a0a4-9c0c8f19dd66"
Tables = "bd369af6-aec1-5ad0-b16a-f7cc5008161c"
Utils = "2e7d9420-a934-49d6-aef3-a4152ec1ce09"

[compat]
Altair = "~0.1.1"
CodecBzip2 = "~0.7.2"
CodecLz4 = "~0.4.0"
CodecXz = "~0.7.0"
CodecZlib = "~0.7.0"
CodecZstd = "~0.7.0"
DBInterface = "~2.4.1"
DataPipes = "~0.1.9"
Faker = "~0.3.1"
RectiGrids = "~0.1.3"
SQLite = "~1.1.4"
SQLiteCompress = "~0.1.0"
SplitApplyCombine = "~1.1.4"
Tables = "~1.4.4"
Utils = "~0.4.2"
"""

# ╔═╡ 00000000-0000-0000-0000-000000000002
PLUTO_MANIFEST_TOML_CONTENTS = """
# This file is machine-generated - editing it directly is not advised

[[AbstractFFTs]]
deps = ["LinearAlgebra"]
git-tree-sha1 = "485ee0867925449198280d4af84bdb46a2a404d0"
uuid = "621f4979-c628-5d54-868e-fcf4e3e8185c"
version = "1.0.1"

[[Adapt]]
deps = ["LinearAlgebra"]
git-tree-sha1 = "84918055d15b3114ede17ac6a7182f68870c16f7"
uuid = "79e6a3ab-5dfb-504d-930d-738a2a938a0e"
version = "3.3.1"

[[Altair]]
deps = ["JSON", "Pandas", "PyCall", "VegaLite"]
git-tree-sha1 = "46fbac54bc17f3f0a07a808451ab6814e0b49a4d"
uuid = "b5d8985d-ff0a-46fa-83e6-c6893fdbcf16"
version = "0.1.1"

[[ArgTools]]
uuid = "0dad84c5-d112-42e6-8d28-ef12dabb789f"

[[ArrayInterface]]
deps = ["IfElse", "LinearAlgebra", "Requires", "SparseArrays", "Static"]
git-tree-sha1 = "655d9e28a75f88eea3fb81a12f62da9bade89fb5"
uuid = "4fba245c-0d91-5ea0-9b3e-6abc04ee57a9"
version = "3.1.19"

[[Artifacts]]
uuid = "56f22d72-fd6d-98f1-02f0-08ddc0907c33"

[[AxisKeys]]
deps = ["AbstractFFTs", "CovarianceEstimation", "IntervalSets", "InvertedIndices", "LazyStack", "LinearAlgebra", "NamedDims", "OffsetArrays", "Statistics", "StatsBase", "Tables"]
git-tree-sha1 = "f7a17e0381cd8e627d9793433074823a2c1d7096"
uuid = "94b1ba4f-4ee9-5380-92f1-94cde586c3c5"
version = "0.1.18"

[[Base64]]
uuid = "2a0f44e3-6c83-55bd-87e4-b1978d98bd5f"

[[BinaryProvider]]
deps = ["Libdl", "Logging", "SHA"]
git-tree-sha1 = "ecdec412a9abc8db54c0efc5548c64dfce072058"
uuid = "b99e7846-7c00-51b0-8f62-c81ae34c0232"
version = "0.5.10"

[[Bzip2_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl", "Pkg"]
git-tree-sha1 = "19a35467a82e236ff51bc17a3a44b69ef35185a2"
uuid = "6e34b625-4abd-537c-b88f-471c36dfa7a0"
version = "1.0.8+0"

[[CodecBzip2]]
deps = ["Bzip2_jll", "Libdl", "TranscodingStreams"]
git-tree-sha1 = "2e62a725210ce3c3c2e1a3080190e7ca491f18d7"
uuid = "523fee87-0ab8-5b00-afb7-3ecf72e48cfd"
version = "0.7.2"

[[CodecLz4]]
deps = ["Lz4_jll", "TranscodingStreams"]
git-tree-sha1 = "59fe0cb37784288d6b9f1baebddbf75457395d40"
uuid = "5ba52731-8f18-5e0d-9241-30f10d1ec561"
version = "0.4.0"

[[CodecXz]]
deps = ["Libdl", "TranscodingStreams", "XZ_jll"]
git-tree-sha1 = "82c4c000edf64b6bda6766377e69a1028f3549ee"
uuid = "ba30903b-d9e8-5048-a5ec-d1f5b0d4b47b"
version = "0.7.0"

[[CodecZlib]]
deps = ["TranscodingStreams", "Zlib_jll"]
git-tree-sha1 = "ded953804d019afa9a3f98981d99b33e3db7b6da"
uuid = "944b1d66-785c-5afd-91f1-9de20f533193"
version = "0.7.0"

[[CodecZstd]]
deps = ["TranscodingStreams", "Zstd_jll"]
git-tree-sha1 = "d19cd9ae79ef31774151637492291d75194fc5fa"
uuid = "6b39b394-51ab-5f42-8807-6242bab2b4c2"
version = "0.7.0"

[[Compat]]
deps = ["Base64", "Dates", "DelimitedFiles", "Distributed", "InteractiveUtils", "LibGit2", "Libdl", "LinearAlgebra", "Markdown", "Mmap", "Pkg", "Printf", "REPL", "Random", "SHA", "Serialization", "SharedArrays", "Sockets", "SparseArrays", "Statistics", "Test", "UUIDs", "Unicode"]
git-tree-sha1 = "344f143fa0ec67e47917848795ab19c6a455f32c"
uuid = "34da2185-b29b-5c13-b0c7-acf172513d20"
version = "3.32.0"

[[Conda]]
deps = ["JSON", "VersionParsing"]
git-tree-sha1 = "299304989a5e6473d985212c28928899c74e9421"
uuid = "8f4d0f93-b110-5947-807f-2305c1781a2d"
version = "1.5.2"

[[ConstructionBase]]
deps = ["LinearAlgebra"]
git-tree-sha1 = "f74e9d5388b8620b4cee35d4c5a618dd4dc547f4"
uuid = "187b0558-2788-49d3-abe0-74a17ed4e7c9"
version = "1.3.0"

[[CovarianceEstimation]]
deps = ["LinearAlgebra", "Statistics", "StatsBase"]
git-tree-sha1 = "bc3930158d2be029e90b7c40d1371c4f54fa04db"
uuid = "587fd27a-f159-11e8-2dae-1979310e6154"
version = "0.2.6"

[[DBInterface]]
git-tree-sha1 = "d3e9099ef8d63b180a671a35552f93a1e0250cbb"
uuid = "a10d1c49-ce27-4219-8d33-6db1a4562965"
version = "2.4.1"

[[DataAPI]]
git-tree-sha1 = "ee400abb2298bd13bfc3df1c412ed228061a2385"
uuid = "9a962f9c-6df0-11e9-0e5d-c546b8b5ee8a"
version = "1.7.0"

[[DataPipes]]
deps = ["SplitApplyCombine"]
git-tree-sha1 = "6f88db1e9fb926ccb663ef86abe21d9687322656"
uuid = "02685ad9-2d12-40c3-9f73-c6aeda6a7ff5"
version = "0.1.9"

[[DataStructures]]
deps = ["Compat", "InteractiveUtils", "OrderedCollections"]
git-tree-sha1 = "4437b64df1e0adccc3e5d1adbc3ac741095e4677"
uuid = "864edb3b-99cc-5e75-8d2d-829cb0a9cfe8"
version = "0.18.9"

[[DataValueInterfaces]]
git-tree-sha1 = "bfc1187b79289637fa0ef6d4436ebdfe6905cbd6"
uuid = "e2d170a0-9d28-54be-80f0-106bbe20a464"
version = "1.0.0"

[[DataValues]]
deps = ["DataValueInterfaces", "Dates"]
git-tree-sha1 = "d88a19299eba280a6d062e135a43f00323ae70bf"
uuid = "e7dc6d0d-1eca-5fa6-8ad6-5aecde8b7ea5"
version = "0.4.13"

[[Dates]]
deps = ["Printf"]
uuid = "ade2ca70-3891-5945-98fb-dc099432e06a"

[[DelimitedFiles]]
deps = ["Mmap"]
uuid = "8bb1440f-4735-579b-a4ab-409b98df4dab"

[[Dictionaries]]
deps = ["Indexing", "Random"]
git-tree-sha1 = "011d10589449681ad631c0f866a2ce72771648c6"
uuid = "85a47980-9c8c-11e8-2b9f-f7ca1fa99fb4"
version = "0.3.10"

[[Distributed]]
deps = ["Random", "Serialization", "Sockets"]
uuid = "8ba89e20-285c-5b6f-9357-94700520ee1b"

[[DocStringExtensions]]
deps = ["LibGit2"]
git-tree-sha1 = "a32185f5428d3986f47c2ab78b1f216d5e6cc96f"
uuid = "ffbed154-4ef7-542d-bbb7-c09d3a79fcae"
version = "0.8.5"

[[Downloads]]
deps = ["ArgTools", "LibCURL", "NetworkOptions"]
uuid = "f43a241f-c20a-4ad4-852c-f6b1247861c6"

[[EllipsisNotation]]
deps = ["ArrayInterface"]
git-tree-sha1 = "8041575f021cba5a099a456b4163c9a08b566a02"
uuid = "da5c29d0-fa7d-589e-88eb-ea29b0a81949"
version = "1.1.0"

[[Faker]]
deps = ["Dates", "Random", "YAML"]
git-tree-sha1 = "58f48c6ce369fd3739e860041596ed0250be098e"
uuid = "0efc519c-db33-5916-ab87-703215c3906f"
version = "0.3.1"

[[FileIO]]
deps = ["Pkg", "Requires", "UUIDs"]
git-tree-sha1 = "256d8e6188f3f1ebfa1a5d17e072a0efafa8c5bf"
uuid = "5789e2e9-d7fb-5bc7-8068-2c6fae9b9549"
version = "1.10.1"

[[FilePaths]]
deps = ["FilePathsBase", "MacroTools", "Reexport", "Requires"]
git-tree-sha1 = "919d9412dbf53a2e6fe74af62a73ceed0bce0629"
uuid = "8fc22ac5-c921-52a6-82fd-178b2807b824"
version = "0.8.3"

[[FilePathsBase]]
deps = ["Dates", "Mmap", "Printf", "Test", "UUIDs"]
git-tree-sha1 = "0f5e8d0cb91a6386ba47bd1527b240bd5725fbae"
uuid = "48062228-2e41-5def-b9a4-89aafe57970f"
version = "0.9.10"

[[Future]]
deps = ["Random"]
uuid = "9fa8497b-333b-5362-9e8d-4d0656e87820"

[[HTTP]]
deps = ["Base64", "Dates", "IniFile", "Logging", "MbedTLS", "NetworkOptions", "Sockets", "URIs"]
git-tree-sha1 = "c6a1fff2fd4b1da29d3dccaffb1e1001244d844e"
uuid = "cd3eb016-35fb-5094-929b-558a96fad6f3"
version = "0.9.12"

[[IfElse]]
git-tree-sha1 = "28e837ff3e7a6c3cdb252ce49fb412c8eb3caeef"
uuid = "615f187c-cbe4-4ef1-ba3b-2fcf58d6d173"
version = "0.1.0"

[[Indexing]]
git-tree-sha1 = "ce1566720fd6b19ff3411404d4b977acd4814f9f"
uuid = "313cdc1a-70c2-5d6a-ae34-0150d3930a38"
version = "1.1.1"

[[IniFile]]
deps = ["Test"]
git-tree-sha1 = "098e4d2c533924c921f9f9847274f2ad89e018b8"
uuid = "83e8ac13-25f8-5344-8a64-a9f2b223428f"
version = "0.5.0"

[[InteractiveUtils]]
deps = ["Markdown"]
uuid = "b77e0a4c-d291-57a0-90e8-8db25a27a240"

[[IntervalSets]]
deps = ["Dates", "EllipsisNotation", "Statistics"]
git-tree-sha1 = "3cc368af3f110a767ac786560045dceddfc16758"
uuid = "8197267c-284f-5f27-9208-e0e47529a953"
version = "0.5.3"

[[InvertedIndices]]
deps = ["Test"]
git-tree-sha1 = "15732c475062348b0165684ffe28e85ea8396afc"
uuid = "41ab1584-1d38-5bbf-9106-f11c6c58b48f"
version = "1.0.0"

[[IteratorInterfaceExtensions]]
git-tree-sha1 = "a3f24677c21f5bbe9d2a714f95dcd58337fb2856"
uuid = "82899510-4779-5014-852e-03e436cf321d"
version = "1.0.0"

[[JLLWrappers]]
deps = ["Preferences"]
git-tree-sha1 = "642a199af8b68253517b80bd3bfd17eb4e84df6e"
uuid = "692b3bcd-3c85-4b1f-b108-f13ce0eb3210"
version = "1.3.0"

[[JSON]]
deps = ["Dates", "Mmap", "Parsers", "Unicode"]
git-tree-sha1 = "81690084b6198a2e1da36fcfda16eeca9f9f24e4"
uuid = "682c06a0-de6a-54ab-a142-c8b1cf79cde6"
version = "0.21.1"

[[JSONSchema]]
deps = ["HTTP", "JSON", "ZipFile"]
git-tree-sha1 = "b84ab8139afde82c7c65ba2b792fe12e01dd7307"
uuid = "7d188eb4-7ad8-530c-ae41-71a32a6d4692"
version = "0.3.3"

[[Lazy]]
deps = ["MacroTools"]
git-tree-sha1 = "1370f8202dac30758f3c345f9909b97f53d87d3f"
uuid = "50d2b5c4-7a5e-59d5-8109-a42b560f39c0"
version = "0.15.1"

[[LazyStack]]
deps = ["LinearAlgebra", "NamedDims", "OffsetArrays", "Test", "ZygoteRules"]
git-tree-sha1 = "a8bf67afad3f1ee59d367267adb7c44ccac7fdee"
uuid = "1fad7336-0346-5a1a-a56f-a06ba010965b"
version = "0.0.7"

[[LibCURL]]
deps = ["LibCURL_jll", "MozillaCACerts_jll"]
uuid = "b27032c2-a3e7-50c8-80cd-2d36dbcbfd21"

[[LibCURL_jll]]
deps = ["Artifacts", "LibSSH2_jll", "Libdl", "MbedTLS_jll", "Zlib_jll", "nghttp2_jll"]
uuid = "deac9b47-8bc7-5906-a0fe-35ac56dc84c0"

[[LibGit2]]
deps = ["Base64", "NetworkOptions", "Printf", "SHA"]
uuid = "76f85450-5226-5b5a-8eaa-529ad045b433"

[[LibSSH2_jll]]
deps = ["Artifacts", "Libdl", "MbedTLS_jll"]
uuid = "29816b5a-b9ab-546f-933c-edad1886dfa8"

[[Libdl]]
uuid = "8f399da3-3557-5675-b5ff-fb832c97cbdb"

[[Libiconv_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl", "Pkg"]
git-tree-sha1 = "42b62845d70a619f063a7da093d995ec8e15e778"
uuid = "94ce4f54-9a6c-5748-9c1c-f9c7231a4531"
version = "1.16.1+1"

[[LinearAlgebra]]
deps = ["Libdl"]
uuid = "37e2e46d-f89d-539d-b4ee-838fcccc9c8e"

[[Logging]]
uuid = "56ddb016-857b-54e1-b83d-db4d58db5568"

[[Lz4_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl", "Pkg"]
git-tree-sha1 = "5d494bc6e85c4c9b626ee0cab05daa4085486ab1"
uuid = "5ced341a-0733-55b8-9ab6-a4889d929147"
version = "1.9.3+0"

[[MacroTools]]
deps = ["Markdown", "Random"]
git-tree-sha1 = "6a8a2a625ab0dea913aba95c11370589e0239ff0"
uuid = "1914dd2f-81c6-5fcd-8719-6d5c9610ff09"
version = "0.5.6"

[[Markdown]]
deps = ["Base64"]
uuid = "d6f4376e-aef5-505a-96c1-9c027394607a"

[[Match]]
git-tree-sha1 = "5cf525d97caf86d29307150fcba763a64eaa9cbe"
uuid = "7eb4fadd-790c-5f42-8a69-bfa0b872bfbf"
version = "1.1.0"

[[MbedTLS]]
deps = ["Dates", "MbedTLS_jll", "Random", "Sockets"]
git-tree-sha1 = "1c38e51c3d08ef2278062ebceade0e46cefc96fe"
uuid = "739be429-bea8-5141-9913-cc70e7f3736d"
version = "1.0.3"

[[MbedTLS_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "c8ffd9c3-330d-5841-b78e-0817d7145fa1"

[[Missings]]
deps = ["DataAPI"]
git-tree-sha1 = "f8c673ccc215eb50fcadb285f522420e29e69e1c"
uuid = "e1d29d7a-bbdc-5cf2-9ac0-f12de2c33e28"
version = "0.4.5"

[[Mmap]]
uuid = "a63ad114-7e13-5084-954f-fe012c677804"

[[MozillaCACerts_jll]]
uuid = "14a3606d-f60d-562e-9121-12d972cd8159"

[[NamedDims]]
deps = ["AbstractFFTs", "LinearAlgebra", "Pkg", "Requires", "Statistics"]
git-tree-sha1 = "52985b34519b12fd0dcebbe34e74b2dbe6d03183"
uuid = "356022a1-0364-5f58-8944-0da4b18d706f"
version = "0.2.35"

[[NetworkOptions]]
uuid = "ca575930-c2e3-43a9-ace4-1e988b2c1908"

[[NodeJS]]
deps = ["Pkg"]
git-tree-sha1 = "905224bbdd4b555c69bb964514cfa387616f0d3a"
uuid = "2bd173c7-0d6d-553b-b6af-13a54713934c"
version = "1.3.0"

[[OffsetArrays]]
deps = ["Adapt"]
git-tree-sha1 = "4f825c6da64aebaa22cc058ecfceed1ab9af1c7e"
uuid = "6fe1bfb0-de20-5000-8ca7-80f57d26f881"
version = "1.10.3"

[[OrderedCollections]]
git-tree-sha1 = "85f8e6578bf1f9ee0d11e7bb1b1456435479d47c"
uuid = "bac558e1-5e72-5ebc-8fee-abe8a469f55d"
version = "1.4.1"

[[Pandas]]
deps = ["Compat", "DataValues", "IteratorInterfaceExtensions", "Lazy", "Pkg", "PyCall", "Statistics", "TableTraits", "TableTraitsUtils", "Test"]
git-tree-sha1 = "40b84de2260989a63792704014b99a86bb8f1425"
uuid = "eadc2687-ae89-51f9-a5d9-86b5a6373a9c"
version = "1.4.0"

[[Parsers]]
deps = ["Dates"]
git-tree-sha1 = "94bf17e83a0e4b20c8d77f6af8ffe8cc3b386c0a"
uuid = "69de0a69-1ddd-5017-9359-2bf0b02dc9f0"
version = "1.1.1"

[[Pkg]]
deps = ["Artifacts", "Dates", "Downloads", "LibGit2", "Libdl", "Logging", "Markdown", "Printf", "REPL", "Random", "SHA", "Serialization", "TOML", "Tar", "UUIDs", "p7zip_jll"]
uuid = "44cfe95a-1eb2-52ea-b672-e2afdf69b78f"

[[Preferences]]
deps = ["TOML"]
git-tree-sha1 = "00cfd92944ca9c760982747e9a1d0d5d86ab1e5a"
uuid = "21216c6a-2e73-6563-6e65-726566657250"
version = "1.2.2"

[[Printf]]
deps = ["Unicode"]
uuid = "de0858da-6303-5e67-8744-51eddeeeb8d7"

[[PyCall]]
deps = ["Conda", "Dates", "Libdl", "LinearAlgebra", "MacroTools", "Serialization", "VersionParsing"]
git-tree-sha1 = "169bb8ea6b1b143c5cf57df6d34d022a7b60c6db"
uuid = "438e738f-606a-5dbb-bf0a-cddfbfd45ab0"
version = "1.92.3"

[[REPL]]
deps = ["InteractiveUtils", "Markdown", "Sockets", "Unicode"]
uuid = "3fa0cd96-eef1-5676-8a61-b3b8758bbffb"

[[Random]]
deps = ["Serialization"]
uuid = "9a3f8284-a2c9-5f02-9a11-845980a1fd5c"

[[RectiGrids]]
deps = ["AxisKeys", "Random"]
git-tree-sha1 = "14163d8e458b9c81a91f1386adacc95a329bf53c"
uuid = "8ac6971d-971d-971d-971d-971d5ab1a71a"
version = "0.1.3"

[[Reexport]]
git-tree-sha1 = "5f6c21241f0f655da3952fd60aa18477cf96c220"
uuid = "189a3867-3050-52da-a836-e630ba90ab69"
version = "1.1.0"

[[Requires]]
deps = ["UUIDs"]
git-tree-sha1 = "4036a3bd08ac7e968e27c203d45f5fff15020621"
uuid = "ae029012-a4dd-5104-9daa-d747884805df"
version = "1.1.3"

[[SHA]]
uuid = "ea8e919c-243c-51af-8825-aaa63cd721ce"

[[SQLite]]
deps = ["BinaryProvider", "DBInterface", "Dates", "Libdl", "Random", "SQLite_jll", "Serialization", "Tables", "Test", "WeakRefStrings"]
git-tree-sha1 = "97261d38a26415048ce87f49a7a20902aa047836"
uuid = "0aa819cd-b072-5ff4-a722-6bc24af294d9"
version = "1.1.4"

[[SQLiteCompress]]
deps = ["DBInterface", "SQLite", "TranscodingStreams"]
git-tree-sha1 = "b02cff226c01c72e81cf565e19b8894e8eaa5080"
uuid = "ca66c970-58dd-40fd-84ba-8333ff914a26"
version = "0.1.0"

[[SQLite_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl", "Pkg", "Zlib_jll"]
git-tree-sha1 = "9a0e24b81e3ce02c4b2eb855476467c7b93b8a8f"
uuid = "76ed43ae-9a5d-5a62-8c75-30186b810ce8"
version = "3.36.0+0"

[[Serialization]]
uuid = "9e88b42a-f829-5b0c-bbe9-9e923198166b"

[[Setfield]]
deps = ["ConstructionBase", "Future", "MacroTools", "Requires"]
git-tree-sha1 = "d5640fc570fb1b6c54512f0bd3853866bd298b3e"
uuid = "efcf1570-3423-57d1-acb7-fd33fddbac46"
version = "0.7.0"

[[SharedArrays]]
deps = ["Distributed", "Mmap", "Random", "Serialization"]
uuid = "1a1011a3-84de-559e-8e89-a11a2f7dc383"

[[Sockets]]
uuid = "6462fe0b-24de-5631-8697-dd941f90decc"

[[SortingAlgorithms]]
deps = ["DataStructures"]
git-tree-sha1 = "b3363d7460f7d098ca0912c69b082f75625d7508"
uuid = "a2af1166-a08f-5f64-846c-94a0d3cef48c"
version = "1.0.1"

[[SparseArrays]]
deps = ["LinearAlgebra", "Random"]
uuid = "2f01184e-e22b-5df5-ae63-d93ebab69eaf"

[[SplitApplyCombine]]
deps = ["Dictionaries", "Indexing"]
git-tree-sha1 = "3cdd86a92737fa0c8af19aecb1141e71057dc2db"
uuid = "03a91e81-4c3e-53e1-a0a4-9c0c8f19dd66"
version = "1.1.4"

[[Static]]
deps = ["IfElse"]
git-tree-sha1 = "62701892d172a2fa41a1f829f66d2b0db94a9a63"
uuid = "aedffcd0-7271-4cad-89d0-dc628f76c6d3"
version = "0.3.0"

[[Statistics]]
deps = ["LinearAlgebra", "SparseArrays"]
uuid = "10745b16-79ce-11e8-11f9-7d13ad32a3b2"

[[StatsAPI]]
git-tree-sha1 = "1958272568dc176a1d881acb797beb909c785510"
uuid = "82ae8749-77ed-4fe6-ae5f-f523153014b0"
version = "1.0.0"

[[StatsBase]]
deps = ["DataAPI", "DataStructures", "LinearAlgebra", "Missings", "Printf", "Random", "SortingAlgorithms", "SparseArrays", "Statistics", "StatsAPI"]
git-tree-sha1 = "fed1ec1e65749c4d96fc20dd13bea72b55457e62"
uuid = "2913bbd2-ae8a-5f71-8c99-4fb6c76f3a91"
version = "0.33.9"

[[StringEncodings]]
deps = ["Libiconv_jll"]
git-tree-sha1 = "50ccd5ddb00d19392577902f0079267a72c5ab04"
uuid = "69024149-9ee7-55f6-a4c4-859efe599b68"
version = "0.3.5"

[[TOML]]
deps = ["Dates"]
uuid = "fa267f1f-6049-4f14-aa54-33bafae1ed76"

[[TableTraits]]
deps = ["IteratorInterfaceExtensions"]
git-tree-sha1 = "c06b2f539df1c6efa794486abfb6ed2022561a39"
uuid = "3783bdb8-4a98-5b6b-af9a-565f29a5fe9c"
version = "1.0.1"

[[TableTraitsUtils]]
deps = ["DataValues", "IteratorInterfaceExtensions", "Missings", "TableTraits"]
git-tree-sha1 = "8fc12ae66deac83e44454e61b02c37b326493233"
uuid = "382cd787-c1b6-5bf2-a167-d5b971a19bda"
version = "1.0.1"

[[Tables]]
deps = ["DataAPI", "DataValueInterfaces", "IteratorInterfaceExtensions", "LinearAlgebra", "TableTraits", "Test"]
git-tree-sha1 = "8ed4a3ea724dac32670b062be3ef1c1de6773ae8"
uuid = "bd369af6-aec1-5ad0-b16a-f7cc5008161c"
version = "1.4.4"

[[Tar]]
deps = ["ArgTools", "SHA"]
uuid = "a4e569a6-e804-4fa4-b0f3-eef7a1d5b13e"

[[Test]]
deps = ["InteractiveUtils", "Logging", "Random", "Serialization"]
uuid = "8dfed614-e22c-5e08-85e1-65c5234f0b40"

[[TranscodingStreams]]
deps = ["Random", "Test"]
git-tree-sha1 = "7c53c35547de1c5b9d46a4797cf6d8253807108c"
uuid = "3bb67fe8-82b1-5028-8e26-92a6c54297fa"
version = "0.9.5"

[[URIParser]]
deps = ["Unicode"]
git-tree-sha1 = "53a9f49546b8d2dd2e688d216421d050c9a31d0d"
uuid = "30578b45-9adc-5946-b283-645ec420af67"
version = "0.4.1"

[[URIs]]
git-tree-sha1 = "97bbe755a53fe859669cd907f2d96aee8d2c1355"
uuid = "5c2747f8-b7ea-4ff2-ba2e-563bfd36b1d4"
version = "1.3.0"

[[UUIDs]]
deps = ["Random", "SHA"]
uuid = "cf7118a7-6976-5b1a-9a39-7adc72f591a4"

[[Unicode]]
uuid = "4ec0a83e-493e-50e2-b9ac-8f72acf5a8f5"

[[Utils]]
deps = ["AxisKeys", "DocStringExtensions", "LinearAlgebra", "Match", "NamedDims", "SplitApplyCombine", "Statistics", "StatsBase"]
git-tree-sha1 = "8d2898067bb18b26d66b3e1e02ded14f6e8ebc34"
uuid = "2e7d9420-a934-49d6-aef3-a4152ec1ce09"
version = "0.4.2"

[[Vega]]
deps = ["DataStructures", "DataValues", "Dates", "FileIO", "FilePaths", "IteratorInterfaceExtensions", "JSON", "JSONSchema", "MacroTools", "NodeJS", "Pkg", "REPL", "Random", "Setfield", "TableTraits", "TableTraitsUtils", "URIParser"]
git-tree-sha1 = "43f83d3119a868874d18da6bca0f4b5b6aae53f7"
uuid = "239c3e63-733f-47ad-beb7-a12fde22c578"
version = "2.3.0"

[[VegaLite]]
deps = ["Base64", "DataStructures", "DataValues", "Dates", "FileIO", "FilePaths", "IteratorInterfaceExtensions", "JSON", "MacroTools", "NodeJS", "Pkg", "REPL", "Random", "TableTraits", "TableTraitsUtils", "URIParser", "Vega"]
git-tree-sha1 = "3e23f28af36da21bfb4acef08b144f92ad205660"
uuid = "112f6efa-9a02-5b7d-90c0-432ed331239a"
version = "2.6.0"

[[VersionParsing]]
git-tree-sha1 = "80229be1f670524750d905f8fc8148e5a8c4537f"
uuid = "81def892-9a0e-5fdd-b105-ffc91e053289"
version = "1.2.0"

[[WeakRefStrings]]
deps = ["DataAPI", "Random", "Test"]
git-tree-sha1 = "28807f85197eaad3cbd2330386fac1dcb9e7e11d"
uuid = "ea10d353-3f73-51f8-a26c-33c1cb351aa5"
version = "0.6.2"

[[XZ_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl", "Pkg"]
git-tree-sha1 = "9f76853ea2ba894054e24640abfb73d73e5a4cb5"
uuid = "ffd25f8a-64ca-5728-b0f7-c24cf3aae800"
version = "5.2.5+0"

[[YAML]]
deps = ["Base64", "Dates", "Printf", "StringEncodings"]
git-tree-sha1 = "3c6e8b9f5cdaaa21340f841653942e1a6b6561e5"
uuid = "ddb6d928-2868-570f-bddf-ab3f9cf99eb6"
version = "0.4.7"

[[ZipFile]]
deps = ["Libdl", "Printf", "Zlib_jll"]
git-tree-sha1 = "c3a5637e27e914a7a445b8d0ad063d701931e9f7"
uuid = "a5390f91-8eb1-5f08-bee0-b1d1ffed6cea"
version = "0.9.3"

[[Zlib_jll]]
deps = ["Libdl"]
uuid = "83775a58-1f1d-513f-b197-d71354ab007a"

[[Zstd_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl", "Pkg"]
git-tree-sha1 = "cc4bf3fdde8b7e3e9fa0351bdeedba1cf3b7f6e6"
uuid = "3161d3a3-bdf6-5164-811a-617609db77b4"
version = "1.5.0+0"

[[ZygoteRules]]
deps = ["MacroTools"]
git-tree-sha1 = "9e7a1e8ca60b742e508a315c17eef5211e7fbfd7"
uuid = "700de1a5-db45-46bc-99cf-38207098b444"
version = "0.2.1"

[[nghttp2_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "8e850ede-7688-5339-a07c-302acd2aaf8d"

[[p7zip_jll]]
deps = ["Artifacts", "Libdl"]
uuid = "3f19e933-33d8-53b3-aaab-bd5110c3b7a0"
"""

# ╔═╡ Cell order:
# ╟─0e579386-ff75-4ac7-b35c-83f3986427e5
# ╟─4ad13a18-49d0-4007-a4b7-a6c7821d0a20
# ╠═733a58e2-43b9-430f-8274-a1014bb139a1
# ╠═4c990900-57ee-4958-85b4-c91d2a44ccce
# ╠═da481a77-e59b-47d0-9d85-42f2d486ccb1
# ╠═2f8e71d7-32de-4900-b81b-0ddb50d4289a
# ╟─04fc6b07-4d06-4484-8332-6ee2baae4724
# ╠═a047dd62-718c-4a30-b830-c62cf55e2005
# ╠═c7c19b9d-17d7-4619-a727-503336d18cec
# ╠═fc895725-d150-4b03-9253-b03de205efcb
# ╠═f1c58ee8-c8f7-47a1-badd-2410979d8235
# ╠═98282574-4bfd-465e-abb0-161b2985c41c
# ╠═ee12692a-64fc-4819-9d93-b64818c0f486
# ╟─00000000-0000-0000-0000-000000000001
# ╟─00000000-0000-0000-0000-000000000002
