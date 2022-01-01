#!/usr/bin/env julia
import Pkg
Pkg.UPDATED_REGISTRY_THIS_SESSION[] = true
Pkg.activate(temp=true)
Pkg.add("Pluto")
import Pluto

cd(@__DIR__)

files = [
    "notebook.jl" => "notebook.html"
]

s = Pluto.ServerSession()
for (nbfile, htmlfile) in files
    nb = Pluto.SessionActions.open(s, nbfile; run_async=false)
    html_contents = Pluto.generate_html(nb)
    write(htmlfile, html_contents)
    Pluto.SessionActions.shutdown(s, nb)
end
