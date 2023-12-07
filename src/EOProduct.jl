module EOProduct

using YAXArrays, Zarr

function iter_groups!(vars::Dict{String,ZArray},z::ZGroup)
    if isempty(z.groups)
        println(z)
        for var in z.arrays
            vars[var.first]=var.second
        end
    end
    for g in z.groups
        if isa(g.second,ZGroup)
            iter_groups!(vars,g.second)
        end
    end

end
"""
    open_eoproduct(path::String)

Open a Copernicus Zarr product
returns a Dict{String,ZArray} containing all the Variables stored in the product


# Examples
```julia-repl
julia> d = open_eoproduct("S3SLSLST_20191227T124111_0179_A109_T921.zarr")
```
"""
function open_eoproduct(path::String)
    z = zopen(path,consolidated=true)
    vars = Dict{String,ZArray}()
    iter_groups!(vars,z)
    return vars
end

"""
    eoproduct_dataset(path::String)

Open a Copernicus Zarr product
returns a Dict{String,Dataset} containing all the Variables stored in the product


# Examples
```julia-repl
julia> d = open_eoproduct("S3SLSLST_20191227T124111_0179_A109_T921.zarr")
```
"""
function eoproduct_dataset(path::String)
    # Get leaf groups of the product
    variables = [ d[1] for d in walkdir(path) if isempty(d[2]) ]
    leaf_groups = unique(dirname.(variables))

    eo_product = Dict{String,Dataset}()
    for p in leaf_groups
        try
            ds = open_dataset(zopen(p,consolidated=true))
        catch e
            @warn e
            @warn "Problem encountered for $p"
        end
        key = basename(p)
        key = replace(p,path=>"")
        if key[1] == '/'
            key=key[2:end]
        end
        eo_product[key] = ds
    end
    return eo_product
end

end # module EOProduct
