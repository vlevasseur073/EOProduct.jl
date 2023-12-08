module EOProduct

export eoproduct_dataset

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
        ds = Dataset()
        zgroup = zopen(p,consolidated=true)
        for zarray in zgroup.arrays
            if haskey(zarray.second.attrs,"_FillValue")
                zarray.second.attrs["missing_value"] = zarray.second.attrs["_FillValue"]
                # zarray.second.attrs=delete!(zarray.second.attrs,"_FillValue")
            end
        end
        try
            ds = open_dataset(zgroup)
        catch e
            @warn e
            @warn "Problem encountered for $p"
            continue
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

function image_to_instrument(image::YAXArray,indices::YAXArray...)
    
    scan=indices[1] .+1 .- minimum(skipmissing(indices[1]))
    pixel=indices[2] .+1
    detector=indices[3] .+1
    s_size = maximum(skipmissing(scan))+1
    p_size = maximum(skipmissing(pixel))+1
    d_size = maximum(skipmissing(detector))+1

    instr = Array{eltype(image)}(undef,s_size,p_size,d_size)
    for i in image.rows_in
        for j in image.columns_in
            s = scan[j,i]
            p = pixel[j,i]
            d = detector[j,i]
            if !ismissing(s) && !ismissing(p) && !ismissing(d)
                instr[s,p,d] = image[j,i]
            end
        end
    end
end

end # module EOProduct
