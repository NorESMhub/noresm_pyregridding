import numpy as np
import xarray as xr
import math
import xesmf

def make_generic_regridder(weightfile, filename_exmp):
    exmp_dataset = xr.open_dataset(filename_exmp)
    if "lon" in exmp_dataset.dims and "lat" in exmp_dataset.dims:
        return None
    else:
        return make_se_regridder(weight_file=weightfile)
   
def make_se_regridder(weight_file, regrid_method="conserved"):
    weights = xr.open_dataset(weight_file)
    in_shape = weights.src_grid_dims.load().data

    # Since xESMF expects 2D vars, we'll insert a dummy dimension of size-1
    if len(in_shape) == 1:
        in_shape = [1, in_shape.item()]

    # output variable shape
    out_shape = weights.dst_grid_dims.load().data.tolist()[::-1]

    # print(in_shape, out_shape)

    # Some prep to get the bounds:
    # Note that bounds are needed for conservative regridding and not for bilinear
    lat_b_out = np.zeros(out_shape[0]+1)
    lon_b_out = weights.xv_b.data[:out_shape[1]+1, 0]
    lat_b_out[:-1] = weights.yv_b.data[np.arange(out_shape[0])*out_shape[1],0]
    lat_b_out[-1] = weights.yv_b.data[-1,-1]

    dummy_in = xr.Dataset(
        {
            "lat": ("lat", np.empty((in_shape[0],))),
            "lon": ("lon", np.empty((in_shape[1],))),
            "lat_b": ("lat_b", np.empty((in_shape[0] + 1,))),
            "lon_b": ("lon_b", np.empty((in_shape[1] + 1,))),
        }
    )
    dummy_out = xr.Dataset(
        {
            "lat": ("lat", weights.yc_b.data.reshape(out_shape)[:, 0]),
            "lon": ("lon", weights.xc_b.data.reshape(out_shape)[0, :]),
            "lat_b": ("lat_b", lat_b_out),
            "lon_b": ("lon_b", lon_b_out),
        }
    )

    regridder = xesmf.Regridder(
        dummy_in,
        dummy_out,
        weights=weight_file,
        method=regrid_method,#"conservative_normed",
        #method="bilinear",
        reuse_weights=True,
        periodic=True,
    )
    return regridder

def regrid_se_data(regridder, data_to_regrid, dimname, debug):
    if regridder is None:
        print (f"No data to regrid, returning")
        return data_to_regrid
    data_copy = data_to_regrid.copy()
    vars_with_ncol = [name for name in list(data_to_regrid.data_vars.keys()) if dimname in data_to_regrid[name].dims]
    for var in vars_with_ncol:
        if "FATES_DAYSINCE_DROUGHTLEAFON_PF" not in var and "FATES_DAYSINCE_DROUGHTLEAFOFF_PF" not in var:
            if debug:
                print (f"var is {var}")
            data_copy[var] = data_copy[var].transpose(..., dimname).expand_dims("dummy", axis=-2)
    regridded = regridder(data_copy.rename({"dummy": "lat", dimname: "lon"}))
    return regridded

def make_regular_grid_regridder(regrid_start, regrid_target, method= "bilinear"):
    # print(regrid_start)
    lat_min = np.argmin(np.abs((regrid_target["lat"].values - regrid_start["lat"].values.min())))
    lat_max = np.argmin(np.abs(regrid_target["lat"].values - regrid_start["lat"].values.max()))
    regrid_target = regrid_target.isel(lat=slice(lat_min, lat_max))
    # print(f"lat_min {lat_min}, lat_max: {lat_max}")# lon_min: {lon_min}, lon_max: {lon_max}")

    # print(regrid_target)
    return xesmf.Regridder(
        regrid_start,
        regrid_target,
        method = method,
        periodic = True,
        #reuse_weights=True
    )

def make_regridder_regular_to_coarsest_resolution(regrid_target1, regrid_target2):
    if (regrid_target2.lat.shape[0] == regrid_target1.lat.shape[0]) and (regrid_target2.lon.shape[0] == regrid_target1.lon.shape[0]):
        return None, False
    if regrid_target1.lat.shape[0] > regrid_target2.lat.shape[0]:
        regridder_here = make_regular_grid_regridder(regrid_target1, regrid_target2)
        return regridder_here, True
    regridder_here = make_regular_grid_regridder(regrid_target2, regrid_target1)
    return regridder_here, False


