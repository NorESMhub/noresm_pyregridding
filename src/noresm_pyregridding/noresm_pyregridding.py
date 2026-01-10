import numpy as np
import xarray as xr
import math
import xesmf

def make_regridder_regular_to_coarsest_resolution(regrid_target1, regrid_target2):
    if (regrid_target2.lat.shape[0] == regrid_target1.lat.shape[0]) and (regrid_target2.lon.shape[0] == regrid_target1.lon.shape[0]):
        return None, False
    if regrid_target1.lat.shape[0] > regrid_target2.lat.shape[0]:
        regridder_here = make_regular_grid_regridder(regrid_target1, regrid_target2)
        return regridder_here, True
    regridder_here = make_regular_grid_regridder(regrid_target2, regrid_target1)
    return regridder_here, False


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
        method=regrid_method,
        reuse_weights=True,
        periodic=True,
    )
    return regridder


def regrid_se_data(regridder: xesmf.Regridder, ds_in: xr.Dataset, dimname: str, debug: bool) -> xr.Dataset:
    if regridder is None:
        print (f"No data to regrid, returning")
        return ds_in

    # make a copy of input dataset
    ds_in_copy = ds_in.copy()

    # determine variables that will be regridded
    #vars_with_ncol = [name for name in list(ds_in.data_vars.keys()) if dimname in ds_in[name].dims]
    vars_with_ncol = [name for name in ds_in.data_vars if dimname in ds_in[name].dims]

    # For land variables - need to multiple variables by landfrac before regridding and then
    # divide by the mapped landfrac after regridding
    if dimname == 'lndgrid':
        # remove variables from those to be regridded
        exclude_regridding_vars = ["FATES_DAYSINCE_DROUGHTLEAFON_PF","FATES_DAYSINCE_DROUGHTLEAFOFF_PF"]
        for var in exclude_regridding_vars:
            if var in vars_with_ncol:
                vars_with_ncol.remove(var) 
                print (f"removed var {var} from list to regrid")
            
        # determine list of variables that will not be normalized
        exclude_normalization_vars = ["landfrac","landmask"]

        # normalize input field by landfrac
        landfrac = ds_in["landfrac"].fillna(0)
        for var in vars_with_ncol:
            if debug:
                print (f"var is {var}")
            ds_in_copy[var] = ds_in_copy[var].transpose(..., dimname).expand_dims("dummy", axis=-2)
            if var not in exclude_normalization_vars:
                print (f"var is {var}")
                ds_in_copy[var] = ds_in_copy[var] * ds_in_copy["landfrac"]

        # regrid data
        ds_out = regridder(ds_in_copy.rename({"dummy": "lat", dimname: "lon"}))

        # normalize the mapped land data by dividing by the mapped land fraction
        for var in vars_with_ncol:
            if var not in exclude_normalization_vars:
                ds_out[var] = ds_out[var] / ds_out["landfrac"]

    else: # for atm
        for var in vars_with_ncol:
            if debug:
                print (f"var is {var}")
            ds_in_copy[var] = ds_in_copy[var].transpose(..., dimname).expand_dims("dummy", axis=-2)

        # regrid the field
        ds_out = regridder(ds_in_copy.rename({"dummy": "lat", dimname: "lon"}))

    return ds_out


