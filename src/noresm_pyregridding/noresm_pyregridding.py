import numpy as np
import xarray as xr
import math
import xesmf


def _restore_passthrough_and_attrs(
    ds_out: xr.Dataset, ds_in: xr.Dataset, horiz_dim: str
) -> xr.Dataset:
    """Restore variables, coords and attrs that xESMF drops during regridding.

    xESMF's ``Regridder.__call__`` only emits the variables that have the
    horizontal regridding dims; everything else (``time_bnds``, ``hyam``,
    ``hybm``, ``hyai``, ``hybi``, ``P0``, ``lev``, ``ilev``, ``gw``, scalar
    reference values, etc.) is silently dropped from the output dataset.
    In older xESMF versions the per-variable ``attrs`` and the global
    dataset ``attrs`` are also lost.

    This helper copies them back from the input dataset:

    * for every variable in ``ds_in`` that is **not** present in ``ds_out``
      and does not depend on ``horiz_dim``, the variable is added back
      verbatim (with its attrs and encoding);
    * for every variable that is already in ``ds_out`` (i.e., was
      regridded), per-variable ``attrs`` and ``encoding`` are restored
      from ``ds_in`` if the regridder produced an empty set;
    * the dataset-level ``attrs`` are copied back from ``ds_in``.
    """
    # Restore global (dataset-level) attrs
    ds_out.attrs = dict(ds_in.attrs)

    # Restore per-variable attrs/encoding on regridded variables
    for name in ds_out.variables:
        if name in ds_in.variables:
            if not ds_out[name].attrs:
                ds_out[name].attrs = dict(ds_in[name].attrs)
            if not ds_out[name].encoding:
                ds_out[name].encoding = dict(ds_in[name].encoding)

    # Add back any pass-through variables that don't carry the horiz dim
    for name in ds_in.variables:
        if name in ds_out.variables:
            continue
        if horiz_dim in ds_in[name].dims:
            # had the horiz dim but didn't survive regridding — skip
            continue
        ds_out[name] = ds_in[name]

    return ds_out


def make_se_regridder(weight_file, regrid_method="conserved"):
    weights = xr.open_dataset(weight_file)
    in_shape = weights.src_grid_dims.load().data

    # Since xESMF expects 2D vars, we'll insert a dummy dimension of size-1
    if len(in_shape) == 1:
        in_shape = [1, in_shape.item()]

    # output variable shape
    out_shape = weights.dst_grid_dims.load().data.tolist()[::-1]

    # Some prep to get the bounds:
    # Note that bounds are needed for conservative regridding and not for bilinear
    lat_b_out = np.zeros(out_shape[0] + 1)
    lon_b_out = weights.xv_b.data[: out_shape[1] + 1, 0]
    lat_b_out[:-1] = weights.yv_b.data[np.arange(out_shape[0]) * out_shape[1], 0]
    lat_b_out[-1] = weights.yv_b.data[-1, -1]

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


def regrid_ctsm_se_data(
    regridder: xesmf.Regridder, ds_in: xr.Dataset, debug: bool
) -> xr.Dataset:

    if regridder is None:
        print(f"No data to regrid, returning")
        return ds_in

    dimname = "lndgrid"

    # make a copy of input dataset
    ds_in_copy = ds_in.copy()

    # determine variables that will be regridded
    vars_to_regrid = [name for name in ds_in.data_vars if dimname in ds_in[name].dims]

    # For land variables - need to multiple variables by landfrac before regridding and then
    # divide by the mapped landfrac after regridding

    # remove variables from those to be regridded
    exclude_regridding_vars = [
        "FATES_DAYSINCE_DROUGHTLEAFON_PF",
        "FATES_DAYSINCE_DROUGHTLEAFOFF_PF",
    ]
    for var in exclude_regridding_vars:
        if var in vars_to_regrid:
            vars_to_regrid.remove(var)
            print(f"removed var {var} from list to regrid")

    # determine list of variables that will not be normalized
    exclude_normalization_vars = ["landfrac", "landmask"]

    # normalize input vars by landfrac and also multiply FATES specific variable by FATES_FRACTION
    # Wrap the arithmetic in keep_attrs=True so that variable units (and other attrs)
    # survive the landfrac / FATES_FRACTION multiplication.
    landfrac = ds_in["landfrac"].fillna(0)
    with xr.set_options(keep_attrs=True):
        for var in vars_to_regrid:
            if debug:
                print(f"var is {var}")
            ds_in_copy[var] = (
                ds_in_copy[var].transpose(..., dimname).expand_dims("dummy", axis=-2)
            )
            if var not in exclude_normalization_vars:
                print(f"var is {var}")

                # multiply variable by landfrac
                ds_in_copy[var] = ds_in_copy[var] * ds_in_copy["landfrac"]

                # if variable is a FATES variable, multiply  by FATES_FRACTION
                if var.startswith("FATES") and var != "FATES_FRACTION":
                    ds_in_copy[var] = ds_in_copy[var] * ds_in_copy["FATES_FRACTION"]

        # regrid data
        ds_out = regridder(ds_in_copy.rename({"dummy": "lat", dimname: "lon"}))

        # normalize the mapped land data by dividing by the mapped land fraction
        for var in vars_to_regrid:
            if var not in exclude_normalization_vars:
                ds_out[var] = ds_out[var] / ds_out["landfrac"]

    # Restore pass-through variables (time_bnds, vertical coords, etc.) and any
    # per-variable / global attrs dropped by xESMF.  Use the original horizontal
    # dim name from ds_in (lndgrid), not the renamed one used during regridding.
    ds_out = _restore_passthrough_and_attrs(ds_out, ds_in, horiz_dim=dimname)

    # return regridded dataset
    return ds_out


def regrid_cam_se_data(
    regridder: xesmf.Regridder, ds_in: xr.Dataset, debug: bool
) -> xr.Dataset:

    if regridder is None:
        print(f"No data to regrid, returning")
        return ds_in

    dimname = "ncol"

    # make a copy of input dataset
    ds_in_copy = ds_in.copy()

    # determine variables that will be regridded
    vars_to_regrid = [name for name in ds_in.data_vars if dimname in ds_in[name].dims]

    # Wrap reshape + regrid in keep_attrs=True so variable units (and other attrs)
    # are preserved through .expand_dims/.transpose and into the regridder call.
    with xr.set_options(keep_attrs=True):
        for var in vars_to_regrid:
            if debug:
                print(f"var is {var}")
            ds_in_copy[var] = (
                ds_in_copy[var].transpose(..., dimname).expand_dims("dummy", axis=-2)
            )

        # regrid all the variables
        ds_out = regridder(ds_in_copy.rename({"dummy": "lat", dimname: "lon"}))

    # Restore pass-through variables (time_bnds, hyam/hybm/hyai/hybi/P0, lev/ilev,
    # gw, scalar reference values, etc.) and any per-variable / global attrs
    # dropped by xESMF.  Use the original horizontal dim name from ds_in (ncol).
    ds_out = _restore_passthrough_and_attrs(ds_out, ds_in, horiz_dim=dimname)

    # return regridded dataset
    return ds_out
