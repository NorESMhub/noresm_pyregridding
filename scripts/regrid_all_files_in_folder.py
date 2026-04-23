#!/usr/bin/env python3

"""
script: regrid_all_files_in_folder
regrids all cam or clm output files from spectral element grid to output lat/lon grid
"""

#++++++++++++++++++++++++++++++
# Import python modules
#++++++++++++++++++++++++++++++

import os
import logging
import sys
import glob
import argparse
import xarray as xr
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# Determine local directory path:
_LOCAL_PATH = os.path.dirname(os.path.abspath(__file__))

# Append path to regridding utilities
sys.path.append(os.path.join(_LOCAL_PATH, "../", "src"))

# Now import regridding utilities
from noresm_pyregridding import noresm_pyregridding

#++++++++++++++++++++++++++++++
# Input argument parser function
#++++++++++++++++++++++++++++++

def parse_arguments():
    """
    Parses command-line input arguments using the argparse
    python module and outputs the final argument object.
    """

    parser = argparse.ArgumentParser(description='Command-line wrapper to regridder from SE to lat/lon grid')

    parser.add_argument('--debug', action='store_true',
                        help="Turn on debug output (False by default).")

    parser.add_argument("--realm",
                        choices=["atm", "lnd"],
                        help="Realm to process (required)",
                        required=True)

    parser.add_argument('--inputdir', type=str,
                        help="Full pathname of directory containing input spectral element data files (required)",
                        required=True)

    parser.add_argument('--outputdir', type=str,
                        help="Full path to directory where output regridded data will be placed (required)",
                        required=True)

    parser.add_argument('--inputres', type=str,
                        choices=["ne16", "ne30"],
                        help="input_grid name (required)",
                        required=True)

    parser.add_argument("--workers",
                        type=int,
                        default=1,
                        help="Number of parallel workers (default: 1, set to >1 for parallel execution)")

    return parser.parse_args()


#++++++++++++++++++++++++++++++
# Per-file regridding function
#++++++++++++++++++++++++++++++

def regrid_file(filepath, outputdir, weight_file, realm, debug):
    """
    Regrids a single file and writes the output.
    Designed to be called from a worker process.

    Returns a tuple of (filepath, success, message).
    """
    logger = logging.getLogger("noresm_pyregridding")

    filename = Path(filepath).name
    output_file = outputdir / filename.replace(".nc", "_regridded.nc")

    # Skip if already regridded
    if output_file.exists():
        return filepath, True, f"Output file {output_file.name} already exists - skipping"

    try:
        # Each worker creates its own regridder (weight file read is cheap)
        regridder = noresm_pyregridding.make_se_regridder(weight_file=weight_file)

        data_in = xr.open_dataset(filepath)

        if "ncol" not in data_in.dims and "lndgrid" not in data_in.dims:
            return filepath, False, f"Neither ncol nor lndgrid found in {filename}"

        if realm == "atm":
            data_regridded = noresm_pyregridding.regrid_cam_se_data(regridder, data_in, debug)
        elif realm == "lnd":
            data_regridded = noresm_pyregridding.regrid_ctsm_se_data(regridder, data_in, debug)

        data_regridded.to_netcdf(output_file)
        return filepath, True, f"Successfully regridded and wrote {output_file.name}"

    except Exception as e:
        return filepath, False, f"Failed to regrid {filename}: {e}"


#++++++++++++++++++++++++++++++
# Main regridding script
#++++++++++++++++++++++++++++++

def main():

    args = parse_arguments()
    debug = args.debug

    # Set up logging
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logger = logging.getLogger("noresm_pyregridding")

    # Validate input directory
    inputdir = Path(args.inputdir)
    if not inputdir.exists():
        raise ValueError(f"inputdir {inputdir} does not exist")

    # Create output directory if needed
    outputdir = Path(args.outputdir)
    if not outputdir.exists():
        try:
            outputdir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise ValueError(f"Could not create output directory {outputdir}: {e}")

    # Determine weights file
    if args.inputres == "ne16":
        weight_file = "/datalake/NS9560K/diagnostics/land_xesmf_diag_data/map_ne16pg3_to_1.9x2.5_nomask_scripgrids_c250425.nc"
    elif args.inputres == "ne30":
        weight_file = "/datalake/NS9560K/diagnostics/land_xesmf_diag_data/map_ne30pg3_to_0.5x0.5_nomask_aave_da_c180515.nc"
    else:
        raise ValueError("Only input grids of ne16 and ne30 are currently supported")

    # Find files to regrid
    filelist = sorted(f for f in glob.glob(str(inputdir / "*.nc")) if ".cam.i." not in Path(f).name)
    if not filelist:
        logger.error(f"No netcdf files found in {inputdir}")
        return

    logger.info(f"Found {len(filelist)} netcdf file(s) to process")
    logger.info(f"Using {args.workers} worker(s)")

    # --- Sequential execution ---
    if args.workers == 1:
        for filepath in filelist:
            _, success, message = regrid_file(filepath, outputdir, weight_file, args.realm, debug)
            if success:
                logger.info(message)
            else:
                logger.error(message)

    # --- Parallel execution ---
    else:
        n_success = 0
        n_failed = 0
        n_skipped = 0

        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(regrid_file, fp, outputdir, weight_file, args.realm, debug): fp
                for fp in filelist
            }
            for future in as_completed(futures):
                filepath, success, message = future.result()
                if success:
                    if "skipping" in message:
                        n_skipped += 1
                        logger.info(message)
                    else:
                        n_success += 1
                        logger.info(message)
                else:
                    n_failed += 1
                    logger.error(message)

        logger.info(
            f"Finished: {n_success} regridded, {n_skipped} skipped, {n_failed} failed "
            f"(out of {len(filelist)} total)"
        )
        if n_failed > 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
