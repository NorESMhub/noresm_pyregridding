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
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

# Determine local directory path:
_LOCAL_PATH = os.path.dirname(os.path.abspath(__file__))

# Append path to regridding utilities
sys.path.append(os.path.join(_LOCAL_PATH, "../", "src"))

# Now import regridding utilities
from noresm_pyregridding import noresm_pyregridding

include_patterns = ["cam.h0a","clm2.h0a"]

#++++++++++++++++++++++++++++++
# Input argument parser function
#++++++++++++++++++++++++++++++

def parse_arguments():

    """
    Parses command-line input arguments using the argparse
    python module and outputs the final argument object.
    """

    #Create parser object:
    parser = argparse.ArgumentParser(description='Command-line wrapper to regridder from SE to lat/lon grid')

    parser.add_argument('--debug', action='store_true',
                        help="Turn on debug output (False by default).")

    parser.add_argument("--realm",
                        choices=["atm","lnd"],
                        help="Realm to process (required)",
                        required=True,)

    parser.add_argument('--inputdir', type=str,
                        help="Full pathname of directory containing input spectral element data files (required)",
                        required=True)

    parser.add_argument('--outputdir', type=str,
                        help="Full path to directory where output regridded data will be placed (required)",
                        required=True)

    parser.add_argument ('--inputres', type=str,
                         choices=["ne16","ne30"],
                         help="input_grid name (required)",
                         required=True)

    parser.add_argument("--workers",
                        type=int,
                        default=1,
                        help="Number of parallel workers (default: 1)",
                        )

    # Parse Argument inputs
    args = parser.parse_args()

    # Error checks
    return args


#++++++++++++++++++++++++++++++
# Worker initializer and per-file processing function
#++++++++++++++++++++++++++++++

# Module-level variable to hold the regridder in each worker process
_worker_regridder = None

def _init_worker(weight_file, local_path, debug):
    """
    Runs once per worker process. Creates the regridder so it is reused
    across all files handled by this worker rather than rebuilt per file.
    """
    global _worker_regridder
    sys.path.append(os.path.join(local_path, "../", "src"))
    from noresm_pyregridding import noresm_pyregridding as _nr

    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("noresm_pyregridding").info("Worker initializing regridder")
    _worker_regridder = _nr.make_se_regridder(weight_file=weight_file)


def process_file(filepath, realm, outputdir, debug):
    """Regrid a single file and write output. Runs in a worker process."""
    from noresm_pyregridding import noresm_pyregridding

    logger = logging.getLogger("noresm_pyregridding")

    filename = os.path.basename(filepath)
    output_file = filename.replace(".nc", "_regridded.nc")
    outfile_path = os.path.join(outputdir, output_file)

    if os.path.exists(outfile_path):
        logger.info(f"Output file {output_file} already exists - skipping")
        return outfile_path

    data_in = xr.open_dataset(filepath)

    if "ncol" not in data_in.dims and "lndgrid" not in data_in.dims:
        raise ValueError(f"{filepath}: neither ncol nor lndgrid found in dims")

    if realm == 'atm':
        data_regridded = noresm_pyregridding.regrid_cam_se_data(_worker_regridder, data_in, debug)
    elif realm == 'lnd':
        data_regridded = noresm_pyregridding.regrid_ctsm_se_data(_worker_regridder, data_in, debug)

    data_regridded.to_netcdf(outfile_path)
    logger.info(f"Wrote regridded file {outfile_path}")
    return outfile_path


#++++++++++++++++++++++++++++++
# main regridding script
#++++++++++++++++++++++++++++++

def main():

    # Parse command-line arguments
    args = parse_arguments()

    # Set up logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logger = logging.getLogger("noresm_pyregridding")

    # Determine input directory
    inputdir = Path(args.inputdir)
    if not inputdir.exists():
        raise ValueError(f"inputdir {inputdir} does not exist")

    # Determine output directory and create if needed
    outputdir = Path(args.outputdir)
    if not outputdir.exists():
        try:
            outputdir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise ValueError(f"Could not create output directory {outputdir}, error: {e}")

    # Determine weights file
    if args.inputres == 'ne16':
        weight_file = "/datalake/NS9560K/diagnostics/land_xesmf_diag_data/map_ne16pg3_to_1.9x2.5_nomask_scripgrids_c250425.nc"
    elif args.inputres == 'ne30':
        weight_file = "/datalake/NS9560K/diagnostics/land_xesmf_diag_data/map_ne30pg3_to_0.5x0.5_nomask_aave_da_c180515.nc"
    else:
        raise ValueError("only ne16 and ne30 are currently supported")

    # Determine list of files to regrid
    filelist = sorted({f for pattern in include_patterns for f in glob.glob(f"{inputdir}/*{pattern}*.nc")})
    if not filelist:
        logger.error(f"No netcdf files found in {inputdir}")
        return

    logger.info(f"Found {len(filelist)} file(s) to process with {args.workers} worker(s)")

    if args.workers == 1:
        # Serial path: initialise once in the main process
        _init_worker(weight_file, _LOCAL_PATH, args.debug)
        for filepath in filelist:
            process_file(filepath, args.realm, str(outputdir), args.debug)
    else:
        # Parallel path: each worker process calls _init_worker once on startup,
        # then handles its share of files without rebuilding the regridder.
        with ProcessPoolExecutor(
            max_workers=args.workers,
            initializer=_init_worker,
            initargs=(weight_file, _LOCAL_PATH, args.debug),
        ) as executor:
            futures = {
                executor.submit(process_file, fp, args.realm, str(outputdir), args.debug): fp
                for fp in filelist
            }
            for future in as_completed(futures):
                fp = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Failed to process {fp}: {e}")

if __name__ == "__main__":
    main()
