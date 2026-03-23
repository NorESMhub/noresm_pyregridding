# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

Before running any scripts, source the environment setup:
```bash
cd scripts && source setup.sh
```
This loads `ESMF/8.6.0-foss-2023a`, `Miniforge3/24.1.2-0`, and activates the conda environment at `/projects/NS9560K/diagnostics/land_xesmf_env/diag_xesmf_env/`.

## Running the Main Scripts

**Regrid files in a folder:**
```bash
python scripts/regrid_all_files_in_folder.py \
  --realm {atm|lnd} \
  --inputdir <path> \
  --outputdir <path> \
  --inputres {ne16|ne30} \
  [--workers N] [--debug]
```

**Generate time series:**
```bash
python scripts/gen_timeseries.py \
  --realm {atm|lnd} \
  --inputdir <path> \
  --outputdir <path> \
  [--workers N] [--overwrite_timeseries] [--debug]
```

## Architecture

The project converts climate model output from spectral element (SE/unstructured) grids to regular lat-lon grids using pre-computed XESMF weight files stored at `/datalake/NS9560K/diagnostics/land_xesmf_diag_data/`.

**`src/noresm_pyregridding/noresm_pyregridding.py`** — Core regridding logic:
- `make_se_regridder()` / `make_generic_regridder()` — create xESMF regridders from weight files
- `regrid_cam_se_data()` — regrid CAM (atmosphere) data; identifies variables by the `ncol` dimension
- `regrid_ctsm_se_data()` — regrid CTSM/CLM (land) data; normalizes by land fraction (`landfrac`) before/after regridding and scales FATES variables by `FATES_FRACTION`

**`src/noresm_pyregridding/misc_help_functions.py`** — Unit conversion utilities and `make_regridding_target_from_weightfile()` to extract target grid metadata from a weight file.

**`src/noresm_pyregridding/plotting_utils.py`** — Matplotlib/Cartopy visualization helpers (Robinson projection bias plots, multi-panel 3D plots).

**Key design details:**
- CAM uses `ncol` as its unstructured dimension; CTSM uses `lndgrid`
- Already-regridded files (suffixed `_regridded.nc`) are automatically skipped
- Dask (`LocalCluster` or `dask-jobqueue` for SLURM) is used for parallel file processing when `--workers > 1`
- Include patterns in `regrid_all_files_in_folder.py` (e.g., `cam.h0a`, `clm2.h0a`) filter which history files are processed

## Remote

GitHub: https://github.com/NorESMhub/noresm_pyregridding.git
