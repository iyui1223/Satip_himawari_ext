from satip.utils import (
    load_native_to_dataset,
    save_dataset_to_zarr,
    check_if_timestep_exists,
)
from satip.eumetsat import eumetsat_filename_to_datetime, eumetsat_cloud_name_to_datetime
import os
import pandas as pd
from pathlib import Path
import multiprocessing
from itertools import repeat
import xarray as xr
from glob import glob
from tqdm import tqdm

processed_queue = multiprocessing.Queue(maxsize = 64)

def split_per_3_months(directory: str,
                       zarr_path: str,
                       hrv_zarr_path: str,
                       region: str,
                       spatial_chunk_size: int = 256,
                       temporal_chunk_size: int = 1,):
    """
    Split per 3 months of these

    Args:
        directory:
        zarr_path:
        hrv_zarr_path:
        region:
        spatial_chunk_size:
        temporal_chunk_size:

    Returns:

    """

    # Get year
    year_directories = os.listdir(directory)
    print(year_directories)
    dirs = []
    zarrs = []
    hrv_zarrs = []
    for year in year_directories:
        print(year)
        print(os.path.join(directory, year))
        print(os.path.isdir(os.path.join(directory, year)))
        if not os.path.isdir(os.path.join(directory, year)):
            continue
        if year in ["2020", "2021"]:
            month_directories = os.listdir(os.path.join(directory, year))
            for month in month_directories:
                print(year)
                print(os.path.join(directory, year, month))
                print(os.path.isdir(os.path.join(directory, year, month)))
                if not os.path.isdir(os.path.join(directory, year, month)):
                    continue
                month_directory = os.path.join(directory, year.split('/')[0], month.split('/')[0])
                month_zarr_path = zarr_path + f"_{year.split('/')[0]}_{month.split('/')[0]}.zarr"
                hrv_month_zarr_path = hrv_zarr_path + f"_{year.split('/')[0]}" \
                                                  f"_{month.split('/')[0]}.zarr"
                dirs.append(month_directory)
                zarrs.append(month_zarr_path)
                hrv_zarrs.append(hrv_month_zarr_path)
                zarr_exists = os.path.exists(month_zarr_path)
                if not zarr_exists:
                    # Inital zarr path before then appending
                    compressed_native_files = list(Path(month_directory).rglob("*.bz2"))
                    dataset, hrv_dataset = load_native_to_dataset(compressed_native_files[0], region)
                    save_dataset_to_zarr(dataset, zarr_path=month_zarr_path, zarr_mode="w")
                    save_dataset_to_zarr(hrv_dataset, zarr_path=hrv_month_zarr_path, zarr_mode="w")
    print(dirs)
    print(zarrs)
    pool = multiprocessing.Pool(processes=16)
    for _ in tqdm(pool.imap_unordered(
            wrapper,
            zip(
                dirs,
                zarrs,
                hrv_zarrs,
                repeat(region),
                repeat(spatial_chunk_size),
                repeat(temporal_chunk_size)
                ),
            )):
        print("Month done")


def wrapper(args):
    dirs, zarrs, hrv_zarrs, region, spatial_chunk_size, temporal_chunk_size = args
    create_or_update_zarr_with_native_files(dirs, zarrs, hrv_zarrs, region, spatial_chunk_size,
                                            temporal_chunk_size)


def create_or_update_zarr_with_native_files(
    directory: str,
    zarr_path: str,
    hrv_zarr_path: str,
    region: str,
    spatial_chunk_size: int = 256,
    temporal_chunk_size: int = 1,
) -> None:
    """
    Creates or updates a zarr file with satellite native files

    Args:
        directory: Top-level directory containing the compressed native files
        zarr_path: Path of the final Zarr file
        region: Name of the region to keep for the datastore
        spatial_chunk_size: Chunk size, in pixels in the x  and y directions, passed to Xarray
        temporal_chunk_size: Chunk size, in timesteps, for saving into the zarr file

    """

    # Satpy Scene doesn't do well with fsspec
    compressed_native_files = list(Path(directory).rglob("*.bz2"))
    zarr_exists = os.path.exists(zarr_path)
    if zarr_exists:
        zarr_dataset = xr.open_zarr(zarr_path, consolidated=True)
        new_compressed_files = []
        for f in compressed_native_files:
            base_filename = f.name
            file_timestep = eumetsat_filename_to_datetime(str(base_filename))
            exists = check_if_timestep_exists(
                pd.Timestamp(file_timestep).round("5 min"), zarr_dataset
            )
            if not exists:
                new_compressed_files.append(f)
        compressed_native_files = new_compressed_files
    # Check if zarr already exists
    for entry in tqdm(compressed_native_files):
        dataset, hrv_dataset = load_native_to_dataset(entry, region)
        if dataset is not None and hrv_dataset is not None:
            save_dataset_to_zarr(
                dataset,
                zarr_path=zarr_path,
                x_size_per_chunk=spatial_chunk_size,
                y_size_per_chunk=spatial_chunk_size,
                timesteps_per_chunk=temporal_chunk_size,
                channel_chunk_size=11
            )
            save_dataset_to_zarr(
                hrv_dataset,
                zarr_path=hrv_zarr_path,
                x_size_per_chunk=spatial_chunk_size,
                y_size_per_chunk=spatial_chunk_size,
                timesteps_per_chunk=temporal_chunk_size,
                channel_chunk_size=1
            )
        del dataset
        del hrv_dataset


def pool_init(q):
    global processed_queue # make queue global in workers
    processed_queue = q

def native_wrapper(filename_and_area):
    filename, area = filename_and_area
    processed_queue.put(load_native_to_dataset(filename, area))
