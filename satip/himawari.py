#!/usr/bin/env python
#!/usr/bin/env python
'''
This is a wrapper tool for ahi_hsd extension of satpy, which combines and converts the raw image data from Himawari satellite into single zarr file for easier access.
'''

import os
import zarr
import numpy as np
from satpy import Scene
from PIL import Image
import io
from datetime import datetime, timedelta

def list_input_dirs(base_dir, start_time, end_time):
    """Generate directories based on start and end time with a step of 10 minutes."""
    current_time = start_time
    while current_time <= end_time:
        subdir = os.path.join(base_dir, current_time.strftime('%Y%m%d_%H%M'))
        if os.path.exists(subdir):
            yield subdir
        current_time += timedelta(minutes=10)

def process_file(input_file, zarr_store, timestamp, bands):
    print(f"Processing file {input_file}")
    
    scn = Scene(reader='ahi_hsd', filenames=[input_file])
    
    available_bands = scn.available_dataset_names()
    bands = [band for band in ['B{:02d}'.format(i) for i in range(1, 17)] if band in available_bands]
    scn.load(bands)
    
    # Create a subgroup for the current timestamp if it does not exist
    if timestamp not in zarr_store:
        time_group = zarr_store.create_group(timestamp, overwrite=False)
    else:
        time_group = zarr_store[timestamp]
        
    for band in bands:
        data_array = scn[band].compute()

        # Normalize data to 0-255 and convert to uint8, to reduce the data volume
        # TODO: How to give universal max and min values for each observation bands?
        # If 8int conversion is adequate, it as well means that the compressed data resolution is about 1/256:
        # hence about 1/256 = ~0.3% top-bottom quantile may as well be adequate for rounding.
        data_normalized = ((data_array - data_array.min()) / (data_array.max() - data_array.min()) * 255).astype(np.uint8)
        
        # Convert xarray DataArray to numpy array before using it with PIL
        numpy_data = data_normalized.values

        # Save as JPEG to a buffer
        img = Image.fromarray(numpy_data)
        buffer = io.BytesIO()
        img.save(buffer, format='JPEG', quality=85)
        buffer.seek(0)  # Rewind the buffer to the beginning

        # Store JPEG compressed data in Zarr
        time_group.create_dataset(band, data=np.frombuffer(buffer.getvalue(), dtype=np.uint8), overwrite=True)

def convert_to_zarr(input_dirs, output_dir, compress=True):
    zarr_store = zarr.open_group(output_dir, mode='a')

    for input_dir in input_dirs:
        print(f"Processing data in {input_dir}")
        files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.DAT.bz2')]
        if not files:
            print(f"No files found in {input_dir}")
            continue

        # Extract timestamp from the directory name (assuming the format is YYYYMMDD_HHMM)
        timestamp = os.path.basename(input_dir)

        for file in files:
            process_file(file, zarr_store, timestamp, ['B{:02d}'.format(i) for i in range(1, 17)])
    
    print("Conversion completed.")

if __name__ == "__main__":
    base_dir = r'C:\Users\iyui\AppData\Roaming\MobaXterm\home\Himawari\Satip_himawari_ext\satip\ahi_hsd\data'
    output_dir = r'C:\Users\iyui\AppData\Roaming\MobaXterm\home\Himawari\Satip_himawari_ext\satip\ahi_hsd\zarr'
    start_time = datetime(2021, 10, 1, 0, 0)
    end_time = datetime(2021, 10, 1, 1, 0)
    input_dirs = list_input_dirs(base_dir, start_time, end_time)

    convert_to_zarr(input_dirs, output_dir)






