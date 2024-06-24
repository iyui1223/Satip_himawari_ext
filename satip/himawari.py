'''
This is a wrapper tool for ahi_hsd extension of satpy, which combines and converts the raw image data from Himawari satellite into single zarr file for easier access.

'''

import os
import zarr
import numpy as np
from satpy import Scene
from PIL import Image
import io
import dask.array as da

def convert_to_zarr(input_dir, output_dir, compress=True):
    files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.DAT.bz2')]
    scn = Scene(reader='ahi_hsd', filenames=files)
    bands = ['B{:02d}'.format(i) for i in range(1, 3)]  # Testing with first 2 bands.
    scn.load(bands)

    # Create Zarr output store
    zarr_store = zarr.open_group(output_dir, mode='w')

    # Assuming all bands with same resolution share the same area and time, use the first loaded band for these properties.
    reference_band = scn[bands[0]]
    lat, lon = reference_band.attrs['area'].get_lonlats()
    time = reference_band.attrs['start_time'].strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # Save lat and lon as Zarr arrays
    # TODO: Investigate whether the lat-long zarr arrays be used for all sensors which are saved on the same resolution? And for the different time period?
    #       If not, can it be adjusted to be so without interpolating and only with slicing?
    # TODO: How to save multiple (3) types of griddings for each bands in same zarr file? The separate resolution requires separate file organization?
    zarr_store.create_dataset('latitude', data=lat, chunks=(1000, 1000), overwrite=True)
    zarr_store.create_dataset('longitude', data=lon, chunks=(1000, 1000), overwrite=True)
    zarr_store.attrs['time'] = time

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
        img.save(buffer, format='JPEG', quality=85) # TODO: decide the preferred quality.
        buffer.seek(0)  # Rewind the buffer to the beginning

        # Store JPEG compressed data in Zarr
        zarr_store.create_dataset(band, data=np.frombuffer(buffer.getvalue(), dtype=np.uint8), overwrite=True)

    print("Conversion completed.")

if __name__ == "__main__":
    input_dir = r'C:\Users\iyui\AppData\Roaming\MobaXterm\home\Himawari\Satip_himawari_ext\satip\ahi_hsd\data'
    output_dir = r'C:\Users\iyui\AppData\Roaming\MobaXterm\home\Himawari\Satip_himawari_ext\satip\ahi_hsd\zarr'
    convert_to_zarr(input_dir, output_dir)
