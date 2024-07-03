#!/usr/bin/env python

"""
Himawari satellite data downloader
This script downloads Himawari generation 8-9 data from AWS webserver run by the US NOAA (National Oceanic and Atmospheric Administration).
https://registry.opendata.aws/noaa-himawari/
for the user specified period, bands, and area segments.
While the AWS makes no restriction of access or the data usage, directly commercial usage is restricted by the data owner JMA, Japan Meteorological Agency.
If the usage is not for R&D, nor preriminary research for commercial use, users are adviced to obtain data from JMBSC.
https://www.jmbsc.or.jp/jp/online/satellite/s-online11.html; Even though their website is mostly in Jaapanese, they will accept English speaking customers. data@jmbsc.or.jp
Also, the data downloaded is raw image data in geostationry gridding: the most upsteam of the data processing.
If the data converted to conventional lat-long gridding is prefered, the users are adviced to use the interpolated product provided by Chiba university http://quicklooks.cr.chiba-u.ac.jp/~himawari_movie/rd_gridded.html.
"""

import os
import argparse
from datetime import datetime, timedelta
import s3fs
from satpy import config
from concurrent.futures import ThreadPoolExecutor

def download_ahi_data(base_dir, start_time, end_time, channels, segments, parallel_downloads=4):
    fs = s3fs.S3FileSystem(anon=True)

    current_time = start_time
    while current_time <= end_time:
        # Create subdirectory for each time step
        subdir = os.path.join(base_dir, current_time.strftime('%Y%m%d_%H%M'))
        os.makedirs(subdir, exist_ok=True)

        data_files = []
        for channel in channels:
            resolution = {1: 10, 2: 10, 3: 5, 4: 10}.get(channel, 20)
            for segment in segments:
                filename = f"HS_H08_{current_time.strftime('%Y%m%d_%H%M')}_B{channel:02d}_FLDK_R{resolution:02d}_S{segment:02d}10.DAT.bz2"
                data_files.append((filename, subdir))

        def download_file(file_info):
            filename, dir_path = file_info
            local_filename = os.path.join(dir_path, filename)
            if not os.path.exists(local_filename):
                remote_path = f"noaa-himawari8/AHI-L1b-FLDK/{current_time.year}/{current_time.strftime('%m/%d/%H%M')}/{filename}"
                fs.get(remote_path, local_filename)
                return f"Downloaded: {filename}"
            else:
                return f"Already downloaded: {filename}"

        with ThreadPoolExecutor(max_workers=parallel_downloads) as executor:
            futures = [executor.submit(download_file, file_info) for file_info in data_files]
            for future in futures:
                print(future.result())

        current_time += timedelta(minutes=10)

def main():
    parser = argparse.ArgumentParser(description="Download Himawari8-9 satellite data.")
    parser.add_argument("--base_dir", type=str, default=config.get("demo_data_dir", "."),
                        help="Base directory to download the data")
    parser.add_argument("--start_time", type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M'),
                        required=True, help="Start time in 'YYYY-MM-DD HH:MM' format")
    parser.add_argument("--end_time", type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M'),
                        required=True, help="End time in 'YYYY-MM-DD HH:MM' format")
    parser.add_argument("--channels", nargs='+', type=int, default=[1, 2, 3, 4],
                        help="List of channels to download")
    parser.add_argument("--segments", nargs='+', type=int, default=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        help="List of area segments to download")
    parser.add_argument("--parallel_downloads", type=int, default=4,
                        help="Number of parallel downloads")
    args = parser.parse_args()

    download_ahi_data(args.base_dir, args.start_time, args.end_time, args.channels, args.segments, args.parallel_downloads)

if __name__ == "__main__":
    main()
    
    
    