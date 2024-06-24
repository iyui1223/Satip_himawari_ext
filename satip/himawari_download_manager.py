#!/usr/bin/env python

"""
Himawari satellite data downloader
This script downloads Himawari generation 8-9 data from AWS webserver run by the US NOAA.
--- address of website ---
for the user specified period, bands, and area segments.
While the AWS makes no restriction of access or the data usage. Directly commercial usage is restriucted by the data owner JMA.
If the usage is not for R&D, or preriminary research for commercial use, users are adviced to obtain data from JMSBC.

Also, the data downloaded is raw image data in geostationry gridding: the most upsteam of the data processing.
If the data converted to conventional lat-long gridding is prefered, the users are adviced to use the ones provided by Chiba university.
"""

import os
import argparse
from datetime import datetime
import s3fs
from satpy import config
from concurrent.futures import ThreadPoolExecutor

def download_ahi_data(base_dir, start_time, end_time, channels, segments, parallel_downloads=4):
    # make base directory
    subdir = os.path.join(base_dir, f"ahi_hsd/{start_time.strftime('%Y%m%d_%H%M')}_typhoon_surigae")
    os.makedirs(subdir, exist_ok=True)
    fs = s3fs.S3FileSystem(anon=True)

    # Setup data files to download
    channel_resolution = {1: 10, 2: 10, 3: 5, 4: 10}
    data_files = []
    while start_time <= end_time:
        for channel in channels:
            resolution = channel_resolution.get(channel, 20) # 20 if not 1-4
            for segment in segments:
                filename = f"HS_H08_{start_time.strftime('%Y%m%d_%H%M')}_B{channel:02d}_FLDK_R{resolution:02d}_S{segment:02d}10.DAT.bz2"
                data_files.append((filename, start_time))

        # Increment time by 10 minutes for next file group
        start_time += timedelta(minutes=10)

    # Download using multiple threads
    def download_file(filename, date):
        local_filename = os.path.join(subdir, filename)
        if os.path.exists(local_filename):
            return f"Already downloaded: {filename}"

        remote_path = f"noaa-himawari8/AHI-L1b-FLDK/{date.year}/{date.strftime('%m/%d/%H%M')}/{filename}"
        fs.get(remote_path, local_filename)
        return f"Downloaded: {filename}"

    with ThreadPoolExecutor(max_workers=parallel_downloads) as executor:
        futures = [executor.submit(download_file, file, date) for file, date in data_files]
        for future in futures:
            print(future.result())

def main():
    parser = argparse.ArgumentParser(description="Download Himawari8-9 satellite data.")
    parser.add_argument("--base_dir", type=str, default=config.get("demo_data_dir", "."),
                        help="Base directory to download the data")
    parser.add_argument("--start_time", type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M'),
                        required=True, help="Start time in 'YYYY-MM-DD HH:MM' format")
    parser.add_argument("--end_time", type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M'),
                        required=True, help="End time in 'YYYY-MM-DD HH:MM' format")
    parser.add_argument("--channels", nargs='+', type=int, default=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                        help="List of channels to download")
    parser.add_argument("--segments", nargs='+', type=int, default=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                        help="List of area segments to download")
    parser.add_argument("--parallel_downloads", type=int, default=4,
                        help="Number of parallel downloads")

    args = parser.parse_args()

    download_ahi_data(args.base_dir, args.start_time, args.end_time, args.channels, args.segments, args.parallel_downloads)

if __name__ == "__main__":
    main()
