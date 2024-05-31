"""
    Himawari Satellite Data Downloader
    -----------------------------------

    Description:
    ------------
    This script is designed to automate the downloading of Himawari satellite data,
    specifically the full-disk standard data from the Himawari-8 and Himawari-9 satellites.
    It focuses on downloading the most upstream of data processing (raw image data), which has not been
    projected into mapping coordinates.
    
    Additional References:
    ----------------------
    - Himawari Standard Data User Guide (Japanese): https://www.data.jma.go.jp/mscweb/ja/info/pdf/HS_D_users_guide_jp_v13.pdf

    Note:

    Alternative Data Sources:
    -------------------------
    For users requiring data that is already projected into latitude-longitude coordinates,
    it is recommended to explore resources provided by Chiba University, available at:
    http://www.cr.chiba-u.jp/databases/GEO/H8_9/FD/index_jp.html

    For tools that can directly interface with these datasets, consider exploring projects like:
    https://github.com/zxdawn/Himawari-8-gridded

    Usage:
    ------
    Users can specify the start and end dates for the data download, and the script will manage
    URL generation, data retrieval, and local storage. The downloader skips downloading files
    that already exist locally to save bandwidth and reduce redundancy.

    TODO:
    -----
    - Implement data conversion from DAT format to more accessible formats like netCDF or ZARR
      for easier integration with data analysis tools.
    - Address the 2GB download limitation. Contact NICT for clarification on whether this limit
      applies per day or per hour and inquire about possible exemptions or temporary increases
      for research purposes.
    - Enhance error handling to manage network issues and data availability more gracefully.


"""
import datetime
import requests
import logging
import os

class HimawariDownloadManager:
    def __init__(self, data_dir, log_directory=None):
        self.data_dir = data_dir
        self.log_directory = log_directory if log_directory else data_dir
        self.ensure_directory_exists(self.data_dir)
        self.setup_logging()

    def setup_logging(self):
        log_file = os.path.join(self.log_directory, 'himawari_download.log')
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s %(message)s')
        logging.info("HimawariDownloadManager initialized.")

    @staticmethod
    def ensure_directory_exists(directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
            logging.info(f"Created directory: {directory}")

    def download_himawari_data(self, start_date, end_date):
        base_url = "https://sc-nc-web.nict.go.jp/wsdb_osndisk/shareDirDownload/03ZzRnKS"
        product_path = "HISD/Hsfd"
        switch_date = datetime.datetime(2022, 12, 13, 5)  # 5 UTC on Dec 13, 2022

        current_date = start_date
        while current_date <= end_date:
            date_path = current_date.strftime('%Y%m/%d/%Y%m%d%H%M')
            
            # Determine which satellite to use based on the date
            if current_date < datetime.datetime(2015, 7, 1):
                logging.info("Himawari data not supported before 2015 Jul 1.")
                continue
            elif current_date < switch_date:
                satellite = "HIMAWARI-8"
            else:
                satellite = "HIMAWARI-9"

            for minute in range(0, 60, 10): # Every ten minutes
                for band in range(1, 17):  # 16 bands
                    file_name = f"{current_date.strftime('%Y%m%d%H%M')}_{minute:02d}_B{band:02d}.DAT"
                    full_url = f"{base_url}/{satellite}/{product_path}/{date_path}/{minute:02d}/B{band:02d}/{file_name}"
                    self.download_file(full_url, file_name)
            current_date += datetime.timedelta(days=1)

    def download_file(self, url, file_name):
        local_path = os.path.join(self.data_dir, file_name)
        if not os.path.exists(local_path):
            response = requests.get(url)
            if response.status_code == 200:
                with open(local_path, 'wb') as file:
                    file.write(response.content)
                logging.info(f"Downloaded and saved {file_name}")
            else:
                logging.warning(f"Failed to download {file_name} from {url}")
        else:
            logging.info(f"{file_name} already exists locally. Skipping download.")

# testung for switch over
if __name__ == "__main__":
    data_dir = '/path/to/data'
    downloader = HimawariDownloadManager(data_dir)
    start_date = datetime.datetime(2022, 12, 12)
    end_date = datetime.datetime(2022, 12, 14)
    downloader.download_himawari_data(start_date, end_date)
