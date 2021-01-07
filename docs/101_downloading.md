# Downloading Data From EUMETSAT



```python
from satip import eumetsat

import matplotlib.pyplot as plt
import cartopy.crs as ccrs

import os
import dotenv
```

    C:\Users\Ayrto\anaconda3\envs\satip_dev\lib\site-packages\google\auth\_default.py:69: UserWarning: Your application has authenticated using end user credentials from Google Cloud SDK without a quota project. You might receive a "quota exceeded" or "API not enabled" error. We recommend you rerun `gcloud auth application-default login` and make sure a quota project is added. Or you can use service accounts instead. For more information about service accounts, see https://cloud.google.com/docs/authentication/
      warnings.warn(_CLOUD_SDK_CREDENTIALS_WARNING)
    Downloading: 100%|â–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆâ–ˆ| 1/1 [00:00<00:00,  1.40rows/s]
    

<br>

### User Inputs

We have to specify the directory where the data native filepaths are located

```python
data_dir = '../data/raw'
debug_fp = '../logs/EUMETSAT_download.txt'
env_vars_fp = '../.env'
metadata_db_fp = '../data/EUMETSAT_metadata.db'
```

<br>

### Using the Download Manager

First we'll load the the environment variables

```python
dotenv.load_dotenv(env_vars_fp)

user_key = os.environ.get('USER_KEY')
user_secret = os.environ.get('USER_SECRET')
slack_id = os.environ.get('SLACK_ID')
slack_webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
```

<br>

Then we'll use the download manager to retrieve a single dataset

```python
dm = eumetsat.DownloadManager(user_key, user_secret, data_dir, metadata_db_fp, debug_fp, 
                              slack_webhook_url=slack_webhook_url, slack_id=slack_id)

start_date = '2020-01-01 00:00'
end_date = '2020-01-01 00:05'

dm.download_datasets(start_date, end_date)
```

    2020-12-17 00:21:11,192 - INFO - ********** Download Manager Initialised **************
    2020-12-17 00:21:11,777 - INFO - 1 files queried, 0 found in ../data/raw, 1 to download.
    


<div><span class="Text-label" style="display:inline-block; overflow:hidden; white-space:nowrap; text-overflow:ellipsis; min-width:0; max-width:15ex; vertical-align:middle; text-align:right"></span>
<progress style="width:60ex" max="1" value="1" class="Progress-main"/></progress>
<span class="Progress-label"><strong>100%</strong></span>
<span class="Iteration-label">1/1</span>
<span class="Time-label">[00:07<00:07, 6.59s/it]</span></div>





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>start_date</th>
      <th>end_date</th>
      <th>result_time</th>
      <th>platform_short_name</th>
      <th>platform_orbit_type</th>
      <th>instrument_name</th>
      <th>sensor_op_mode</th>
      <th>center_srs_name</th>
      <th>center_position</th>
      <th>file_name</th>
      <th>file_size</th>
      <th>missing_pct</th>
      <th>downloaded</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2020-01-01 00:00:07.683000+00:00</td>
      <td>2020-01-01 00:04:14.102000+00:00</td>
      <td>2020-01-01 00:04:14.102000+00:00</td>
      <td>MSG3</td>
      <td>GEO</td>
      <td>SEVIRI</td>
      <td>RSS</td>
      <td>EPSG:4326</td>
      <td>0 9.5</td>
      <td>MSG3-SEVI-MSG15-0100-NA-20200101000414.1020000...</td>
      <td>99819</td>
      <td>0.0</td>
      <td>2020-12-17 00:21:18.312026</td>
    </tr>
  </tbody>
</table>
</div>



<br>

Once the files have been downloaded they will be automatically detected and skipped if downloading is attempted again

```python
_ = dm.download_datasets(start_date, end_date)
```

    2020-12-17 00:21:31,507 - INFO - 1 files queried, 1 found in ../data/raw, 0 to download.
    2020-12-17 00:21:31,512 - INFO - No files will be downloaded. Set DownloadManager bucket_name argument for local download
    

<br>

We can retrieve the metadata for all historical downloads by calling the `get_df_metadata` method

```python
df_metadata = dm.get_df_metadata()

df_metadata.head()
```