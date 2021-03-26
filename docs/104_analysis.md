# Extracting Time-Series for Analysis



```python
from satip import io

import shapely
import geopandas as gpd

import seaborn as sns
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
```

<br>

### User Inputs

We have to specify the bucket where the data is located

```python
zarr_bucket = 'solar-pv-nowcasting-data/satellite/EUMETSAT/SEVIRI_RSS/full_extent_TM_int16'
```

<br>

### Loading Data

Then the `satip` wrapper for loading data will generate an `xarray` `Dataset`

```python
ds = io.load_from_zarr_bucket(zarr_bucket)

ds
```




<div><svg style="position: absolute; width: 0; height: 0; overflow: hidden">
<defs>
<symbol id="icon-database" viewBox="0 0 32 32">
<path d="M16 0c-8.837 0-16 2.239-16 5v4c0 2.761 7.163 5 16 5s16-2.239 16-5v-4c0-2.761-7.163-5-16-5z"></path>
<path d="M16 17c-8.837 0-16-2.239-16-5v6c0 2.761 7.163 5 16 5s16-2.239 16-5v-6c0 2.761-7.163 5-16 5z"></path>
<path d="M16 26c-8.837 0-16-2.239-16-5v6c0 2.761 7.163 5 16 5s16-2.239 16-5v-6c0 2.761-7.163 5-16 5z"></path>
</symbol>
<symbol id="icon-file-text2" viewBox="0 0 32 32">
<path d="M28.681 7.159c-0.694-0.947-1.662-2.053-2.724-3.116s-2.169-2.030-3.116-2.724c-1.612-1.182-2.393-1.319-2.841-1.319h-15.5c-1.378 0-2.5 1.121-2.5 2.5v27c0 1.378 1.122 2.5 2.5 2.5h23c1.378 0 2.5-1.122 2.5-2.5v-19.5c0-0.448-0.137-1.23-1.319-2.841zM24.543 5.457c0.959 0.959 1.712 1.825 2.268 2.543h-4.811v-4.811c0.718 0.556 1.584 1.309 2.543 2.268zM28 29.5c0 0.271-0.229 0.5-0.5 0.5h-23c-0.271 0-0.5-0.229-0.5-0.5v-27c0-0.271 0.229-0.5 0.5-0.5 0 0 15.499-0 15.5 0v7c0 0.552 0.448 1 1 1h7v19.5z"></path>
<path d="M23 26h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
<path d="M23 22h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
<path d="M23 18h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
</symbol>
</defs>
</svg>
<style>/* CSS stylesheet for displaying xarray objects in jupyterlab.
 *
 */

:root {
  --xr-font-color0: var(--jp-content-font-color0, rgba(0, 0, 0, 1));
  --xr-font-color2: var(--jp-content-font-color2, rgba(0, 0, 0, 0.54));
  --xr-font-color3: var(--jp-content-font-color3, rgba(0, 0, 0, 0.38));
  --xr-border-color: var(--jp-border-color2, #e0e0e0);
  --xr-disabled-color: var(--jp-layout-color3, #bdbdbd);
  --xr-background-color: var(--jp-layout-color0, white);
  --xr-background-color-row-even: var(--jp-layout-color1, white);
  --xr-background-color-row-odd: var(--jp-layout-color2, #eeeeee);
}

html[theme=dark],
body.vscode-dark {
  --xr-font-color0: rgba(255, 255, 255, 1);
  --xr-font-color2: rgba(255, 255, 255, 0.54);
  --xr-font-color3: rgba(255, 255, 255, 0.38);
  --xr-border-color: #1F1F1F;
  --xr-disabled-color: #515151;
  --xr-background-color: #111111;
  --xr-background-color-row-even: #111111;
  --xr-background-color-row-odd: #313131;
}

.xr-wrap {
  display: block;
  min-width: 300px;
  max-width: 700px;
}

.xr-text-repr-fallback {
  /* fallback to plain text repr when CSS is not injected (untrusted notebook) */
  display: none;
}

.xr-header {
  padding-top: 6px;
  padding-bottom: 6px;
  margin-bottom: 4px;
  border-bottom: solid 1px var(--xr-border-color);
}

.xr-header > div,
.xr-header > ul {
  display: inline;
  margin-top: 0;
  margin-bottom: 0;
}

.xr-obj-type,
.xr-array-name {
  margin-left: 2px;
  margin-right: 10px;
}

.xr-obj-type {
  color: var(--xr-font-color2);
}

.xr-sections {
  padding-left: 0 !important;
  display: grid;
  grid-template-columns: 150px auto auto 1fr 20px 20px;
}

.xr-section-item {
  display: contents;
}

.xr-section-item input {
  display: none;
}

.xr-section-item input + label {
  color: var(--xr-disabled-color);
}

.xr-section-item input:enabled + label {
  cursor: pointer;
  color: var(--xr-font-color2);
}

.xr-section-item input:enabled + label:hover {
  color: var(--xr-font-color0);
}

.xr-section-summary {
  grid-column: 1;
  color: var(--xr-font-color2);
  font-weight: 500;
}

.xr-section-summary > span {
  display: inline-block;
  padding-left: 0.5em;
}

.xr-section-summary-in:disabled + label {
  color: var(--xr-font-color2);
}

.xr-section-summary-in + label:before {
  display: inline-block;
  content: 'â–º';
  font-size: 11px;
  width: 15px;
  text-align: center;
}

.xr-section-summary-in:disabled + label:before {
  color: var(--xr-disabled-color);
}

.xr-section-summary-in:checked + label:before {
  content: 'â–¼';
}

.xr-section-summary-in:checked + label > span {
  display: none;
}

.xr-section-summary,
.xr-section-inline-details {
  padding-top: 4px;
  padding-bottom: 4px;
}

.xr-section-inline-details {
  grid-column: 2 / -1;
}

.xr-section-details {
  display: none;
  grid-column: 1 / -1;
  margin-bottom: 5px;
}

.xr-section-summary-in:checked ~ .xr-section-details {
  display: contents;
}

.xr-array-wrap {
  grid-column: 1 / -1;
  display: grid;
  grid-template-columns: 20px auto;
}

.xr-array-wrap > label {
  grid-column: 1;
  vertical-align: top;
}

.xr-preview {
  color: var(--xr-font-color3);
}

.xr-array-preview,
.xr-array-data {
  padding: 0 5px !important;
  grid-column: 2;
}

.xr-array-data,
.xr-array-in:checked ~ .xr-array-preview {
  display: none;
}

.xr-array-in:checked ~ .xr-array-data,
.xr-array-preview {
  display: inline-block;
}

.xr-dim-list {
  display: inline-block !important;
  list-style: none;
  padding: 0 !important;
  margin: 0;
}

.xr-dim-list li {
  display: inline-block;
  padding: 0;
  margin: 0;
}

.xr-dim-list:before {
  content: '(';
}

.xr-dim-list:after {
  content: ')';
}

.xr-dim-list li:not(:last-child):after {
  content: ',';
  padding-right: 5px;
}

.xr-has-index {
  font-weight: bold;
}

.xr-var-list,
.xr-var-item {
  display: contents;
}

.xr-var-item > div,
.xr-var-item label,
.xr-var-item > .xr-var-name span {
  background-color: var(--xr-background-color-row-even);
  margin-bottom: 0;
}

.xr-var-item > .xr-var-name:hover span {
  padding-right: 5px;
}

.xr-var-list > li:nth-child(odd) > div,
.xr-var-list > li:nth-child(odd) > label,
.xr-var-list > li:nth-child(odd) > .xr-var-name span {
  background-color: var(--xr-background-color-row-odd);
}

.xr-var-name {
  grid-column: 1;
}

.xr-var-dims {
  grid-column: 2;
}

.xr-var-dtype {
  grid-column: 3;
  text-align: right;
  color: var(--xr-font-color2);
}

.xr-var-preview {
  grid-column: 4;
}

.xr-var-name,
.xr-var-dims,
.xr-var-dtype,
.xr-preview,
.xr-attrs dt {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  padding-right: 10px;
}

.xr-var-name:hover,
.xr-var-dims:hover,
.xr-var-dtype:hover,
.xr-attrs dt:hover {
  overflow: visible;
  width: auto;
  z-index: 1;
}

.xr-var-attrs,
.xr-var-data {
  display: none;
  background-color: var(--xr-background-color) !important;
  padding-bottom: 5px !important;
}

.xr-var-attrs-in:checked ~ .xr-var-attrs,
.xr-var-data-in:checked ~ .xr-var-data {
  display: block;
}

.xr-var-data > table {
  float: right;
}

.xr-var-name span,
.xr-var-data,
.xr-attrs {
  padding-left: 25px !important;
}

.xr-attrs,
.xr-var-attrs,
.xr-var-data {
  grid-column: 1 / -1;
}

dl.xr-attrs {
  padding: 0;
  margin: 0;
  display: grid;
  grid-template-columns: 125px auto;
}

.xr-attrs dt,
.xr-attrs dd {
  padding: 0;
  margin: 0;
  float: left;
  padding-right: 10px;
  width: auto;
}

.xr-attrs dt {
  font-weight: normal;
  grid-column: 1;
}

.xr-attrs dt:hover span {
  display: inline-block;
  background: var(--xr-background-color);
  padding-right: 10px;
}

.xr-attrs dd {
  grid-column: 2;
  white-space: pre-wrap;
  word-break: break-all;
}

.xr-icon-database,
.xr-icon-file-text2 {
  display: inline-block;
  vertical-align: middle;
  width: 1em;
  height: 1.5em !important;
  stroke-width: 0;
  stroke: currentColor;
  fill: currentColor;
}
</style><pre class='xr-text-repr-fallback'>&lt;xarray.Dataset&gt;
Dimensions:                (time: 924, variable: 12, x: 1870, y: 1831)
Coordinates:
  * time                   (time) datetime64[ns] 2020-12-16T18:40:08 ... 2021...
  * variable               (variable) object &#x27;HRV&#x27; &#x27;IR_016&#x27; ... &#x27;WV_073&#x27;
  * x                      (x) float64 -3.088e+06 -3.084e+06 ... 4.388e+06
  * y                      (y) float64 9.012e+06 9.008e+06 ... 1.692e+06
Data variables:
    stacked_eumetsat_data  (time, x, y, variable) int16 dask.array&lt;chunksize=(36, 1870, 1831, 1), meta=np.ndarray&gt;</pre><div class='xr-wrap' hidden><div class='xr-header'><div class='xr-obj-type'>xarray.Dataset</div></div><ul class='xr-sections'><li class='xr-section-item'><input id='section-6a1398ba-8123-465b-81e7-c248227e8d78' class='xr-section-summary-in' type='checkbox' disabled ><label for='section-6a1398ba-8123-465b-81e7-c248227e8d78' class='xr-section-summary'  title='Expand/collapse section'>Dimensions:</label><div class='xr-section-inline-details'><ul class='xr-dim-list'><li><span class='xr-has-index'>time</span>: 924</li><li><span class='xr-has-index'>variable</span>: 12</li><li><span class='xr-has-index'>x</span>: 1870</li><li><span class='xr-has-index'>y</span>: 1831</li></ul></div><div class='xr-section-details'></div></li><li class='xr-section-item'><input id='section-a7289a84-5c2d-4183-8151-382f0109ddd9' class='xr-section-summary-in' type='checkbox'  checked><label for='section-a7289a84-5c2d-4183-8151-382f0109ddd9' class='xr-section-summary' >Coordinates: <span>(4)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><ul class='xr-var-list'><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>time</span></div><div class='xr-var-dims'>(time)</div><div class='xr-var-dtype'>datetime64[ns]</div><div class='xr-var-preview xr-preview'>2020-12-16T18:40:08 ... 2021-01-...</div><input id='attrs-8ae926ab-e1be-4468-b957-75add1476382' class='xr-var-attrs-in' type='checkbox' disabled><label for='attrs-8ae926ab-e1be-4468-b957-75add1476382' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-93910307-bd25-4f2b-8bee-ea63bfb529ba' class='xr-var-data-in' type='checkbox'><label for='data-93910307-bd25-4f2b-8bee-ea63bfb529ba' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'></dl></div><div class='xr-var-data'><pre>array([&#x27;2020-12-16T18:40:08.000000000&#x27;, &#x27;2021-01-07T12:04:16.000000000&#x27;,
       &#x27;2021-01-07T12:09:16.000000000&#x27;, ..., &#x27;2021-01-21T22:14:15.000000000&#x27;,
       &#x27;2021-01-21T22:19:15.000000000&#x27;, &#x27;2021-01-21T22:24:16.000000000&#x27;],
      dtype=&#x27;datetime64[ns]&#x27;)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>variable</span></div><div class='xr-var-dims'>(variable)</div><div class='xr-var-dtype'>object</div><div class='xr-var-preview xr-preview'>&#x27;HRV&#x27; &#x27;IR_016&#x27; ... &#x27;WV_073&#x27;</div><input id='attrs-f1945135-f2b6-4409-820c-f55bc698f9c7' class='xr-var-attrs-in' type='checkbox' disabled><label for='attrs-f1945135-f2b6-4409-820c-f55bc698f9c7' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-907e5c81-4a3c-497e-b98e-374450bf5ca7' class='xr-var-data-in' type='checkbox'><label for='data-907e5c81-4a3c-497e-b98e-374450bf5ca7' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'></dl></div><div class='xr-var-data'><pre>array([&#x27;HRV&#x27;, &#x27;IR_016&#x27;, &#x27;IR_039&#x27;, &#x27;IR_087&#x27;, &#x27;IR_097&#x27;, &#x27;IR_108&#x27;, &#x27;IR_120&#x27;,
       &#x27;IR_134&#x27;, &#x27;VIS006&#x27;, &#x27;VIS008&#x27;, &#x27;WV_062&#x27;, &#x27;WV_073&#x27;], dtype=object)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>x</span></div><div class='xr-var-dims'>(x)</div><div class='xr-var-dtype'>float64</div><div class='xr-var-preview xr-preview'>-3.088e+06 -3.084e+06 ... 4.388e+06</div><input id='attrs-ec174fc5-5419-4d56-85b8-aa5b23d9e89d' class='xr-var-attrs-in' type='checkbox' disabled><label for='attrs-ec174fc5-5419-4d56-85b8-aa5b23d9e89d' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-f00413db-369b-4af8-b374-a7b9bb965843' class='xr-var-data-in' type='checkbox'><label for='data-f00413db-369b-4af8-b374-a7b9bb965843' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'></dl></div><div class='xr-var-data'><pre>array([-3088000., -3084000., -3080000., ...,  4380000.,  4384000.,  4388000.])</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>y</span></div><div class='xr-var-dims'>(y)</div><div class='xr-var-dtype'>float64</div><div class='xr-var-preview xr-preview'>9.012e+06 9.008e+06 ... 1.692e+06</div><input id='attrs-a07661f6-a4c4-4d5b-8b04-c1308ecc4d66' class='xr-var-attrs-in' type='checkbox' disabled><label for='attrs-a07661f6-a4c4-4d5b-8b04-c1308ecc4d66' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-fccdcc3d-f831-4225-90c4-2de8c3b6adad' class='xr-var-data-in' type='checkbox'><label for='data-fccdcc3d-f831-4225-90c4-2de8c3b6adad' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'></dl></div><div class='xr-var-data'><pre>array([9012000., 9008000., 9004000., ..., 1700000., 1696000., 1692000.])</pre></div></li></ul></div></li><li class='xr-section-item'><input id='section-0bf62ab6-066e-4c5a-b0f9-f9873d46828d' class='xr-section-summary-in' type='checkbox'  checked><label for='section-0bf62ab6-066e-4c5a-b0f9-f9873d46828d' class='xr-section-summary' >Data variables: <span>(1)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><ul class='xr-var-list'><li class='xr-var-item'><div class='xr-var-name'><span>stacked_eumetsat_data</span></div><div class='xr-var-dims'>(time, x, y, variable)</div><div class='xr-var-dtype'>int16</div><div class='xr-var-preview xr-preview'>dask.array&lt;chunksize=(36, 1870, 1831, 1), meta=np.ndarray&gt;</div><input id='attrs-95264569-173a-485d-b180-8c95bd6711a3' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-95264569-173a-485d-b180-8c95bd6711a3' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-c9634843-ea33-42ed-86a3-a2e1c754eb35' class='xr-var-data-in' type='checkbox'><label for='data-c9634843-ea33-42ed-86a3-a2e1c754eb35' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>meta :</span></dt><dd>{&#x27;orbital_parameters&#x27;: {&#x27;projection_longitude&#x27;: 9.5, &#x27;projection_latitude&#x27;: 0.0, &#x27;projection_altitude&#x27;: 35785831.0}, &#x27;sun_earth_distance_correction_applied&#x27;: True, &#x27;sun_earth_distance_correction_factor&#x27;: 0.9680361623200268, &#x27;units&#x27;: &#x27;%&#x27;, &#x27;wavelength&#x27;: WavelengthRange(min=0.5, central=0.7, max=0.9, unit=&#x27;Âµm&#x27;), &#x27;standard_name&#x27;: &#x27;toa_bidirectional_reflectance&#x27;, &#x27;platform_name&#x27;: &#x27;Meteosat-10&#x27;, &#x27;sensor&#x27;: &#x27;seviri&#x27;, &#x27;start_time&#x27;: datetime.datetime(2020, 12, 16, 18, 35, 8, 985163), &#x27;end_time&#x27;: datetime.datetime(2020, 12, 16, 18, 40, 8, 829133), &#x27;area&#x27;: Area ID: geos_seviri_hrv
Description: SEVIRI high resolution channel area
Projection ID: seviri_hrv
Projection: {&#x27;a&#x27;: &#x27;6378169&#x27;, &#x27;h&#x27;: &#x27;35785831&#x27;, &#x27;lon_0&#x27;: &#x27;9.5&#x27;, &#x27;no_defs&#x27;: &#x27;None&#x27;, &#x27;proj&#x27;: &#x27;geos&#x27;, &#x27;rf&#x27;: &#x27;295.488065897014&#x27;, &#x27;type&#x27;: &#x27;crs&#x27;, &#x27;units&#x27;: &#x27;m&#x27;, &#x27;x_0&#x27;: &#x27;0&#x27;, &#x27;y_0&#x27;: &#x27;0&#x27;}
Number of columns: 5568
Number of rows: 4176
Area extent: (2792875.1692, 5571248.3904, -2775872.8853, 1394687.3495), &#x27;name&#x27;: &#x27;HRV&#x27;, &#x27;resolution&#x27;: 1000.134348869, &#x27;calibration&#x27;: &#x27;reflectance&#x27;, &#x27;modifiers&#x27;: (), &#x27;_satpy_id&#x27;: DataID(name=&#x27;HRV&#x27;, wavelength=WavelengthRange(min=0.5, central=0.7, max=0.9, unit=&#x27;Âµm&#x27;), resolution=1000.134348869, calibration=&lt;calibration.reflectance&gt;, modifiers=()), &#x27;ancillary_variables&#x27;: []}</dd></dl></div><div class='xr-var-data'><table>
<tr>
<td>
<table>
  <thead>
    <tr><td> </td><th> Array </th><th> Chunk </th></tr>
  </thead>
  <tbody>
    <tr><th> Bytes </th><td> 75.93 GB </td> <td> 246.53 MB </td></tr>
    <tr><th> Shape </th><td> (924, 1870, 1831, 12) </td> <td> (36, 1870, 1831, 1) </td></tr>
    <tr><th> Count </th><td> 313 Tasks </td><td> 312 Chunks </td></tr>
    <tr><th> Type </th><td> int16 </td><td> numpy.ndarray </td></tr>
  </tbody>
</table>
</td>
<td>
<svg width="404" height="238" style="stroke:rgb(0,0,0);stroke-width:1" >

  <!-- Horizontal lines -->
  <line x1="0" y1="0" x2="59" y2="0" style="stroke-width:2" />
  <line x1="0" y1="25" x2="59" y2="25" style="stroke-width:2" />

  <!-- Vertical lines -->
  <line x1="0" y1="0" x2="0" y2="25" style="stroke-width:2" />
  <line x1="2" y1="0" x2="2" y2="25" />
  <line x1="4" y1="0" x2="4" y2="25" />
  <line x1="6" y1="0" x2="6" y2="25" />
  <line x1="9" y1="0" x2="9" y2="25" />
  <line x1="11" y1="0" x2="11" y2="25" />
  <line x1="13" y1="0" x2="13" y2="25" />
  <line x1="16" y1="0" x2="16" y2="25" />
  <line x1="18" y1="0" x2="18" y2="25" />
  <line x1="20" y1="0" x2="20" y2="25" />
  <line x1="23" y1="0" x2="23" y2="25" />
  <line x1="25" y1="0" x2="25" y2="25" />
  <line x1="27" y1="0" x2="27" y2="25" />
  <line x1="30" y1="0" x2="30" y2="25" />
  <line x1="32" y1="0" x2="32" y2="25" />
  <line x1="34" y1="0" x2="34" y2="25" />
  <line x1="36" y1="0" x2="36" y2="25" />
  <line x1="39" y1="0" x2="39" y2="25" />
  <line x1="41" y1="0" x2="41" y2="25" />
  <line x1="43" y1="0" x2="43" y2="25" />
  <line x1="46" y1="0" x2="46" y2="25" />
  <line x1="48" y1="0" x2="48" y2="25" />
  <line x1="50" y1="0" x2="50" y2="25" />
  <line x1="53" y1="0" x2="53" y2="25" />
  <line x1="55" y1="0" x2="55" y2="25" />
  <line x1="57" y1="0" x2="57" y2="25" />
  <line x1="59" y1="0" x2="59" y2="25" style="stroke-width:2" />

  <!-- Colored Rectangle -->
  <polygon points="0.0,0.0 59.294117647058826,0.0 59.294117647058826,25.412616514582485 0.0,25.412616514582485" style="fill:#ECB172A0;stroke-width:0"/>

  <!-- Text -->
  <text x="29.647059" y="45.412617" font-size="1.0rem" font-weight="100" text-anchor="middle" >924</text>
  <text x="79.294118" y="12.706308" font-size="1.0rem" font-weight="100" text-anchor="middle" transform="rotate(0,79.294118,12.706308)">1</text>


  <!-- Horizontal lines -->
  <line x1="129" y1="0" x2="199" y2="70" style="stroke-width:2" />
  <line x1="129" y1="117" x2="199" y2="188" style="stroke-width:2" />

  <!-- Vertical lines -->
  <line x1="129" y1="0" x2="129" y2="117" style="stroke-width:2" />
  <line x1="199" y1="70" x2="199" y2="188" style="stroke-width:2" />

  <!-- Colored Rectangle -->
  <polygon points="129.0,0.0 199.58823529411765,70.58823529411765 199.58823529411765,188.0855614973262 129.0,117.49732620320856" style="fill:#ECB172A0;stroke-width:0"/>

  <!-- Horizontal lines -->
  <line x1="129" y1="0" x2="154" y2="0" style="stroke-width:2" />
  <line x1="199" y1="70" x2="225" y2="70" style="stroke-width:2" />

  <!-- Vertical lines -->
  <line x1="129" y1="0" x2="199" y2="70" style="stroke-width:2" />
  <line x1="131" y1="0" x2="201" y2="70" />
  <line x1="133" y1="0" x2="203" y2="70" />
  <line x1="135" y1="0" x2="205" y2="70" />
  <line x1="137" y1="0" x2="208" y2="70" />
  <line x1="139" y1="0" x2="210" y2="70" />
  <line x1="141" y1="0" x2="212" y2="70" />
  <line x1="143" y1="0" x2="214" y2="70" />
  <line x1="145" y1="0" x2="216" y2="70" />
  <line x1="148" y1="0" x2="218" y2="70" />
  <line x1="150" y1="0" x2="220" y2="70" />
  <line x1="152" y1="0" x2="222" y2="70" />
  <line x1="154" y1="0" x2="225" y2="70" style="stroke-width:2" />

  <!-- Colored Rectangle -->
  <polygon points="129.0,0.0 154.41261651458248,0.0 225.00085180870013,70.58823529411765 199.58823529411765,70.58823529411765" style="fill:#ECB172A0;stroke-width:0"/>

  <!-- Horizontal lines -->
  <line x1="199" y1="70" x2="225" y2="70" style="stroke-width:2" />
  <line x1="199" y1="188" x2="225" y2="188" style="stroke-width:2" />

  <!-- Vertical lines -->
  <line x1="199" y1="70" x2="199" y2="188" style="stroke-width:2" />
  <line x1="201" y1="70" x2="201" y2="188" />
  <line x1="203" y1="70" x2="203" y2="188" />
  <line x1="205" y1="70" x2="205" y2="188" />
  <line x1="208" y1="70" x2="208" y2="188" />
  <line x1="210" y1="70" x2="210" y2="188" />
  <line x1="212" y1="70" x2="212" y2="188" />
  <line x1="214" y1="70" x2="214" y2="188" />
  <line x1="216" y1="70" x2="216" y2="188" />
  <line x1="218" y1="70" x2="218" y2="188" />
  <line x1="220" y1="70" x2="220" y2="188" />
  <line x1="222" y1="70" x2="222" y2="188" />
  <line x1="225" y1="70" x2="225" y2="188" style="stroke-width:2" />

  <!-- Colored Rectangle -->
  <polygon points="199.58823529411765,70.58823529411765 225.00085180870013,70.58823529411765 225.00085180870013,188.0855614973262 199.58823529411765,188.0855614973262" style="fill:#ECB172A0;stroke-width:0"/>

  <!-- Text -->
  <text x="212.294544" y="208.085561" font-size="1.0rem" font-weight="100" text-anchor="middle" >12</text>
  <text x="245.000852" y="129.336898" font-size="1.0rem" font-weight="100" text-anchor="middle" transform="rotate(-90,245.000852,129.336898)">1831</text>
  <text x="154.294118" y="172.791444" font-size="1.0rem" font-weight="100" text-anchor="middle" transform="rotate(45,154.294118,172.791444)">1870</text>
</svg>
</td>
</tr>
</table></div></li></ul></div></li><li class='xr-section-item'><input id='section-d6b323cc-ec81-40e3-87fd-d51ee0e94658' class='xr-section-summary-in' type='checkbox' disabled ><label for='section-d6b323cc-ec81-40e3-87fd-d51ee0e94658' class='xr-section-summary'  title='Expand/collapse section'>Attributes: <span>(0)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><dl class='xr-attrs'></dl></div></li></ul></div></div>



<br>

We can then index this as we would any other `xarray` object

```python
da_HRV_sample = ds['stacked_eumetsat_data'].isel(time=slice(800, 900)).sel(variable='HRV')

da_HRV_sample
```




<div><svg style="position: absolute; width: 0; height: 0; overflow: hidden">
<defs>
<symbol id="icon-database" viewBox="0 0 32 32">
<path d="M16 0c-8.837 0-16 2.239-16 5v4c0 2.761 7.163 5 16 5s16-2.239 16-5v-4c0-2.761-7.163-5-16-5z"></path>
<path d="M16 17c-8.837 0-16-2.239-16-5v6c0 2.761 7.163 5 16 5s16-2.239 16-5v-6c0 2.761-7.163 5-16 5z"></path>
<path d="M16 26c-8.837 0-16-2.239-16-5v6c0 2.761 7.163 5 16 5s16-2.239 16-5v-6c0 2.761-7.163 5-16 5z"></path>
</symbol>
<symbol id="icon-file-text2" viewBox="0 0 32 32">
<path d="M28.681 7.159c-0.694-0.947-1.662-2.053-2.724-3.116s-2.169-2.030-3.116-2.724c-1.612-1.182-2.393-1.319-2.841-1.319h-15.5c-1.378 0-2.5 1.121-2.5 2.5v27c0 1.378 1.122 2.5 2.5 2.5h23c1.378 0 2.5-1.122 2.5-2.5v-19.5c0-0.448-0.137-1.23-1.319-2.841zM24.543 5.457c0.959 0.959 1.712 1.825 2.268 2.543h-4.811v-4.811c0.718 0.556 1.584 1.309 2.543 2.268zM28 29.5c0 0.271-0.229 0.5-0.5 0.5h-23c-0.271 0-0.5-0.229-0.5-0.5v-27c0-0.271 0.229-0.5 0.5-0.5 0 0 15.499-0 15.5 0v7c0 0.552 0.448 1 1 1h7v19.5z"></path>
<path d="M23 26h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
<path d="M23 22h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
<path d="M23 18h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
</symbol>
</defs>
</svg>
<style>/* CSS stylesheet for displaying xarray objects in jupyterlab.
 *
 */

:root {
  --xr-font-color0: var(--jp-content-font-color0, rgba(0, 0, 0, 1));
  --xr-font-color2: var(--jp-content-font-color2, rgba(0, 0, 0, 0.54));
  --xr-font-color3: var(--jp-content-font-color3, rgba(0, 0, 0, 0.38));
  --xr-border-color: var(--jp-border-color2, #e0e0e0);
  --xr-disabled-color: var(--jp-layout-color3, #bdbdbd);
  --xr-background-color: var(--jp-layout-color0, white);
  --xr-background-color-row-even: var(--jp-layout-color1, white);
  --xr-background-color-row-odd: var(--jp-layout-color2, #eeeeee);
}

html[theme=dark],
body.vscode-dark {
  --xr-font-color0: rgba(255, 255, 255, 1);
  --xr-font-color2: rgba(255, 255, 255, 0.54);
  --xr-font-color3: rgba(255, 255, 255, 0.38);
  --xr-border-color: #1F1F1F;
  --xr-disabled-color: #515151;
  --xr-background-color: #111111;
  --xr-background-color-row-even: #111111;
  --xr-background-color-row-odd: #313131;
}

.xr-wrap {
  display: block;
  min-width: 300px;
  max-width: 700px;
}

.xr-text-repr-fallback {
  /* fallback to plain text repr when CSS is not injected (untrusted notebook) */
  display: none;
}

.xr-header {
  padding-top: 6px;
  padding-bottom: 6px;
  margin-bottom: 4px;
  border-bottom: solid 1px var(--xr-border-color);
}

.xr-header > div,
.xr-header > ul {
  display: inline;
  margin-top: 0;
  margin-bottom: 0;
}

.xr-obj-type,
.xr-array-name {
  margin-left: 2px;
  margin-right: 10px;
}

.xr-obj-type {
  color: var(--xr-font-color2);
}

.xr-sections {
  padding-left: 0 !important;
  display: grid;
  grid-template-columns: 150px auto auto 1fr 20px 20px;
}

.xr-section-item {
  display: contents;
}

.xr-section-item input {
  display: none;
}

.xr-section-item input + label {
  color: var(--xr-disabled-color);
}

.xr-section-item input:enabled + label {
  cursor: pointer;
  color: var(--xr-font-color2);
}

.xr-section-item input:enabled + label:hover {
  color: var(--xr-font-color0);
}

.xr-section-summary {
  grid-column: 1;
  color: var(--xr-font-color2);
  font-weight: 500;
}

.xr-section-summary > span {
  display: inline-block;
  padding-left: 0.5em;
}

.xr-section-summary-in:disabled + label {
  color: var(--xr-font-color2);
}

.xr-section-summary-in + label:before {
  display: inline-block;
  content: 'â–º';
  font-size: 11px;
  width: 15px;
  text-align: center;
}

.xr-section-summary-in:disabled + label:before {
  color: var(--xr-disabled-color);
}

.xr-section-summary-in:checked + label:before {
  content: 'â–¼';
}

.xr-section-summary-in:checked + label > span {
  display: none;
}

.xr-section-summary,
.xr-section-inline-details {
  padding-top: 4px;
  padding-bottom: 4px;
}

.xr-section-inline-details {
  grid-column: 2 / -1;
}

.xr-section-details {
  display: none;
  grid-column: 1 / -1;
  margin-bottom: 5px;
}

.xr-section-summary-in:checked ~ .xr-section-details {
  display: contents;
}

.xr-array-wrap {
  grid-column: 1 / -1;
  display: grid;
  grid-template-columns: 20px auto;
}

.xr-array-wrap > label {
  grid-column: 1;
  vertical-align: top;
}

.xr-preview {
  color: var(--xr-font-color3);
}

.xr-array-preview,
.xr-array-data {
  padding: 0 5px !important;
  grid-column: 2;
}

.xr-array-data,
.xr-array-in:checked ~ .xr-array-preview {
  display: none;
}

.xr-array-in:checked ~ .xr-array-data,
.xr-array-preview {
  display: inline-block;
}

.xr-dim-list {
  display: inline-block !important;
  list-style: none;
  padding: 0 !important;
  margin: 0;
}

.xr-dim-list li {
  display: inline-block;
  padding: 0;
  margin: 0;
}

.xr-dim-list:before {
  content: '(';
}

.xr-dim-list:after {
  content: ')';
}

.xr-dim-list li:not(:last-child):after {
  content: ',';
  padding-right: 5px;
}

.xr-has-index {
  font-weight: bold;
}

.xr-var-list,
.xr-var-item {
  display: contents;
}

.xr-var-item > div,
.xr-var-item label,
.xr-var-item > .xr-var-name span {
  background-color: var(--xr-background-color-row-even);
  margin-bottom: 0;
}

.xr-var-item > .xr-var-name:hover span {
  padding-right: 5px;
}

.xr-var-list > li:nth-child(odd) > div,
.xr-var-list > li:nth-child(odd) > label,
.xr-var-list > li:nth-child(odd) > .xr-var-name span {
  background-color: var(--xr-background-color-row-odd);
}

.xr-var-name {
  grid-column: 1;
}

.xr-var-dims {
  grid-column: 2;
}

.xr-var-dtype {
  grid-column: 3;
  text-align: right;
  color: var(--xr-font-color2);
}

.xr-var-preview {
  grid-column: 4;
}

.xr-var-name,
.xr-var-dims,
.xr-var-dtype,
.xr-preview,
.xr-attrs dt {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  padding-right: 10px;
}

.xr-var-name:hover,
.xr-var-dims:hover,
.xr-var-dtype:hover,
.xr-attrs dt:hover {
  overflow: visible;
  width: auto;
  z-index: 1;
}

.xr-var-attrs,
.xr-var-data {
  display: none;
  background-color: var(--xr-background-color) !important;
  padding-bottom: 5px !important;
}

.xr-var-attrs-in:checked ~ .xr-var-attrs,
.xr-var-data-in:checked ~ .xr-var-data {
  display: block;
}

.xr-var-data > table {
  float: right;
}

.xr-var-name span,
.xr-var-data,
.xr-attrs {
  padding-left: 25px !important;
}

.xr-attrs,
.xr-var-attrs,
.xr-var-data {
  grid-column: 1 / -1;
}

dl.xr-attrs {
  padding: 0;
  margin: 0;
  display: grid;
  grid-template-columns: 125px auto;
}

.xr-attrs dt,
.xr-attrs dd {
  padding: 0;
  margin: 0;
  float: left;
  padding-right: 10px;
  width: auto;
}

.xr-attrs dt {
  font-weight: normal;
  grid-column: 1;
}

.xr-attrs dt:hover span {
  display: inline-block;
  background: var(--xr-background-color);
  padding-right: 10px;
}

.xr-attrs dd {
  grid-column: 2;
  white-space: pre-wrap;
  word-break: break-all;
}

.xr-icon-database,
.xr-icon-file-text2 {
  display: inline-block;
  vertical-align: middle;
  width: 1em;
  height: 1.5em !important;
  stroke-width: 0;
  stroke: currentColor;
  fill: currentColor;
}
</style><pre class='xr-text-repr-fallback'>&lt;xarray.DataArray &#x27;stacked_eumetsat_data&#x27; (time: 100, x: 1870, y: 1831)&gt;
dask.array&lt;getitem, shape=(100, 1870, 1831), dtype=int16, chunksize=(36, 1870, 1831), chunktype=numpy.ndarray&gt;
Coordinates:
  * time      (time) datetime64[ns] 2021-01-08T03:09:15 ... 2020-01-01T00:59:16
    variable  &lt;U3 &#x27;HRV&#x27;
  * x         (x) float64 -3.088e+06 -3.084e+06 ... 4.384e+06 4.388e+06
  * y         (y) float64 9.012e+06 9.008e+06 9.004e+06 ... 1.696e+06 1.692e+06
Attributes:
    meta:     {&#x27;orbital_parameters&#x27;: {&#x27;projection_longitude&#x27;: 9.5, &#x27;projectio...</pre><div class='xr-wrap' hidden><div class='xr-header'><div class='xr-obj-type'>xarray.DataArray</div><div class='xr-array-name'>'stacked_eumetsat_data'</div><ul class='xr-dim-list'><li><span class='xr-has-index'>time</span>: 100</li><li><span class='xr-has-index'>x</span>: 1870</li><li><span class='xr-has-index'>y</span>: 1831</li></ul></div><ul class='xr-sections'><li class='xr-section-item'><div class='xr-array-wrap'><input id='section-9f1b9b21-f49c-4876-9fc5-fbfce3f09181' class='xr-array-in' type='checkbox' checked><label for='section-9f1b9b21-f49c-4876-9fc5-fbfce3f09181' title='Show/hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-array-preview xr-preview'><span>dask.array&lt;chunksize=(28, 1870, 1831), meta=np.ndarray&gt;</span></div><div class='xr-array-data'><table>
<tr>
<td>
<table>
  <thead>
    <tr><td> </td><th> Array </th><th> Chunk </th></tr>
  </thead>
  <tbody>
    <tr><th> Bytes </th><td> 684.79 MB </td> <td> 246.53 MB </td></tr>
    <tr><th> Shape </th><td> (100, 1870, 1831) </td> <td> (36, 1870, 1831) </td></tr>
    <tr><th> Count </th><td> 352 Tasks </td><td> 3 Chunks </td></tr>
    <tr><th> Type </th><td> int16 </td><td> numpy.ndarray </td></tr>
  </tbody>
</table>
</td>
<td>
<svg width="198" height="190" style="stroke:rgb(0,0,0);stroke-width:1" >

  <!-- Horizontal lines -->
  <line x1="10" y1="0" x2="30" y2="20" style="stroke-width:2" />
  <line x1="10" y1="120" x2="30" y2="140" style="stroke-width:2" />

  <!-- Vertical lines -->
  <line x1="10" y1="0" x2="10" y2="120" style="stroke-width:2" />
  <line x1="15" y1="5" x2="15" y2="125" />
  <line x1="23" y1="13" x2="23" y2="133" />
  <line x1="30" y1="20" x2="30" y2="140" style="stroke-width:2" />

  <!-- Colored Rectangle -->
  <polygon points="10.0,0.0 30.53650492126805,20.53650492126805 30.53650492126805,140.53650492126806 10.0,120.0" style="fill:#ECB172A0;stroke-width:0"/>

  <!-- Horizontal lines -->
  <line x1="10" y1="0" x2="127" y2="0" style="stroke-width:2" />
  <line x1="15" y1="5" x2="133" y2="5" />
  <line x1="23" y1="13" x2="140" y2="13" />
  <line x1="30" y1="20" x2="148" y2="20" style="stroke-width:2" />

  <!-- Vertical lines -->
  <line x1="10" y1="0" x2="30" y2="20" style="stroke-width:2" />
  <line x1="127" y1="0" x2="148" y2="20" style="stroke-width:2" />

  <!-- Colored Rectangle -->
  <polygon points="10.0,0.0 127.49732620320856,0.0 148.03383112447662,20.53650492126805 30.53650492126805,20.53650492126805" style="fill:#ECB172A0;stroke-width:0"/>

  <!-- Horizontal lines -->
  <line x1="30" y1="20" x2="148" y2="20" style="stroke-width:2" />
  <line x1="30" y1="140" x2="148" y2="140" style="stroke-width:2" />

  <!-- Vertical lines -->
  <line x1="30" y1="20" x2="30" y2="140" style="stroke-width:2" />
  <line x1="148" y1="20" x2="148" y2="140" style="stroke-width:2" />

  <!-- Colored Rectangle -->
  <polygon points="30.53650492126805,20.53650492126805 148.03383112447662,20.53650492126805 148.03383112447662,140.53650492126806 30.53650492126805,140.53650492126806" style="fill:#ECB172A0;stroke-width:0"/>

  <!-- Text -->
  <text x="89.285168" y="160.536505" font-size="1.0rem" font-weight="100" text-anchor="middle" >1831</text>
  <text x="168.033831" y="80.536505" font-size="1.0rem" font-weight="100" text-anchor="middle" transform="rotate(-90,168.033831,80.536505)">1870</text>
  <text x="10.268252" y="150.268252" font-size="1.0rem" font-weight="100" text-anchor="middle" transform="rotate(45,10.268252,150.268252)">100</text>
</svg>
</td>
</tr>
</table></div></div></li><li class='xr-section-item'><input id='section-fc198abd-3af9-40aa-85a0-d03d601783b9' class='xr-section-summary-in' type='checkbox'  checked><label for='section-fc198abd-3af9-40aa-85a0-d03d601783b9' class='xr-section-summary' >Coordinates: <span>(4)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><ul class='xr-var-list'><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>time</span></div><div class='xr-var-dims'>(time)</div><div class='xr-var-dtype'>datetime64[ns]</div><div class='xr-var-preview xr-preview'>2021-01-08T03:09:15 ... 2020-01-...</div><input id='attrs-c9fbb0f3-c333-4b7d-aad3-aa3ca4aa4744' class='xr-var-attrs-in' type='checkbox' disabled><label for='attrs-c9fbb0f3-c333-4b7d-aad3-aa3ca4aa4744' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-8755389a-16d1-44a1-b844-e8bca7217e87' class='xr-var-data-in' type='checkbox'><label for='data-8755389a-16d1-44a1-b844-e8bca7217e87' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'></dl></div><div class='xr-var-data'><pre>array([&#x27;2021-01-08T03:09:15.000000000&#x27;, &#x27;2021-01-08T03:14:15.000000000&#x27;,
       &#x27;2021-01-08T03:19:15.000000000&#x27;, &#x27;2021-01-08T03:24:16.000000000&#x27;,
       &#x27;2021-01-08T03:29:17.000000000&#x27;, &#x27;2021-01-08T03:34:18.000000000&#x27;,
       &#x27;2021-01-08T03:39:18.000000000&#x27;, &#x27;2021-01-08T03:44:18.000000000&#x27;,
       &#x27;2021-01-08T03:49:18.000000000&#x27;, &#x27;2021-01-08T03:54:16.000000000&#x27;,
       &#x27;2021-01-08T03:59:16.000000000&#x27;, &#x27;2021-01-08T04:04:16.000000000&#x27;,
       &#x27;2021-01-08T04:09:16.000000000&#x27;, &#x27;2021-01-08T04:14:16.000000000&#x27;,
       &#x27;2021-01-08T04:19:16.000000000&#x27;, &#x27;2021-01-08T04:24:15.000000000&#x27;,
       &#x27;2021-01-08T04:29:15.000000000&#x27;, &#x27;2021-01-08T04:34:15.000000000&#x27;,
       &#x27;2021-01-08T04:39:15.000000000&#x27;, &#x27;2021-01-08T04:44:15.000000000&#x27;,
       &#x27;2021-01-08T04:49:15.000000000&#x27;, &#x27;2021-01-08T04:54:16.000000000&#x27;,
       &#x27;2021-01-08T04:59:16.000000000&#x27;, &#x27;2021-01-08T05:04:16.000000000&#x27;,
       &#x27;2021-01-08T05:09:15.000000000&#x27;, &#x27;2021-01-08T05:14:15.000000000&#x27;,
       &#x27;2021-01-08T05:19:15.000000000&#x27;, &#x27;2021-01-08T05:24:16.000000000&#x27;,
       &#x27;2021-01-08T05:29:17.000000000&#x27;, &#x27;2021-01-08T05:34:18.000000000&#x27;,
       &#x27;2021-01-08T05:39:18.000000000&#x27;, &#x27;2021-01-08T05:44:18.000000000&#x27;,
       &#x27;2021-01-08T05:49:18.000000000&#x27;, &#x27;2021-01-08T05:54:17.000000000&#x27;,
       &#x27;2021-01-08T05:59:16.000000000&#x27;, &#x27;2021-01-08T06:04:16.000000000&#x27;,
       &#x27;2021-01-08T06:09:16.000000000&#x27;, &#x27;2021-01-08T06:14:16.000000000&#x27;,
       &#x27;2021-01-08T06:19:16.000000000&#x27;, &#x27;2021-01-08T06:24:16.000000000&#x27;,
       &#x27;2021-01-08T06:29:16.000000000&#x27;, &#x27;2021-01-08T06:34:15.000000000&#x27;,
       &#x27;2021-01-08T06:39:15.000000000&#x27;, &#x27;2021-01-08T06:44:15.000000000&#x27;,
       &#x27;2021-01-08T06:49:15.000000000&#x27;, &#x27;2021-01-08T06:54:15.000000000&#x27;,
       &#x27;2021-01-08T06:59:15.000000000&#x27;, &#x27;2021-01-08T07:04:15.000000000&#x27;,
       &#x27;2021-01-08T07:09:15.000000000&#x27;, &#x27;2021-01-08T07:14:14.000000000&#x27;,
       &#x27;2021-01-08T07:19:14.000000000&#x27;, &#x27;2021-01-08T07:24:15.000000000&#x27;,
       &#x27;2021-01-08T07:29:16.000000000&#x27;, &#x27;2021-01-08T07:34:17.000000000&#x27;,
       &#x27;2021-01-08T07:39:17.000000000&#x27;, &#x27;2021-01-08T07:44:17.000000000&#x27;,
       &#x27;2021-01-08T07:49:17.000000000&#x27;, &#x27;2021-01-08T07:54:17.000000000&#x27;,
       &#x27;2021-01-08T07:59:17.000000000&#x27;, &#x27;2021-01-08T08:04:17.000000000&#x27;,
       &#x27;2021-01-08T08:09:17.000000000&#x27;, &#x27;2021-01-08T08:14:16.000000000&#x27;,
       &#x27;2021-01-08T08:19:16.000000000&#x27;, &#x27;2021-01-08T08:24:16.000000000&#x27;,
       &#x27;2021-01-08T08:29:16.000000000&#x27;, &#x27;2021-01-08T08:34:16.000000000&#x27;,
       &#x27;2021-01-08T08:39:16.000000000&#x27;, &#x27;2021-01-08T08:44:16.000000000&#x27;,
       &#x27;2021-01-08T08:49:15.000000000&#x27;, &#x27;2021-01-08T08:09:17.000000000&#x27;,
       &#x27;2021-01-08T08:14:16.000000000&#x27;, &#x27;2021-01-08T08:19:16.000000000&#x27;,
       &#x27;2021-01-08T08:24:16.000000000&#x27;, &#x27;2021-01-08T08:29:16.000000000&#x27;,
       &#x27;2021-01-08T08:34:16.000000000&#x27;, &#x27;2021-01-08T08:39:16.000000000&#x27;,
       &#x27;2021-01-08T08:44:16.000000000&#x27;, &#x27;2021-01-08T08:49:15.000000000&#x27;,
       &#x27;2020-01-01T00:09:15.000000000&#x27;, &#x27;2020-01-01T00:14:16.000000000&#x27;,
       &#x27;2020-01-01T00:19:17.000000000&#x27;, &#x27;2020-01-01T00:24:17.000000000&#x27;,
       &#x27;2020-01-01T00:29:17.000000000&#x27;, &#x27;2020-01-01T00:34:17.000000000&#x27;,
       &#x27;2020-01-01T00:39:17.000000000&#x27;, &#x27;2020-01-01T00:44:16.000000000&#x27;,
       &#x27;2020-01-01T00:49:16.000000000&#x27;, &#x27;2020-01-01T00:54:16.000000000&#x27;,
       &#x27;2020-01-01T00:59:16.000000000&#x27;, &#x27;2020-01-01T00:09:15.000000000&#x27;,
       &#x27;2020-01-01T00:14:16.000000000&#x27;, &#x27;2020-01-01T00:19:17.000000000&#x27;,
       &#x27;2020-01-01T00:24:17.000000000&#x27;, &#x27;2020-01-01T00:29:17.000000000&#x27;,
       &#x27;2020-01-01T00:34:17.000000000&#x27;, &#x27;2020-01-01T00:39:17.000000000&#x27;,
       &#x27;2020-01-01T00:44:16.000000000&#x27;, &#x27;2020-01-01T00:49:16.000000000&#x27;,
       &#x27;2020-01-01T00:54:16.000000000&#x27;, &#x27;2020-01-01T00:59:16.000000000&#x27;],
      dtype=&#x27;datetime64[ns]&#x27;)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span>variable</span></div><div class='xr-var-dims'>()</div><div class='xr-var-dtype'>&lt;U3</div><div class='xr-var-preview xr-preview'>&#x27;HRV&#x27;</div><input id='attrs-5f3ac150-5ce2-4f1b-82e1-c67a97ae3cb3' class='xr-var-attrs-in' type='checkbox' disabled><label for='attrs-5f3ac150-5ce2-4f1b-82e1-c67a97ae3cb3' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-2ea412fa-2a77-4dcc-ac8d-9eff1dada237' class='xr-var-data-in' type='checkbox'><label for='data-2ea412fa-2a77-4dcc-ac8d-9eff1dada237' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'></dl></div><div class='xr-var-data'><pre>array(&#x27;HRV&#x27;, dtype=&#x27;&lt;U3&#x27;)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>x</span></div><div class='xr-var-dims'>(x)</div><div class='xr-var-dtype'>float64</div><div class='xr-var-preview xr-preview'>-3.088e+06 -3.084e+06 ... 4.388e+06</div><input id='attrs-55a6dc7e-c182-490d-8ae8-c609cc9de638' class='xr-var-attrs-in' type='checkbox' disabled><label for='attrs-55a6dc7e-c182-490d-8ae8-c609cc9de638' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-811fce53-3c70-4e3a-8e65-6ac790588c9d' class='xr-var-data-in' type='checkbox'><label for='data-811fce53-3c70-4e3a-8e65-6ac790588c9d' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'></dl></div><div class='xr-var-data'><pre>array([-3088000., -3084000., -3080000., ...,  4380000.,  4384000.,  4388000.])</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>y</span></div><div class='xr-var-dims'>(y)</div><div class='xr-var-dtype'>float64</div><div class='xr-var-preview xr-preview'>9.012e+06 9.008e+06 ... 1.692e+06</div><input id='attrs-a50af94a-78fd-4939-9cef-b882ee38ab67' class='xr-var-attrs-in' type='checkbox' disabled><label for='attrs-a50af94a-78fd-4939-9cef-b882ee38ab67' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-2d326751-a705-40bf-af16-c44e2a97aeda' class='xr-var-data-in' type='checkbox'><label for='data-2d326751-a705-40bf-af16-c44e2a97aeda' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'></dl></div><div class='xr-var-data'><pre>array([9012000., 9008000., 9004000., ..., 1700000., 1696000., 1692000.])</pre></div></li></ul></div></li><li class='xr-section-item'><input id='section-9995dc85-0224-48c3-b41c-c06341f4a719' class='xr-section-summary-in' type='checkbox'  checked><label for='section-9995dc85-0224-48c3-b41c-c06341f4a719' class='xr-section-summary' >Attributes: <span>(1)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><dl class='xr-attrs'><dt><span>meta :</span></dt><dd>{&#x27;orbital_parameters&#x27;: {&#x27;projection_longitude&#x27;: 9.5, &#x27;projection_latitude&#x27;: 0.0, &#x27;projection_altitude&#x27;: 35785831.0}, &#x27;sun_earth_distance_correction_applied&#x27;: True, &#x27;sun_earth_distance_correction_factor&#x27;: 0.9680361623200268, &#x27;units&#x27;: &#x27;%&#x27;, &#x27;wavelength&#x27;: WavelengthRange(min=0.5, central=0.7, max=0.9, unit=&#x27;Âµm&#x27;), &#x27;standard_name&#x27;: &#x27;toa_bidirectional_reflectance&#x27;, &#x27;platform_name&#x27;: &#x27;Meteosat-10&#x27;, &#x27;sensor&#x27;: &#x27;seviri&#x27;, &#x27;start_time&#x27;: datetime.datetime(2020, 12, 16, 18, 35, 8, 985163), &#x27;end_time&#x27;: datetime.datetime(2020, 12, 16, 18, 40, 8, 829133), &#x27;area&#x27;: Area ID: geos_seviri_hrv
Description: SEVIRI high resolution channel area
Projection ID: seviri_hrv
Projection: {&#x27;a&#x27;: &#x27;6378169&#x27;, &#x27;h&#x27;: &#x27;35785831&#x27;, &#x27;lon_0&#x27;: &#x27;9.5&#x27;, &#x27;no_defs&#x27;: &#x27;None&#x27;, &#x27;proj&#x27;: &#x27;geos&#x27;, &#x27;rf&#x27;: &#x27;295.488065897014&#x27;, &#x27;type&#x27;: &#x27;crs&#x27;, &#x27;units&#x27;: &#x27;m&#x27;, &#x27;x_0&#x27;: &#x27;0&#x27;, &#x27;y_0&#x27;: &#x27;0&#x27;}
Number of columns: 5568
Number of rows: 4176
Area extent: (2792875.1692, 5571248.3904, -2775872.8853, 1394687.3495), &#x27;name&#x27;: &#x27;HRV&#x27;, &#x27;resolution&#x27;: 1000.134348869, &#x27;calibration&#x27;: &#x27;reflectance&#x27;, &#x27;modifiers&#x27;: (), &#x27;_satpy_id&#x27;: DataID(name=&#x27;HRV&#x27;, wavelength=WavelengthRange(min=0.5, central=0.7, max=0.9, unit=&#x27;Âµm&#x27;), resolution=1000.134348869, calibration=&lt;calibration.reflectance&gt;, modifiers=()), &#x27;ancillary_variables&#x27;: []}</dd></dl></div></li></ul></div></div>



<br>

The coordinates are for a Transverse Mercator projection, we'll create a helper function that converts latitude and longitude into this coordinate system

```python
def convert_lon_lat_to_crs_coords(lon=0.1, lat=51.5):
    new_coords_point = (gpd.GeoDataFrame(geometry=[shapely.geometry.Point(lon, lat)], crs='EPSG:4326')
                        .to_crs('EPSG:3857')
                        .loc[0, 'geometry'])
    
    new_coords = (new_coords_point.x, new_coords_point.y)
    
    return new_coords

x, y = convert_lon_lat_to_crs_coords(lon=1, lat=50)

x, y
```




    (111319.49079327357, 6446275.841017158)



<br>

We'll now interpolate the data at this location and extract a time-series of the HRV intensity

```python
s_HRV = da_HRV_sample.interp(x=x, y=y).sortby('time').to_series()

s_HRV.plot()
```

    C:\Users\Ayrto\anaconda3\envs\satip_dev\lib\site-packages\xarray\core\indexing.py:1369: PerformanceWarning: Slicing is producing a large chunk. To accept the large
    chunk and silence this warning, set the option
        >>> with dask.config.set(**{'array.slicing.split_large_chunks': False}):
        ...     array[indexer]
    
    To avoid creating the large chunks, set the option
        >>> with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        ...     array[indexer]
      return self.array[key]
    




    <AxesSubplot:xlabel='time'>




![png](img/nbs/output_10_2.png)


<br>

We can also visualise the intensity distribution

```python
sns.histplot(s_HRV)
```




    <AxesSubplot:xlabel='stacked_eumetsat_data', ylabel='Count'>




![png](img/nbs/output_12_1.png)
