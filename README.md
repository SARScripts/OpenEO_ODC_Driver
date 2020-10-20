# OpenEO_ODC_Driver
OpenEO backend written in Python based on OpenDataCube, xarray and dask

## Step 1: Clone the repository
```
git clone https://github.com/SARScripts/openeo_odc_driver.git
cd openeo_odc_driver
```
## Step 2: Prepare the python environment
```
conda env create -f environment.yml
conda activate openeo_odc_driver
git clone https://github.com/clausmichele/openeo-pg-parser-python.git
cd openeo-pg-parser-python
pip install .
```
If the environment creation step fails please create a Python 3.7 environment environment with the following libraries:
gdal, xarray, rioxarray, dask, numpy, scipy, opencv and their dependencies.
## Step 3: Test with local datacube
```
python main.py ./process_graphs/EVI_L1C_D22.json --local 1
```

## Implemented OpenEO processes. See the official API [here](https://processes.openeo.org/)

<details><summary>aggregate & resample</summary>
- resample_cube_temporal
- resample_spatial
</details>
<details><summary>arrays</summary>
- array_element

</details>
<details><summary>climatology</summary>
- climatological_normal (only monthly frquency at the moment)
- anomaly (only monthly frquency at the moment)

</details>
<details><summary>comparison</summary>
- if
- lt
- lte
- gt
- gte
- eq
- neq
  
</details>
<details><summary>cubes</summary>
- load_collection
- save_result (PNG,GTIFF,NETCDF)
- reduce_dimension (dimensions: t (or temporal), bands)
- filter_bands
- filter_temporal
- rename_labels
- merge_cubes
- apply
</details>
<details><summary>development</summary></details>
<details><summary>export</summary></details>
<details><summary>filter</summary></details>
<details><summary>import</summary></details>
<details><summary>logic</summary>
- and
- or
  
</details>
<details><summary>masks</summary>
- mask

</details>
<details><summary>math</summary>
- multiply
- divide
- subtract
- add
- sum
- product
- sqrt
- normalized_difference
- min
- max
- mean
- median
- power
- absolute
- linear_scale_range
</details>
<details><summary>reducer</summary></details>
<details><summary>sorting</summary></details>
<details><summary>texts</summary></details>
<details><summary>udf</summary></details>
<details><summary>vegetation indices</summary></details>












# Experimental processes
- aggregate_spatial_window




