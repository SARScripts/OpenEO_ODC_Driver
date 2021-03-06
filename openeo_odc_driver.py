# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   10/02/2021

# Import necessary libraries
# System
import os
import sys
from time import time
from datetime import datetime
import json
import uuid
# Math
import math
import numpy as np
from scipy.interpolate import griddata
from scipy.spatial import Delaunay
from scipy.interpolate import LinearNDInterpolator
from scipy.optimize import curve_fit
# Geography
from osgeo import gdal, osr
from pyproj import Proj, transform, Transformer, CRS
import rasterio
import rasterio.features
# Datacubes and databases
import datacube
import xarray as xr
import rioxarray
import pandas as pd
# Parallel Computing
import dask
from dask.distributed import Client
from dask import delayed
# openEO & SAR2Cube specific
from openeo_pg_parser.translate import translate_process_graph
from openEO_error_messages import *
from odc_wrapper import Odc
try:
    from sar2cube_utils import *
except:
    pass

DASK_SCHEDULER_ADDRESS = ''
TMP_FOLDER_PATH        = '' # Has to be accessible from all the Dask workers
OPENEO_PROCESSES       = 'https://openeo.eurac.edu/processes' # The processes available at the back-end
client = Client(DASK_SCHEDULER_ADDRESS)
    

class OpenEO():
    def __init__(self,jsonProcessGraph):
        self.jsonProcessGraph = jsonProcessGraph
        self.jobId = jsonProcessGraph['id']
        self.data = None
        self.listExecutedIds = []
        self.partialResults = {}
        self.crs = None
        self.bands = None
        self.graph = translate_process_graph(jsonProcessGraph,process_defs=OPENEO_PROCESSES).sort(by='result')
        self.outFormat = None
        self.mimeType = None
        self.i = 0
        if self.jobId == "None":
            self.tmpFolderPath = TMP_FOLDER_PATH + str(uuid.uuid4())
        else:
            self.tmpFolderPath = TMP_FOLDER_PATH + self.jobId # If it is a batch job, there will be a field with it's id
        self.sar2cubeCollection = False
        self.fitCurveFunctionString = ""
        try:
            os.mkdir(self.tmpFolderPath)
        except:
            pass
        for i in range(0,len(self.graph)+1):
            if not self.process_node(i):
                print('[*] Processing finished!')
                break

    def process_node(self,i):
        node = self.graph[i]
        processName = node.process_id
        print("Process id: {} Process name: {}".format(node.id,processName))
        try:
            if processName == 'load_collection':
                defaultTimeStart = '1970-01-01'
                defaultTimeEnd   = str(datetime.now()).split(' ')[0] # Today is the default date for timeEnd, to include all the dates if not specified
                timeStart        = defaultTimeStart
                timeEnd          = defaultTimeEnd
                collection       = None
                lowLat           = None
                highLat          = None
                lowLon           = None
                highLon          = None
                bands            = None # List of bands
                resolutions      = None # Tuple
                outputCrs        = None
                resamplingMethod = None
                polygon          = None
                if 'bands' in node.arguments:
                    bands = node.arguments['bands']
                    if bands == []: bands = None

                collection = node.arguments['id'] # The datacube we have to load
                if collection is None:
                    raise Exception('[!] You must provide a collection which provides the data!')
                self.sar2cubeCollection = ('SAR2Cube' in collection) # Return True if it's a SAR2Cube collection
        
                if node.arguments['temporal_extent'] is not None:
                    timeStart  = node.arguments['temporal_extent'][0]
                    timeEnd    = node.arguments['temporal_extent'][1]

                # If there is a bounding-box or a polygon we set the variables, otherwise we pass the defaults
                if 'spatial_extent' in node.arguments and node.arguments['spatial_extent'] is not None:
                    if 'south' in node.arguments['spatial_extent'] and \
                       'north' in node.arguments['spatial_extent'] and \
                       'east'  in node.arguments['spatial_extent'] and \
                       'west'  in node.arguments['spatial_extent']:
                        lowLat     = node.arguments['spatial_extent']['south']
                        highLat    = node.arguments['spatial_extent']['north']
                        lowLon     = node.arguments['spatial_extent']['east']
                        highLon    = node.arguments['spatial_extent']['west']

                    elif 'coordinates' in node.arguments['spatial_extent']:
                        # Pass coordinates to odc and process them there
                        polygon = node.arguments['spatial_extent']['coordinates']

                for n in self.graph: # Let's look for resample_spatial nodes
                    parentID = 0
                    if n.content['process_id'] == 'resample_spatial':
                        for n_0 in n.dependencies: # Check if the (or one of the) resample_spatial process is related to this load_collection
                            parentID = n_0.id
                            continue
                        if parentID == node.id: # The found resample_spatial comes right after the current load_collection, let's apply the resampling to the query
                            if 'resolution' in n.arguments:
                                res = n.arguments['resolution']
                                if isinstance(res,float) or isinstance(res,int):
                                    resolutions = (res,res)
                                elif len(res) == 2:
                                    resolutions = (res[0],res[1])
                                else:
                                    print('error')

                            if 'projection' in n.arguments:
                                if n.arguments['projection'] is not None:
                                    projection = n.arguments['projection']
                                    if isinstance(projection,int):           # Check if it's an EPSG code and append 'epsg:' to it, without ODC returns an error
                                        ## TODO: make other projections available
                                        projection = 'epsg:' + str(projection)
                                    else:
                                        print('This type of reprojection is not yet implemented')
                                    outputCrs = projection

                            if 'method' in n.arguments:
                                resamplingMethod = n.arguments['method']

                odc = Odc(collections=collection,timeStart=timeStart,timeEnd=timeEnd,bands=bands,lowLat=lowLat,highLat=highLat,lowLon=lowLon,highLon=highLon,resolutions=resolutions,outputCrs=outputCrs,polygon=polygon,resamplingMethod=resamplingMethod)
                if len(odc.data) == 0:
                    raise Exception("load_collection returned an empty dataset, please check the requested bands, spatial and temporal extent.")
                self.partialResults[node.id] = odc.data.to_array()
                self.crs = odc.data.crs             # We store the data CRS separately, because it's a metadata we may lose it in the processing
                print(self.partialResults[node.id]) # The loaded data, stored in a dictionary with the id of the node that has generated it
                        
            if processName == 'resample_spatial':
                source = node.arguments['data']['from_node']
                self.partialResults[node.id] = self.partialResults[source]
            
            # The following code block handles the fit_curve and predict_curve processes, where we need to convert a process graph into a callable python function
            if node.parent_process is not None:
                if (node.parent_process.process_id=='fit_curve' or node.parent_process.process_id=='predict_curve'):
                    if processName in ['pi']:
                        if processName == 'pi':
                            self.partialResults[node.id] =  "np.pi"
                    if processName in ['array_element']:
                        self.partialResults[node.id] = 'a' + str(node.arguments['index'])
                    if processName in ['multiply','divide','subtract','add','sin','cos']:
                        x = None
                        y = None
                        source = None
                        if 'x' in node.arguments and node.arguments['x'] is not None:
                            if isinstance(node.arguments['x'],float) or isinstance(node.arguments['x'],int): # We have to distinguish when the input data is a number or a datacube from a previous process
                                x = str(node.arguments['x'])
                            else:
                                if 'from_node' in node.arguments['x']:
                                    source = node.arguments['x']['from_node']
                                elif 'from_parameter' in node.arguments['x']:
                                    x = 'x'
                                if source is not None:
                                    x = self.partialResults[source]
                        source = None        
                        if 'y' in node.arguments and node.arguments['y'] is not None:
                            if isinstance(node.arguments['y'],float) or isinstance(node.arguments['y'],int):
                                y = str(node.arguments['y'])
                            else:
                                if 'from_node' in node.arguments['y']:
                                    source = node.arguments['y']['from_node']
                                elif 'from_parameter' in node.arguments['y']:
                                    y = 'x'
                                if source is not None:
                                    y = self.partialResults[source]
                        if processName == 'multiply':
                            if x is None or y is None:
                                raise Exception(MultiplicandMissing)
                            else:
                                self.partialResults[node.id] = "(" + x  + "*" + y + ")"
                        elif processName == 'divide':
                            if y==0:
                                raise Exception(DivisionByZero)
                            else:
                                self.partialResults[node.id] = "(" + x + "/" + y + ")"
                        elif processName == 'subtract':
                            self.partialResults[node.id] = "(" + x + "-" + y + ")"
                        elif processName == 'add':
                            self.partialResults[node.id] = "(" + x + "+" + y + ")"
                        elif processName == 'sin':
                            self.partialResults[node.id] = "np.sin(" + x + ")"
                        elif processName == 'cos':
                            self.partialResults[node.id] = "np.cos(" + x + ")"
                    return 1
                
                
            if processName == 'resample_cube_spatial':
                target = node.arguments['target']['from_node']
                source = node.arguments['data']['from_node']
                method = node.arguments['method']
                if method is None:
                    method = 'nearest'
                if method == 'near':
                    method = 'nearest'
                try:
                    import odc.algo
                    self.partialResults[node.id] = odc.algo._warp.xr_reproject(self.partialResults[source].compute(),self.partialResults[target].geobox,resampling=method).compute()
                except Exception as e:
                    print(e)
                    try:
                        self.partialResults[node.id] = self.partialResults[source].rio.reproject_match(self.partialResults[target],resampling=method)
                    except Exception as e:
                        raise Exception("ODC Error in process: ",processName,'\n Full Python log:\n',str(e))

            if processName == 'resample_cube_temporal':
                target = node.arguments['target']['from_node']
                source = node.arguments['data']['from_node']
                def nearest(items, pivot):
                    return min(items, key=lambda x: abs(x - pivot))
                def resample_temporal(sourceCube,targetCube):
                    # Find in sourceCube the closest dates to tergetCube
                    newTime = []
                    for i,targetTime in enumerate(targetCube.time):
                        nearT = nearest(sourceCube.time.values,targetTime.values)
                        if i==0:
                            tmp = sourceCube.loc[dict(time=nearT)]
                        else:
                            tmp = xr.concat([tmp,sourceCube.loc[dict(time=nearT)]], dim='time')
                    tmp['time'] = targetCube.time
                    return tmp
                self.partialResults[node.id] = resample_temporal(self.partialResults[source],self.partialResults[target])

            if processName in ['multiply','divide','subtract','add','lt','lte','gt','gte','eq','neq']:
                x = None
                y = None
                source = None
                if 'x' in node.arguments and node.arguments['x'] is not None:
                    if isinstance(node.arguments['x'],float) or isinstance(node.arguments['x'],int): # We have to distinguish when the input data is a number or a datacube from a previous process
                        x = node.arguments['x']
                    else:
                    if 'from_node' in node.arguments['x']:
                        source = node.arguments['x']['from_node']
                    elif 'from_parameter' in node.arguments['x']:
                        if node.parent_process.process_id == 'merge_cubes':
                                source = node.parent_process.arguments['cube1']['from_node']
                            else:
                                source = node.parent_process.arguments['data']['from_node']
                        if source is not None:
                            x = self.partialResults[source]
                if 'y' in node.arguments and node.arguments['y'] is not None:
                    if isinstance(node.arguments['y'],float) or isinstance(node.arguments['y'],int):
                        y = node.arguments['y']
                    else:
                    if 'from_node' in node.arguments['y']:
                        source = node.arguments['y']['from_node']
                    elif 'from_parameter' in node.arguments['y']:
                        if node.parent_process.process_id == 'merge_cubes':
                                source = node.parent_process.arguments['cube2']['from_node']
                            else:
                                source = node.parent_process.arguments['data']['from_node']
                        if source is not None:
                            y = self.partialResults[source]

                if processName == 'multiply':
                    if x is None or y is None:
                        raise Exception(MultiplicandMissing)
                    else:
                        try:
                            self.partialResults[node.id] = (x * y).astype(np.float32)
                        except:
                            if hasattr(x,'chunks'):
                                x = x.compute()
                            if hasattr(y,'chunks'):
                                y = y.compute()
                            try:
                                self.partialResults[node.id] = (x * y).astype(np.float32).chunk()
                            except Exception as e:
                                raise e
                elif processName == 'divide':
                    if y==0:
                        raise Exception(DivisionByZero)
                    else:
                        try:
                            self.partialResults[node.id] = (x / y).astype(np.float32)
                        except:
                            if hasattr(x,'chunks'):
                                x = x.compute()
                            if hasattr(y,'chunks'):
                                y = y.compute()
                            try:
                                self.partialResults[node.id] = (x / y).astype(np.float32).chunk()
                            except Exception as e:
                                raise e
                elif processName == 'subtract':
                    try:
                        self.partialResults[node.id] = (x - y).astype(np.float32)
                    except:
                        if hasattr(x,'chunks'):
                            x = x.compute()
                        if hasattr(y,'chunks'):
                            y = y.compute()
                        try:
                            self.partialResults[node.id] = (x - y).astype(np.float32).chunk()
                        except Exception as e:
                            raise e
                elif processName == 'add':
                    try:
                        self.partialResults[node.id] = (x + y).astype(np.float32)
                    except:
                        if hasattr(x,'chunks'):
                            x = x.compute()
                        if hasattr(y,'chunks'):
                            y = y.compute()
                        try:
                            self.partialResults[node.id] = (x + y).astype(np.float32).chunk()
                        except Exception as e:
                            raise e
                elif processName == 'lt':
                    self.partialResults[node.id] = x < y
                elif processName == 'lte':
                    self.partialResults[node.id] = x <= y
                elif processName == 'gt':
                    self.partialResults[node.id] = x > y
                elif processName == 'gte':
                    self.partialResults[node.id] = x >= y
                elif processName == 'eq':
                    self.partialResults[node.id] = x == y
                elif processName == 'neq':
                    self.partialResults[node.id] = x != y       

            if processName == 'not':
                if isinstance(node.arguments['x'],float) or isinstance(node.arguments['x'],int): # We have to distinguish when the input data is a number or a datacube from a previous process
                    x = node.arguments['x']
                else:
                    if 'from_node' in node.arguments['x']:
                        source = node.arguments['x']['from_node']
                    elif 'from_parameter' in node.arguments['x']:
                        source = node.parent_process.arguments['data']['from_node']
                    x = self.partialResults[source]
                self.partialResults[node.id] = np.logical_not(x)

            if processName == 'sum':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    source = node.arguments['data']['from_node']
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = 'pad').sum()
                else:
                    x = 0
                    for i,d in enumerate(node.arguments['data']):
                        if isinstance(d,float) or isinstance(d,int):         # We have to distinguish when the input data is a number or a datacube from a previous process
                            x = d
                        else:
                            if 'from_node' in d:
                                source = d['from_node']
                            elif 'from_parameter' in d:
                                source = node.parent_process.arguments['data']['from_node']
                            x = self.partialResults[source]
                        if i==0: self.partialResults[node.id] = x
                        else: self.partialResults[node.id] += x

            if processName == 'product':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    source = node.arguments['data']['from_node']
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = 'pad').prod()
                else:
                    x = 0
                    for i,d in enumerate(node.arguments['data']):
                        if isinstance(d,float) or isinstance(d,int):        # We have to distinguish when the input data is a number or a datacube from a previous process
                            x = d
                        else:
                            if 'from_node' in d:
                                source = d['from_node']
                            elif 'from_parameter' in d:
                                source = node.parent_process.arguments['data']['from_node']
                            x = self.partialResults[source]
                        if i==0: self.partialResults[node.id] = x
                        else: self.partialResults[node.id] *= x

            if processName == 'sqrt':
                x = node.arguments['x']
                if isinstance(x,float) or isinstance(x,int):        # We have to distinguish when the input data is a number or a datacube from a previous process
                    self.partialResults[node.id] = np.sqrt(x)
                else:
                    if 'from_node' in node.arguments['x']:
                        source = node.arguments['x']['from_node']
                    elif 'from_parameter' in node.arguments['x']:
                        source = node.parent_process.arguments['data']['from_node']
                    self.partialResults[node.id] = np.sqrt(self.partialResults[source])

            if processName == 'and':
                x = node.arguments['x']['from_node']
                y = node.arguments['y']['from_node']
                self.partialResults[node.id] = np.bitwise_and(self.partialResults[x],self.partialResults[y])

            if processName == 'or':
                x = node.arguments['x']['from_node']
                y = node.arguments['y']['from_node']
                self.partialResults[node.id] = np.bitwise_or(self.partialResults[x],self.partialResults[y])

            if processName == 'array_element':
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                noLabel = 1
                if 'label' in node.arguments:
                    if node.arguments['label'] is not None:
                        bandLabel = node.arguments['label']
                        noLabel = 0
                        self.partialResults[node.id] = self.partialResults[source].loc[dict(variable=bandLabel)].drop('variable')
                if 'index' in node.arguments and noLabel:
                    index = node.arguments['index']
                    self.partialResults[node.id] = self.partialResults[source][index]            

            if processName == 'normalized_difference':
                def normalized_difference(x,y):
                    return (x-y)/(x+y)
                xSource = (node.arguments['x']['from_node'])
                ySource = (node.arguments['y']['from_node'])
                self.partialResults[node.id] = normalized_difference(self.partialResults[xSource],self.partialResults[ySource])

            if processName == 'reduce_dimension':
                source = node.arguments['reducer']['from_node']
                self.partialResults[node.id] = self.partialResults[source]

            if processName == 'aggregate_spatial_window':
                source = node.arguments['reducer']['from_node']
                self.partialResults[node.id] = self.partialResults[source]

            if processName == 'max':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = 'pad').max()
                else:
                    dim = parent.dimension
                    if dim in ['t','temporal','DATE'] and 'time' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].max('time')
                    elif dim in ['bands'] and 'variable' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].max('variable')
                    elif dim in ['x'] and 'x' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].max('x')
                    elif dim in ['y'] and 'y' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].max('y')
                    else:
                        self.partialResults[node.id] = self.partialResults[source]
                        print('[!] Dimension {} not available in the current data.'.format(dim))

            if processName == 'min':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    ## TODO get pad, trim parameter from arguments
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = 'pad').min()
                else:
                    dim = parent.dimension
                    if dim in ['t','temporal','DATE'] and 'time' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].min('time')
                    elif dim in ['bands'] and 'variable' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].min('variable')
                    elif dim in ['x'] and 'x' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].min('x')
                    elif dim in ['y'] and 'y' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].min('y')
                    else:
                        self.partialResults[node.id] = self.partialResults[source]
                        print('[!] Dimension {} not available in the current data.'.format(dim))

            if processName == 'mean':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                self.partialResults[source] = self.partialResults[source].astype(np.float32)
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = 'pad').mean()
                else:
                    dim = parent.dimension
                    if dim in ['t','temporal','DATE'] and 'time' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].mean('time')
                    elif dim in ['bands'] and 'variable' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].mean('variable')
                    elif dim in ['x'] and 'x' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].mean('x')
                    elif dim in ['y'] and 'y' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].mean('y')
                    else:
                        self.partialResults[node.id] = self.partialResults[source]
                        print('[!] Dimension {} not available in the current data.'.format(dim))

            if processName == 'median':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                self.partialResults[source] = self.partialResults[source].astype(np.float32)
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = 'pad').median()
                else:
                    dim = parent.dimension
                    if dim in ['t','temporal','DATE'] and 'time' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].median('time')
                    elif dim in ['bands'] and 'variable' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].median('variable')
                    elif dim in ['x'] and 'x' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].median('x')
                    elif dim in ['y'] and 'y' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].median('y')
                    else:
                        self.partialResults[node.id] = self.partialResults[source]
                        print('[!] Dimension {} not available in the current data.'.format(dim))
                     
            if processName == 'sd':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = 'pad').std()
                else:
                    dim = parent.dimension
                    if dim in ['t','temporal','DATE'] and 'time' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].std('time')
                    elif dim in ['bands'] and 'variable' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].std('variable')
                    elif dim in ['x'] and 'x' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].std('x')
                    elif dim in ['y'] and 'y' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].std('y')
                    else:
                        self.partialResults[node.id] = self.partialResults[source]
                        print('[!] Dimension {} not available in the current data.'.format(dim))
                        
            if processName == 'power':
                dim = node.arguments['base']
                if isinstance(node.arguments['base'],float) or isinstance(node.arguments['base'],int): # We have to distinguish when the input data is a number or a datacube from a previous process
                    x = node.arguments['base']
                else:
                    x = self.partialResults[node.arguments['base']['from_node']]
                self.partialResults[node.id] = (x**node.arguments['p']).astype(np.float32)

            if processName == 'absolute':
                source = node.arguments['x']['from_node']
                self.partialResults[node.id] = abs(self.partialResults[source])

            if processName == 'linear_scale_range':
                parent = node.parent_process # I need to read the parent apply process
                if 'from_node' in parent.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                inputMin = node.arguments['inputMin']
                inputMax = node.arguments['inputMax']
                outputMax = node.arguments['outputMax']
                outputMin = 0
                if 'outputMin' in node.arguments:
                    outputMin = node.arguments['outputMin']
                try:
                    tmp = self.partialResults[source].clip(inputMin,inputMax)
                except:
                    try:
                        tmp = self.partialResults[source].compute()
                        tmp = tmp.clip(inputMin,inputMax)
                    except Exception as e:
                        raise e
                self.partialResults[node.id] = ((tmp - inputMin) / (inputMax - inputMin)) * (outputMax - outputMin) + outputMin

            if processName == 'clip':
                parent = node.parent_process # I need to read the parent apply process
                if 'from_node' in parent.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                outputMax = node.arguments['max']
                outputMin = 0
                if 'min' in node.arguments:
                    outputMin = node.arguments['min']
                try:
                    tmp = self.partialResults[source].clip(outputMin,outputMax)
                except:
                    try:
                        tmp = self.partialResults[source].compute()
                        tmp = tmp.fillna(0).clip(outputMin,outputMax).chunk()
                    except Exception as e:
                        raise e
                self.partialResults[node.id] = tmp

            if processName == 'filter_temporal':
                timeStart = node.arguments['extent'][0]
                timeEnd = node.arguments['extent'][1]
                if len(timeStart.split('T')) > 1:                # xarray slicing operation doesn't work with dates in the format 2017-05-01T00:00:00Z but only 2017-05-01
                    timeStart = timeStart.split('T')[0]
                if len(timeEnd.split('T')) > 1:
                    timeEnd = timeEnd.split('T')[0]
                source = node.arguments['data']['from_node']
                self.partialResults[node.id] = self.partialResults[source].loc[dict(time=slice(timeStart,timeEnd))]

            if processName == 'filter_bands':
                bandsToKeep = node.arguments['bands']
                source = node.arguments['data']['from_node']
                self.partialResults[node.id]  = self.partialResults[source].loc[dict(variable=bandsToKeep)]

            if processName == 'rename_labels':
                source = node.arguments['data']['from_node']
                # We need to create a new dataset, with time dimension if present.
                if 'time' in self.partialResults[source].coords:
                    tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x,'time':self.partialResults[source].time})
                else:
                    tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x})
                for i in range(len(node.arguments['target'])):
                    label_target = node.arguments['target'][i]
                    if (len(node.arguments['source']))>0:
                        label_source = node.arguments['source'][i]
                        tmp = tmp.assign({label_target:self.partialResults[source].loc[dict(variable=label_source)]})
                    else:
                        if 'variable' in self.partialResults[source].coords:
                            tmp = tmp.assign({label_target:self.partialResults[source][i]})
                        else:
                            tmp = tmp.assign({label_target:self.partialResults[source]})
                self.partialResults[node.id] = tmp.to_array()

            if processName == 'add_dimension':
                source = node.arguments['data']['from_node']
                try:
                    len(self.partialResults[source].coords['time'])
                    tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x,'time':self.partialResults[source].time})
                except:
                    tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x})
                label_target = node.arguments['label']
                tmp = tmp.assign({label_target:self.partialResults[source]})
                self.partialResults[node.id] = tmp.to_array()

            if processName == 'merge_cubes':
                # x,y,t + x,y (requires overlap resolver)
                # x,y,bands + x,y (requires overlap resolver)
                # x,y,t,bands + x,y (requires overlap resolver)
                # x,y,t,bands + x,y,bands falls into multiple categories. Depending on how the bands are structured. If they have the same bands, they need an overlap resolver. Bands that do only exist in one the cubes, get concatenated.
                
                ## dimensions check
                cube1 = node.arguments['cube1']['from_node']
                cube2 = node.arguments['cube2']['from_node']
                cube1_dims = self.partialResults[cube1].dims
                cube2_dims = self.partialResults[cube2].dims
                #dimensions are x, y, time, variable.
                if hasattr(cube1,'chunks') or hasattr(cube2,'chunks'):
                    # We need to re-chunk the data to avoid errors merging chunked and not chunked data
                    cube1 = cube1.chunk()
                    cube2 = cube2.chunk()
                print("dimensions are x, y, time, variable.")
                if cube1_dims == cube2_dims:
                    # We need to check if they have bands
                    if 'variable' in cube1_dims and 'variable' in cube2_dims:
                        # We need to check if the bands are different or there are some common ones
                        print("We need to check if the bands are different or there are some common ones")
                        cube1_bands = self.partialResults[cube1]['variable'].values
                        cube2_bands = self.partialResults[cube2]['variable'].values
                        # Simple case: same bands in both datacubes
                        print("Simple case: same bands in both datacubes")
                        if (cube1_bands == cube2_bands).all():
                            print("We need to check if the timestep are different, if yes we can merge directly")
                            if (self.partialResults[cube1].time.values != self.partialResults[cube2].time.values).all():
                                self.partialResults[node.id] = xr.concat([self.partialResults[cube1],self.partialResults[cube2]],dim='time')
                            else:
                                #Overlap resolver required
                                print("Overlap resolver required")
                                if 'overlap_resolver' in node.arguments:
                                    if 'from_node' in node.arguments['overlap_resolver']:
                                        source = node.arguments['overlap_resolver']['from_node']
                                        self.partialResults[node.id] = self.partialResults[source]
                                    else:
                                        raise Exception(OverlapResolverMissing)
                                else:
                                    raise Exception(OverlapResolverMissing)
                        else:
                            #Check if at least one band is in common
                            print("Check if at least one band is in common")
                            common_band = False
                            for v in cube1_bands:
                                if v in cube2_bands: common_band=True
                            if common_band:
                                #Complicate case where overlap_resolver has to be appliead only on one or some bands
                                print("Complicate case where overlap_resolver has to be appliead only on one or some bands")
                                raise Exception("[!] Trying to merge two datacubes with one or more common bands, not supported yet!")
                            else:
                                #Simple case where all the bands are different and we can just concatenate the datacubes without overlap resolver
                                print("Simple case where all the bands are different and we can just concatenate the datacubes without overlap resolver")
                                ds1 = self.partialResults[cube1]
                                ds2 = self.partialResults[cube2]
                                self.partialResults[node.id] = xr.concat([ds1,ds2],dim='variable')
                    else:
                        # We don't have bands, dimensions are either x,y or x,y,t for both datacubes
                        print("We don't have bands, dimensions are either x,y or x,y,t for both datacubes")
                        if 'time' in cube1_dims:
                            # TODO: check if the timesteps are the same, if yes use overlap resolver
                            cube1_time = self.partialResults[cube1].time.values
                            cube2_time = self.partialResults[cube2].time.values
                            if (cube1_time == cube2_time).all():
                                #Overlap resolver required
                                print("Overlap resolver required")
                                if 'overlap_resolver' in node.arguments:
                                    try:
                                        source = node.arguments['overlap_resolver']['from_node']
                                        self.partialResults[node.id] = self.partialResults[source]
                                    except:
                                        raise Exception(OverlapResolverMissing)
                                else:
                                    raise Exception(OverlapResolverMissing)
                            ## TODO: Case when only some timesteps are the same
                            ## TODO: Case when no timesteps are in common
                        else:
                            # We have only x,y (or maybe only x or only y)
                            print("We have only x,y (or maybe only x or only y)")
                            # Overlap resolver required
                            if 'overlap_resolver' in node.arguments and 'from_node' in node.arguments['overlap_resolver']:
                                source = node.arguments['overlap_resolver']['from_node']
                                self.partialResults[node.id] = self.partialResults[source]
                            else:
                                raise Exception(OverlapResolverMissing)
                else:
                    # CASE x,y,t + x,y (requires overlap resolver)
                    if 'time' in cube1_dims or 'time' in cube2_dims:
                            if 'x' and 'y' in cube1_dims and 'x' and 'y' in cube2_dims:
                                # We need to check if they have bands, if yes is still not possible
                                if 'variable' not in cube1_dims and 'variable' not in cube2_dims:
                                    #Overlap resolver required
                                    print("Overlap resolver required")
                                    if 'overlap_resolver' in node.arguments and 'from_node' in node.arguments['overlap_resolver']:
                                        source = node.arguments['overlap_resolver']['from_node']
                                        self.partialResults[node.id] = self.partialResults[source]
                                    else:
                                        raise Exception(OverlapResolverMissing)
                                else:
                                    raise Exception("[!] Trying to merge two datacubes with different dimensions, not supported yet!")
                    else:
                        cube1 = node.arguments['cube1']['from_node']
                        cube2 = node.arguments['cube2']['from_node']
                        ds1 = self.partialResults[cube1]
                        ds2 = self.partialResults[cube2]
                        self.partialResults[node.id] = xr.concat([ds1,ds2],dim='variable')

                    #raise Exception("[!] Trying to merge two datacubes with different dimensions, not supported yet!")
                
#                 if 'overlap_resolver' in node.arguments and 'from_node' in node.arguments['overlap_resolver']:
#                         source = node.arguments['overlap_resolver']['from_node']
#                         self.partialResults[node.id] = self.partialResults[source]
#                 else:
#                     cube1 = node.arguments['cube1']['from_node']
#                     cube2 = node.arguments['cube2']['from_node']
#                     ds1 = self.partialResults[cube1]
#                     ds2 = self.partialResults[cube2]
#                     print('++++',ds1)
#                     print('++++',ds2)
#                     self.partialResults[node.id] = xr.concat([ds1,ds2],dim='variable')
#                     print(self.partialResults[node.id])
                
#                 if 'time' in self.partialResults[source].coords:
#                         tmp = xr.Dataset(coords={'t':self.partialResults[source].time.values,'y':self.partialResults[source].y,'x':self.partialResults[source].x})
#                         if 'variable' in self.partialResults[source].coords:
#                             try:
#                                 for var in self.partialResults[source]['variable'].values:
#                                     tmp[str(var)] = (('t','y','x'),self.partialResults[source].loc[dict(variable=var)])
#                             except Exception as e:
#                                 print(e)
#                                 tmp[str((self.partialResults[source]['variable'].values))] = (('t','y','x'),self.partialResults[source])
#                         else:
#                             tmp['result'] = (('t','y','x'),self.partialResults[source])
#                     else:
#                         tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x})
#                         if 'variable' in self.partialResults[source].coords:
#                             try:
#                                 for var in self.partialResults[source]['variable'].values:
#                                     tmp[str(var)] = (('y','x'),self.partialResults[source].loc[dict(variable=var)])
#                             except:
#                                 tmp[str((self.partialResults[source]['variable'].values))] = (('y','x'),self.partialResults[source])
#                         else:
#                             tmp['result'] = (('y','x'),self.partialResults[source])
                
            if processName == 'if':
                acceptVal = None
                rejectVal = None
                valueVal  = None
                if isinstance(node.arguments['reject'],float) or isinstance(node.arguments['reject'],int):
                    rejectVal = node.arguments['reject']
                else:   
                    reject = node.arguments['reject']['from_node']
                    rejectVal = self.partialResults[reject]
                if isinstance(node.arguments['accept'],float) or isinstance(node.arguments['accept'],int):
                    acceptVal = node.arguments['accept']
                else:   
                    accept = node.arguments['accept']['from_node']
                    acceptVal = self.partialResults[accept]
                if isinstance(node.arguments['value'],float) or isinstance(node.arguments['value'],int):
                    valueVal = node.arguments['value']
                else:   
                    value = node.arguments['value']['from_node']
                    valueVal = self.partialResults[value]         

                tmpAccept = valueVal * acceptVal
                tmpReject = xr.ufuncs.logical_not(valueVal) * rejectVal
                self.partialResults[node.id] = tmpAccept + tmpReject     

            if processName == 'apply':
                source = node.arguments['process']['from_node']
                self.partialResults[node.id] = self.partialResults[source]

            if processName == 'mask':
                maskSource = node.arguments['mask']['from_node']
                dataSource = node.arguments['data']['from_node']
                # If the mask has a variable dimension, it will keep only the values of the input with the same variable name.
                # Solution is to take the min over the variable dim to drop that dimension. (Problems if there are more than 1 band/variable)
                if 'variable' in self.partialResults[maskSource].dims:
                    mask = self.partialResults[maskSource].min(dim='variable')
                else:
                    mask = self.partialResults[maskSource]
                self.partialResults[node.id] = self.partialResults[dataSource].where(xr.ufuncs.logical_not(mask))
                if 'replacement' in node.arguments:
                    burnValue  = node.arguments['replacement']
                    self.partialResults[node.id] = self.partialResults[node.id].fillna(burnValue)
            
            if processName == 'climatological_normal':
                source             = node.arguments['data']['from_node']
                frequency          = node.arguments['frequency']
                if 'climatology_period' in node.arguments:
                    climatology_period = node.arguments['climatology_period']
                    # Perform a filter_temporal and then compute the mean over a monthly period
                    timeStart = climatology_period[0]
                    timeEnd   = climatology_period[1]
                    if len(timeStart.split('T')) > 1:         # xarray slicing operation doesn't work with dates in the format 2017-05-01T00:00:00Z but only 2017-05-01
                        timeStart = timeStart.split('T')[0]
                    if len(timeEnd.split('T')) > 1:
                        timeEnd = timeEnd.split('T')[0]
                    tmp = self.partialResults[source].loc[dict(time=slice(timeStart,timeEnd))]
                else:
                    tmp = self.partialResults[source]
                if frequency=='monthly':
                    freq = 'time.month'
                else:
                    freq = None
                self.partialResults[node.id] = tmp.groupby(freq).mean("time")

            if processName == 'anomaly':
                source    = node.arguments['data']['from_node']
                normals   = node.arguments['normals']['from_node']
                frequency = node.arguments['frequency']
                if frequency=='monthly':
                    freq = 'time.month'
                else:
                    freq = None
                self.partialResults[node.id] = (self.partialResults[source].groupby(freq) - self.partialResults[normals]).drop('month')

            if processName == 'apply_kernel':
                def convolve(data, kernel, mode='constant', cval=0, fill_value=0):
                    dims = ('x','y')
                #   scipy.ndimage.convolve(input, weights, output=None, mode='reflect', cval=0.0, origin=0)
                    convolved = lambda data: scipy.ndimage.convolve(data, kernel, mode=mode, cval=cval)

                    data_masked = data.fillna(fill_value)

                    return xr.apply_ufunc(convolved, data_masked,
                                          vectorize=True,
                                          dask='parallelized',
                                          input_core_dims = [dims],
                                          output_core_dims = [dims],
                                          output_dtypes=[data.dtype],
                                          dask_gufunc_kwargs={'allow_rechunk':True})

                kernel = np.array(node.arguments['kernel'])
                factor = node.arguments['factor']
                fill_value = 0
                source = node.arguments['data']['from_node']
                openeo_scipy_modes = {'replicate':'nearest','reflect':'reflect','reflect_pixel':'mirror','wrap':'wrap'}
                if 'replace_invalid' in node.arguments:
                    fill_value = node.arguments['replace_invalid']
                if 'border' in node.arguments:
                    if isinstance(node.arguments['border'],int) or isinstance(node.arguments['border'],float):
                        mode = 'constant'
                        cval = node.arguments['border']
                    else:
                        mode_openeo = node.arguments['border']
                        mode = openeo_scipy_modes[mode_openeo]
                        cval = 0
                self.partialResults[node.id] = convolve(self.partialResults[source],kernel,mode,cval,fill_value)
                if factor!=1:
                    self.partialResults[node.id] = self.partialResults[node.id] * factor

            if processName == 'geocode':
                from scipy.spatial import Delaunay
                from scipy.interpolate import LinearNDInterpolator
                source = node.arguments['data']['from_node']
                ## TODO: add check res and crs values, if None raise error
                spatialres = node.arguments['resolution']
                output_crs = "epsg:" + str(node.arguments['crs'])
                ## TODO: check if grid_lon and grid_lat are available, else raise error
                try: 
                    self.partialResults[source].loc[dict(variable='grid_lon')]
                    self.partialResults[source].loc[dict(variable='grid_lat')]
                    if len(self.partialResults[source].dims) >= 3:
                        if len(self.partialResults[source].time)>=1 and len(self.partialResults[source].loc[dict(variable='grid_lon')].dims)>2:
                            grid_lon = self.partialResults[source].loc[dict(variable='grid_lon',time=self.partialResults[source].time[0])].values
                            grid_lat = self.partialResults[source].loc[dict(variable='grid_lat',time=self.partialResults[source].time[0])].values
                        else:
                            grid_lon = self.partialResults[source].loc[dict(variable='grid_lon')].values
                            grid_lat = self.partialResults[source].loc[dict(variable='grid_lat')].values
                except Exception as e:
                    raise(e)    
                x_regular, y_regular, grid_x_irregular, grid_y_irregular = create_S2grid(grid_lon,grid_lat,output_crs,spatialres)
                grid_x_regular, grid_y_regular = np.meshgrid(x_regular,y_regular)
                grid_x_irregular = grid_x_irregular.astype(np.float32)
                grid_y_irregular = grid_y_irregular.astype(np.float32)
                x_regular = x_regular.astype(np.float32)
                y_regular = y_regular.astype(np.float32)
                grid_x_regular = grid_x_regular.astype(np.float32)
                grid_y_regular = grid_y_regular.astype(np.float32)
                grid_x_regular_shape = grid_x_regular.shape
                grid_regular_flat = np.asarray([grid_x_regular.flatten(), grid_y_regular.flatten()]).T
                grid_x_regular = None
                grid_y_regular = None
                grid_irregular_flat = np.asarray([grid_x_irregular.flatten(), grid_y_irregular.flatten()]).T
                grid_x_irregular = None
                grid_y_irregular = None

                delaunay_obj = Delaunay(grid_irregular_flat)  # Compute the triangulation
                
                geocoded_cube = xr.Dataset(
                    coords={
                        "y": (["y"],y_regular),
                        "x": (["x"],x_regular)
                    },
                )

                def data_geocoding(data,grid_regular_flat):
                    flat_data = data.values.flatten()

                    def parallel_geocoding(subgrid):
                        interpolator  = LinearNDInterpolator(delaunay_obj, flat_data)
                        geocoded_data_slice = interpolator(subgrid)
                        return geocoded_data_slice

                    chunk_length = 20000000
                    subgrids = []
                    for i in range(int(len(grid_regular_flat)/chunk_length)+1):
                        if i<int(len(grid_regular_flat)/chunk_length):
                            grid_regular_flat_slice = grid_regular_flat[i*chunk_length:(i+1)*chunk_length]
                            subgrids.append(grid_regular_flat_slice)
                        else:
                            grid_regular_flat_slice = grid_regular_flat[i*chunk_length:]
                            subgrids.append(grid_regular_flat_slice)     

                    result = []
                    for s in subgrids:
                        result.append(delayed(parallel_geocoding)(s))

                    result = dask.compute(*result,scheduler='threads')
                    result_list = []
                    for r in result:
                        result_list += r.tolist()
                    result_arr = np.asarray(result_list)
                    return result_arr
                
                print("Geocoding started!")
                start = time()
                try: 
                    self.partialResults[source]['time']
                    for t in self.partialResults[source]['time']:
                        print(t.values)
                        geocoded_dataset = None
                        for var in self.partialResults[source]['variable']:
                            print(var.values)
                            if (var.values!='grid_lon' and var.values!='grid_lat'):
                                data = self.partialResults[source].loc[dict(variable=var,time=t)]
                                geocoded_data = data_geocoding(data,grid_regular_flat).reshape(grid_x_regular_shape)
                                if geocoded_dataset is None:
                                    geocoded_dataset = geocoded_cube.assign_coords(time=t.values).expand_dims('time')
                                    geocoded_dataset[str(var.values)] = (("time","y", "x"),np.expand_dims(geocoded_data,axis=0))
                                else:
                                    geocoded_dataset[str(var.values)] = (("time","y", "x"),np.expand_dims(geocoded_data,axis=0))

                        geocoded_dataset.to_netcdf(self.tmpFolderPath+'/'+str(t.values)+'.nc')
                        geocoded_dataset = None
                    ## With a timeseries of geocoded data, I write every timestep, which can have multiple bands,
                    ## into a NetCDF and then I read the timeseries in chunks to avoid memory problems.
                    self.partialResults[node.id] = xr.open_mfdataset(self.tmpFolderPath + '/*.nc', combine="by_coords").to_array()
                except:
                    geocoded_dataset = None
                    for var in self.partialResults[source]['variable']:
                        if (var.values!='grid_lon' and var.values!='grid_lat'):
                            data = self.partialResults[source].loc[dict(variable=var)]
                            geocoded_data = data_geocoding(data,grid_regular_flat).reshape(grid_x_regular_shape)
                            if geocoded_dataset is None:
                                geocoded_cube[str(var.values)] = (("y", "x"),geocoded_data)
                                geocoded_dataset = geocoded_cube
                            else:
                                geocoded_dataset[str(var.values)] = (("y", "x"),geocoded_data)

                    self.partialResults[node.id] = geocoded_dataset.to_array()
                print("Elapsed time: ", time() - start)
            
            if processName == 'radar_mask':
                source = node.arguments['data']['from_node']
                threshold = node.arguments['threshold']
                orbit = node.arguments['orbit']
                
                src = self.partialResults[source]
                samples_dem = len(src.loc[dict(variable='DEM')].x)
                lines_dem   = len(src.loc[dict(variable='DEM')].y)
                dx = src.loc[dict(variable='DEM')].x[1].values - src.loc[dict(variable='DEM')].x[0].values  # Change based on geocoding output
                dy = src.loc[dict(variable='DEM')].y[1].values - src.loc[dict(variable='DEM')].y[0].values
                demdata = src.loc[dict(variable='DEM')].values
                # heading for sentinel:
                # ASC = -12.5°
                # DSC = +12.5°
                # Convert to radians before the usage
                heading = -12.5*np.pi/180 #ASC
                if orbit == 'DSC':
                    heading = 12.5*np.pi/180
                dx_p=dx*np.tan(heading)
                dy_p=dy*np.tan(heading)
                daz = 2*np.sqrt(dy_p**2+dy**2)
                drg = 2*np.sqrt(dx_p**2+dx**2)
                h_az_0 = demdata[0:lines_dem-3,0:samples_dem-3] + (demdata[0:lines_dem-3,2:samples_dem-1] - demdata[0:lines_dem-3,0:samples_dem-3])/(2*dx)*(dx+dx_p)
                h_az_2 = demdata[2:lines_dem-1,0:samples_dem-3] + (demdata[2:lines_dem-1,2:samples_dem-1] - demdata[2:lines_dem-1,0:samples_dem-3])/(2*dx)*(dx-dx_p)
                inc_h_az = -(h_az_2-h_az_0)
                h_rg_0=demdata[0:lines_dem-3,0:samples_dem-3] + (demdata[2:lines_dem-1,0:samples_dem-3] - demdata[0:lines_dem-3,0:samples_dem-3])/(2*dy)*(dy-dy_p)
                h_rg_2=demdata[0:lines_dem-3,2:samples_dem-1] + (demdata[2:lines_dem-1,2:samples_dem-1] - demdata[0:lines_dem-3,2:samples_dem-1])/(2*dy)*(dy+dy_p)
                inc_h_rg=h_rg_2-h_rg_0
                rg_sign = 0
                az_sign = 0
                if heading >= 0:
                    az_sign=-1
                    rg_sign=-1 
                else:
                    az_sign=1
                    rg_sign=1
                res_out_f = np.zeros((demdata.shape))
                res_out_o = np.zeros((demdata.shape))
                res_out_f[1:lines_dem-2,1:samples_dem-2] = np.arctan(inc_h_rg/drg)*rg_sign # range
                res_out_o[1:lines_dem-2,1:samples_dem-2] = np.arctan(inc_h_az/daz)*az_sign
                res_out_f_deg = res_out_f*180/np.pi
                res_out_o_deg = res_out_o*180/np.pi
                mean_incAngle = np.nanmean(src.loc[dict(variable='LIA')].values)
                #foreshortening
                foreshorteningTH   = float(threshold) # Foreshortening threshold
                foreshortening     = np.bitwise_and(res_out_f_deg > 0,res_out_f_deg < mean_incAngle)*res_out_f_deg / mean_incAngle
                foreshorteningMask = np.zeros((demdata.shape))
                foreshorteningMask = (foreshortening > foreshorteningTH).astype(np.float32)
                #layover
                layover        = np.bitwise_and(res_out_f_deg > 0,res_out_f_deg > mean_incAngle)*res_out_f_deg / mean_incAngle
                layover_Mask   = (layover > 0).astype(np.float32)
                #shadowing
                shadow      = np.bitwise_and(res_out_f_deg  < 0,np.abs(res_out_f_deg) > (90-mean_incAngle)).astype(np.float32)
                radar_mask = ((foreshorteningMask + layover + shadow) > 1).astype(np.float32)
                self.partialResults[node.id] = src
                self.partialResults[node.id]['mask'] = radar_mask

            if processName == 'coherence':
                #{'data': {'from_node': '1_0'}, 'timedelta': '6 days'}
                source = node.arguments['data']['from_node']
                timedelta_str = None
                timedelta = 6
                if 'timedelta' in node.arguments:
                    timedelta_str =  node.arguments['timedelta']
                if timedelta_str=='12 days':
                    timedelta = 12
                elif timedelta_str=='24 days':
                    timedelta = 24
                elif timedelta_str=='48 days':
                    timedelta = 48
                else:
                    pass
                    
                # We put the timesteps of the datacube into an array
                timesteps = self.partialResults[source]['time'].values
                days_pairs = []
                # We loop through the timesteps and check where we have 6-12-24 days pairs of dates

                tmp_dataset_timeseries = None
                for i,t in enumerate(timesteps[:-1]):
                    if(np.timedelta64(timesteps[i+1] - timesteps[i], 'D')) == np.timedelta64(timedelta,'D'):
                        days_pairs.append([timesteps[i],timesteps[i+1]])
                
                src = self.partialResults[source]
                for i,pair in enumerate(days_pairs):
                    print(pair)
                    VV_q_coh = (src.loc[dict(variable='i_VV',time=pair[0])]*src.loc[dict(variable='i_VV',time=pair[1])]+src.loc[dict(variable='q_VV',time=pair[0])]*src.loc[dict(variable='q_VV',time=pair[1])])/                    np.sqrt((src.loc[dict(variable='i_VV',time=pair[0])]**2+src.loc[dict(variable='q_VV',time=pair[0])]**2)*(src.loc[dict(variable='i_VV',time=pair[1])]**2+src.loc[dict(variable='q_VV',time=pair[1])]**2))
                    VV_i_coh = (src.loc[dict(variable='i_VV',time=pair[1])]*src.loc[dict(variable='q_VV',time=pair[0])]-src.loc[dict(variable='i_VV',time=pair[0])]*src.loc[dict(variable='q_VV',time=pair[1])])/                    np.sqrt((src.loc[dict(variable='i_VV',time=pair[0])]**2+src.loc[dict(variable='q_VV',time=pair[0])]**2)*(src.loc[dict(variable='i_VV',time=pair[1])]**2+src.loc[dict(variable='q_VV',time=pair[1])]**2))
                    
                    VH_q_coh = (src.loc[dict(variable='i_VH',time=pair[0])]*src.loc[dict(variable='i_VH',time=pair[1])]+src.loc[dict(variable='q_VH',time=pair[0])]*src.loc[dict(variable='q_VH',time=pair[1])])/                    np.sqrt((src.loc[dict(variable='i_VH',time=pair[0])]**2+src.loc[dict(variable='q_VH',time=pair[0])]**2)*(src.loc[dict(variable='i_VH',time=pair[1])]**2+src.loc[dict(variable='q_VH',time=pair[1])]**2))
                    VH_i_coh = (src.loc[dict(variable='i_VH',time=pair[1])]*src.loc[dict(variable='q_VH',time=pair[0])]-src.loc[dict(variable='i_VH',time=pair[0])]*src.loc[dict(variable='q_VH',time=pair[1])])/                    np.sqrt((src.loc[dict(variable='i_VH',time=pair[0])]**2+src.loc[dict(variable='q_VH',time=pair[0])]**2)*(src.loc[dict(variable='i_VH',time=pair[1])]**2+src.loc[dict(variable='q_VH',time=pair[1])]**2))
                                                 
                    tmp_dataset = xr.Dataset(
                        coords={
                            "y": (["y"],self.partialResults[source].y.values),
                            "x": (["x"],self.partialResults[source].x.values)
                        },
                    )
                    if i==0:
                        tmp_dataset = tmp_dataset.assign_coords(time=pair[0]).expand_dims('time')
                        tmp_dataset['i_VV'] = (("time","y", "x"),VV_i_coh.expand_dims('time'))
                        tmp_dataset['q_VV'] = (("time","y", "x"),VV_q_coh.expand_dims('time'))
                        tmp_dataset['i_VH'] = (("time","y", "x"),VH_i_coh.expand_dims('time'))
                        tmp_dataset['q_VH'] = (("time","y", "x"),VH_q_coh.expand_dims('time'))
                        tmp_dataset_timeseries = tmp_dataset
                    else:
                        tmp_dataset = tmp_dataset.assign_coords(time=pair[0]).expand_dims('time')
                        tmp_dataset['i_VV'] = (("time","y", "x"),VV_i_coh.expand_dims('time'))
                        tmp_dataset['q_VV'] = (("time","y", "x"),VV_q_coh.expand_dims('time'))
                        tmp_dataset['i_VH'] = (("time","y", "x"),VH_i_coh.expand_dims('time'))
                        tmp_dataset['q_VH'] = (("time","y", "x"),VH_q_coh.expand_dims('time'))
                        tmp_dataset_timeseries = xr.concat([tmp_dataset_timeseries,tmp_dataset],dim='time')
                
                print('COHERENCE RESULT:\n',tmp_dataset_timeseries.to_array())
                self.partialResults[node.id] = tmp_dataset_timeseries.to_array()
                
            if processName == 'fit_curve':
                start = time()
                ## The fitting function as been converted in a dedicated if statement into a string
                fitFunction = self.partialResults[node.arguments['function']['from_node']]
                ## The data can't contain NaN values, they are replaced with zeros
                data = self.partialResults[node.arguments['data']['from_node']].compute().fillna(0)
                data_dataset = self.refactor_data(data)
                data_dataset = data_dataset.rename({'t':'time'})
                baseParameters = node.arguments['parameters'] ## TODO: take care of them, currently ignored

                ## Preparation of fitting functions:
                def build_fitting_functions():
                    baseFun = "def fitting_function(x"
                    parametersStr = ""
                    for i in range(len(baseParameters)):
                        parametersStr += ",a"+str(i)
                    baseFun += (parametersStr + "):")
                    baseFun += ('''
    return '''+ fitFunction)
                    return baseFun
                ## Generate python fitting function as string 
                fitFun = build_fitting_functions()
                print(fitFun)
                exec(fitFun,globals())
                def fit_curve(x,y):
                    index = np.nonzero(y) # We don't consider zero values (masked) for fitting.
                    x = x[index]
                    y = y[index]
                    popt, pcov = curve_fit(fitting_function, x, y)
                    return popt
                
                
                dates = data_dataset.time.values
                unixSeconds = [ ((x - np.datetime64('1970-01-01')) / np.timedelta64(1, 's')) for x in dates]
                data_dataset['time'] = unixSeconds
                popts3d = xr.apply_ufunc(fit_curve,data_dataset.time,data_dataset,
                           vectorize=True,
                           input_core_dims=[['time'],['time']], #Dimension along we fit the curve function
                           output_core_dims=[['params']],
                           dask="parallelized",
                           output_dtypes=[np.float32],
                           dask_gufunc_kwargs={'allow_rechunk':True,'output_sizes':{'params':len(baseParameters)}}
                            )
                data_dataset['time'] = dates
                     
                self.partialResults[node.id] = popts3d.compute()
                print("Elapsed time: ",time() - start)
                   
            if processName == 'predict_curve':
                start = time()
                fitFunction = self.partialResults[node.arguments['function']['from_node']]
                data = self.partialResults[node.arguments['data']['from_node']]
                dates = data.time.values
                unixSeconds = [ ((x - np.datetime64('1970-01-01')) / np.timedelta64(1, 's')) for x in dates]
                data['time'] = unixSeconds
                baseParameters = self.partialResults[node.arguments['parameters']['from_node']]
                
                def build_fitting_functions():
                    baseFun = "def predicting_function(x"
                    parametersStr = ""
                    for i in range(len(baseParameters.params)):
                        parametersStr += ",a"+str(i) + "=0"
                    baseFun += (parametersStr + "):")
                    baseFun += ('''
    return '''+ fitFunction)
                    return baseFun
                fitFun = build_fitting_functions()
                print(fitFun)
                exec(fitFun,globals())
                if 'variable' in data.dims:
                    predictedData = xr.Dataset(coords={'time':dates,'y':data.y,'x':data.x})
                    for var in data['variable'].values:
                        input_params = {}
                        for i in range(len(baseParameters.params)):
                            band_parameter = baseParameters.loc[dict(variable=var)].drop('variable')[:,:,i]
                            input_params['a'+str(i)] = band_parameter
                        tmp_var = predicting_function(data.time,**input_params).astype(np.float32)
                        tmp_var['time'] = dates
                        predictedData[var] = tmp_var
                else:
                    predictedData = predicting_function(data.time,baseParameters[0,:,:].drop('variable'),baseParameters[1,:,:].drop('variable'),baseParameters[2,:,:].drop('variable'))
                
                data['time'] = dates
                print("Elapsed time: ",time() - start)
                self.partialResults[node.id] = predictedData.to_array().transpose('variable','time','y','x')
                
            if processName == 'load_result':
                try:
                    # If the data is has a single band we load it directly as xarray.DataArray, otherwise as Dataset and convert to DataArray
                    self.partialResults[node.id] = xr.open_dataarray(TMP_FOLDER_PATH + node.arguments['id'] + '/output.nc',chunks={})
                except:
                    self.partialResults[node.id] = xr.open_dataset(TMP_FOLDER_PATH + node.arguments['id'] + '/output.nc',chunks={}).to_array()
                
            if processName == 'save_result':
                outFormat = node.arguments['format']
                source = node.arguments['data']['from_node']
                print(self.partialResults[source])

                if outFormat.lower() == 'png':
                    self.outFormat = '.png'
                    self.mimeType = 'image/png'
                    import cv2
                    self.partialResults[source] = self.partialResults[source].fillna(0)
                    size = None; red = None; green = None; blue = None; gray = None
                    if 'options' in node.arguments:
                        if 'size' in node.arguments['options']:
                            size = node.arguments['options']['size']
                        if 'red' in node.arguments['options']:
                            red = node.arguments['options']['red']
                        if 'green' in node.arguments['options']:
                            green = node.arguments['options']['green']
                        if 'blue' in node.arguments['options']:
                            blue = node.arguments['options']['blue']
                        if 'gray' in node.arguments['options']:
                            gray = node.arguments['options']['gray']
                        if red is not None and green is not None and blue is not None and gray is not None:
                            redBand   = self.partialResults[source].loc[dict(variable=red)].values
                            blueBand  = self.partialResults[source].loc[dict(variable=blue)].values
                            greenBand = self.partialResults[source].loc[dict(variable=green)].values
                            grayBand  = self.partialResults[source].loc[dict(variable=gray)].values
                            bgr = np.stack((blueBand,greenBand,redBand,grayBand),axis=2)
                        elif red is not None and green is not None and blue is not None:
                            redBand   = self.partialResults[source].loc[dict(variable=red)].values
                            blueBand  = self.partialResults[source].loc[dict(variable=blue)].values
                            greenBand = self.partialResults[source].loc[dict(variable=green)].values
                            bgr = np.stack((blueBand,greenBand,redBand),axis=2)
                        else:
                            bgr = self.partialResults[source].values
                            if bgr.shape[0] in [1,2,3,4]:
                                bgr = np.moveaxis(bgr,0,-1)
                    else:
                        bgr = self.partialResults[source].values
                        if bgr.shape[0] in [1,2,3,4]:
                            bgr = np.moveaxis(bgr,0,-1)
                    if size is not None: # The OpenEO API let the user set the "longest dimension of the image in pixels"
                        # 1 find the bigger dimension
                        if bgr.shape[0] > bgr.shape[1]:
                            scaleFactor = size/bgr.shape[0]
                            width = int(bgr.shape[1] * scaleFactor)
                            height = int(size)
                            dsize = (width, height)
                            # 2 resize
                            bgr = cv2.resize(bgr, dsize)
                        else:
                            scaleFactor = size/bgr.shape[1]
                            width = int(size)
                            height = int(bgr.shape[0] * scaleFactor)
                            dsize = (width, height)
                            bgr = cv2.resize(bgr, dsize)
                    bgr = bgr.astype(np.uint8)
                    if(self.sar2cubeCollection): bgr=np.flipud(bgr)
                    cv2.imwrite(str(self.tmpFolderPath) + '/output.png',bgr)
                    return 0

                if outFormat.lower() in ['gtiff','geotiff','tif','tiff']:
                    self.outFormat = '.tiff'
                    self.mimeType = 'image/tiff'
                    import rioxarray
                    
                    if self.partialResults[source].dtype == 'bool':
                        self.partialResults[source] = self.partialResults[source].astype(np.uint8)
                    
                    if len(self.partialResults[source].dims) > 3:
                        if len(self.partialResults[source].time)>=1 and len(self.partialResults[source].variable)==1:
                            # We keep the time dimension as band in the GeoTiff, timeseries of a single band/variable
                            self.partialResults[node.id] = self.partialResults[source].squeeze('variable').to_dataset(name='result')
                        elif (len(self.partialResults[source].time==1) and len(self.partialResults[source].variable>=1)):
                            # We keep the time variable as band in the GeoTiff, multiple band/variables of the same timestamp
                            self.partialResults[node.id] = self.partialResults[source].squeeze('time')
                            geocoded_cube = xr.Dataset(
                                                        coords={
                                                            "y": (["y"],self.partialResults[node.id].y),
                                                            "x": (["x"],self.partialResults[node.id].x)
                                                        },
                                                    )
                            for var in self.partialResults[node.id]['variable']:
                                geocoded_cube[str(var.values)] = (("y", "x"),self.partialResults[node.id].loc[dict(variable=var.values)])
                            self.partialResults[node.id] = geocoded_cube
                        else:
                            raise Exception("[!] Not possible to write a 4-dimensional GeoTiff, use NetCDF instead.")
                    else:
                        self.partialResults[node.id] = self.partialResults[source] 
                    self.partialResults[node.id].attrs['crs'] = self.crs
                    self.partialResults[node.id].rio.to_raster(self.tmpFolderPath + "/output.tif")
                    return 0

                if outFormat.lower() in ['netcdf','nc']:
                    self.outFormat = '.nc'
                    self.mimeType = 'application/octet-stream'
                    if 'params' in self.partialResults[source].dims:
                        self.partialResults[source].to_netcdf(self.tmpFolderPath + "/output.nc")
                        return
                    
                    tmp = self.refactor_data(self.partialResults[source])
                    tmp.attrs = self.partialResults[source].attrs
    #                 self.partialResults[source].time.encoding['units'] = "seconds since 1970-01-01 00:00:00"
                    try:
                        tmp.to_netcdf(self.tmpFolderPath + "/output.nc")
                    except:
                        pass
                    try:
                        tmp.t.attrs.pop('units', None)
                        tmp.to_netcdf(self.tmpFolderPath + "/output.nc")
                    except:
                        pass
                    return 
                
                if outFormat.lower == 'json':
                    self.outFormat = '.json'
                    self.mimeType = 'application/json'
                    self.partialResults[node.id] = self.partialResults[source].to_dict()
                    with open(self.tmpFolderPath + "/output.json", 'w') as outfile:
                        json.dump(self.partialResults[node.id],outfile)
                    return 
                
                else:
                    raise Exception("[!] Output format not recognized/implemented!")

                return 0 # Save result is the end of the process graph
            
            self.listExecutedIds.append(node.id) # Store the processed nodes ids
            return 1 # Go on and process the next node
        
        except Exception as e:
            print(e)
            raise Exception(processName + '\n' + str(e))
    
    def refactor_data(self,data):
        # The following code is required to recreate a Dataset from the final result as Dataarray, to get a well formatted netCDF
        if 'time' in data.coords:
            tmp = xr.Dataset(coords={'t':data.time.values,'y':data.y,'x':data.x})
            if 'variable' in data.coords:
                try:
                    for var in data['variable'].values:
                        tmp[str(var)] = (('t','y','x'),data.loc[dict(variable=var)].drop('variable').transpose('time','y','x'))
                except Exception as e:
                    print(e)
                    tmp[str((data['variable'].values))] = (('t','y','x'),data.transpose('time','y','x'))
            else:
                return data
        else:
            tmp = xr.Dataset(coords={'y':data.y,'x':data.x})
            if 'variable' in data.coords:
                try:
                    for var in data['variable'].values:
                        tmp[str(var)] = (('y','x'),data.loc[dict(variable=var)].drop('variable').transpose('y','x'))
                except Exception as e:
                    print(e)
                    tmp[str((data['variable'].values))] = (('y','x'),data.transpose('y','x'))
            else:
                return data
        tmp.attrs = data.attrs
        return tmp