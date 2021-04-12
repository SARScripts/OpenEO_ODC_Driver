# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   12/04/2021

import dask
from dask.distributed import Client
from openeo_odc_driver import OpenEO
import argparse
import os
import sys
from flask import Flask, request, jsonify, send_file
import json
import requests
import yaml

app = Flask('openeo_odc_driver')
@app.route('/graph', methods=['POST'])
def process_graph():
    jsonGraph = request.json
    eo = OpenEO(jsonGraph,0)
    return send_file('output'+eo.outFormat, as_attachment=True, attachment_filename='output'+eo.outFormat)

@app.route('/collections/', methods=['GET'])
def list_collections():
    res = requests.get('http://0.0.0.0:9000/products.txt')
    datacubesList = res.text.split('\n')
    collections = {}
    collections['collections'] = {}
    for i,d in enumerate(datacubesList):
        res = requests.get('http://0.0.0.0:9000/collections/'+d)
        test = res.json()
        test.pop('properties')
        test['license'] = 'CC-BY-4.0'
        test['providers'] = [{'name': 'Eurac EO ODC', 'url': 'http://www.eurac.edu/', 'roles': ['producer','host']}]
        test['links'] = {}
        test['links'] = [{'rel' : 'license', 'href' : 'https://creativecommons.org/licenses/by/4.0/', 'type' : 'text/html', 'title' : 'License link'}]
        collections['collections'][i] = test
    return jsonify(collections)


@app.route("/collections/<string:name>/", methods=['GET'])
def datacube_details(name):
    res = requests.get('http://0.0.0.0:9000/collections/'+name)
    test = res.json()
    test.pop('properties')
    test['license'] = 'CC-BY-4.0'
    test['providers'] = [{'name': 'Eurac EO ODC', 'url': 'http://www.eurac.edu/', 'roles': ['producer','host']}]
    test['links'] = {}
    test['links'] = [{'rel' : 'license', 'href' : 'https://creativecommons.org/licenses/by/4.0/', 'type' : 'text/html', 'title' : 'License link'}]
    test['stac_extensions'] = ['datacube']
    test['stac_extensions'] = ['datacube']
    test['cube:dimensions'] = {}
    test['cube:dimensions']['DATE'] = {}
    test['cube:dimensions']['DATE']['type'] = 'temporal'
    test['cube:dimensions']['DATE']['extent'] = test['extent']['temporal']['interval'][0]
    
    test['cube:dimensions']['X'] = {}
    test['cube:dimensions']['X']['type'] = 'spatial'
    test['cube:dimensions']['X']['axis'] = 'x'
    test['cube:dimensions']['X']['extent'] = [test['extent']['spatial']['bbox'][0][0],test['extent']['spatial']['bbox'][0][2]]  
    test['cube:dimensions']['X']['reference_system'] = 'unknown'

    test['cube:dimensions']['Y'] = {}
    test['cube:dimensions']['Y']['type'] = 'spatial'
    test['cube:dimensions']['Y']['axis'] = 'y'
    test['cube:dimensions']['Y']['extent'] = [test['extent']['spatial']['bbox'][0][1],test['extent']['spatial']['bbox'][0][3]]
    test['cube:dimensions']['Y']['reference_system'] = 'unknown'
    
    res = requests.get('http://0.0.0.0:9000/collections/'+name+'/items')
    items = res.json()
    yamlFile = items['features'][0]['assets']['location']['href']
    yamlFile = yamlFile.split('file://')[1].replace('%40','@')
    
    with open(yamlFile, 'r') as stream:
        try:
            yamlDATA = yaml.safe_load(stream)
            #print(yamlDATA['grid_spatial']['projection']['spatial_reference'])
            test['cube:dimensions']['X']['reference_system'] = int(yamlDATA['grid_spatial']['projection']['spatial_reference'].split('EPSG')[-1].split('\"')[-2])
            test['cube:dimensions']['Y']['reference_system'] = int(yamlDATA['grid_spatial']['projection']['spatial_reference'].split('EPSG')[-1].split('\"')[-2])
        except yaml.YAMLError as exc:
            print(exc)
    
    keys = items['features'][0]['assets'].keys()
    list_keys = list(keys)
    list_keys.remove('location')
    bands_list = []
    for key in list_keys:
        if len(items['features'][0]['assets'][key]['eo:bands'])>1:
            for b in items['features'][0]['assets'][key]['eo:bands']:
                bands_list.append(b)
        else:
            bands_list.append(items['features'][0]['assets'][key]['eo:bands'][0])
            
    test['cube:dimensions']['bands'] = {}
    test['cube:dimensions']['bands']['type'] = 'bands'
    test['cube:dimensions']['bands']['values'] = bands_list
    summaries_dict = {}
    summaries_dict['constellation'] = ['No Constellation Information Available']
    summaries_dict['platform'] = ['No Platform Information Available']
    summaries_dict['instruments'] = ['No Instrument Information Available']
    cloud_cover_dict = {}
    cloud_cover_dict['min'] = 0
    cloud_cover_dict['max'] = 0
    summaries_dict['eo:cloud cover'] = cloud_cover_dict
    summaries_dict['eo:gsd'] = [0]
    eo_bands_list_dict = []
    for band in bands_list:
        eo_bands_dict = {}
        eo_bands_dict['name'] = band
        eo_bands_dict['common_name'] = band
        eo_bands_dict['center_wavelength'] = 0
        eo_bands_dict['gsd'] = 0
        eo_bands_list_dict.append(eo_bands_dict)
        
    summaries_dict['eo:bands'] = eo_bands_list_dict
    test['summaries'] = summaries_dict
    
    
    return jsonify(test)
