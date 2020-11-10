#!/usr/bin/python3

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk
import datetime as dt
import pandas as pd
from pathlib import Path
import numpy as np
from datetime import datetime, timedelta

def es_login(username=None, password=None, filename="credentials.key"):
    """ Create and log into an ElasticSearch instance.
    
    Args:
        username (str)
        password (str)
        filename (str): relative path and name of credentials file.
    
    Returns:
        Elasticsearch: An logged-in instance of ElasticSearch.
    """
    
    if not username or not password:
        try:
            with open(filename) as f:
                username = f.readline().strip()
                password = f.readline().strip()
        except:
            print("Valid credentials were not found.")
            return
    
    credentials = (username, password)
    es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200, 'scheme': 'https'}], timeout=240, http_auth=credentials)

    if es.ping():
        print("Connection Successful")
    else:
        print("Connection Unsuccessful")
    
    return es


def ps_meta_hosts(es,start,end,include=["geolocation","host","config.site_name"]):
    query = {
	"query": {
	    "bool": {
	        "filter": [
	            {
    	 	        "range": {
			    "timestamp": {
			        "gte": start,
				"lte": end,
				"format": "strict_date_optional_time"
			    }
			}
		    }
		]
	    }
	}
    }
    try:
        return scan_gen(scan(es,index="ps_meta",query=query, _source=include, filter_path=['_scroll_id', '_shards', 'hits.hits._source']))
    except Exception as e:
        print(e)

def ps_tp_type(es,start,end,host,include=["src_host","dest_host"]):
    query = {
	"query": {
            "bool": {
                "should": [
                        {"term": {"src_host" : host}},
                        {"term": {"dest_host": host}}
                  ]
            }
        }
    }    
    try:
        return scan_gen(scan(es,index="throughput_derived", query=query, _source=include, filter_path=['_scroll_id', '_shards', 'hits.hits._source']))
    except Exception as e:
        print(e)

def ps_pl_type(es,start,end,host,include=["src_host","dest_host"]):
    query = {
        "query": {
            "bool": {
                "should": [
                        {"term": {"src_host" : host}},
                        {"term": {"dest_host": host}}
                  ]
            }
        }
    }
    try:
        return scan_gen(scan(es,index="owd_derived_v2", query=query, _source=include, filter_path=['_scroll_id', '_shards', 'hits.hits._source']))
    except Exception as e:
        print(e)


def scan_gen(scan):
    while True:
        try:
            yield next(scan)['_source']
        except:
            break

PROJECT_ROOT = Path.cwd().parent

def save_data(es,start,stop, file_name):
    scan_gen = ps_meta_hosts(es,start,stop)
    data = []
    neatdata = []
    logrecords = []
    records = 0
    host_array = []
    for meta in scan_gen:
        data.append(meta)
        if 'config' in data[records]:
            site = data[records]['config']['site_name']
        host = data[records]['host']
        host_array.append(host)
        if 'geolocation' in data[records]:
            geoip = data[records]['geolocation'].split(",")
        if 'config' in data[records] and 'geolocation' in data[records]:
            neatdata.append([geoip[0],geoip[1],host])
        if 'geolocation' in data[records] and not 'config' in data[records]:
            neatdata.append([geoip[0],geoip[1],host])
        if 'geolocation' not in data[records]:
            logrecords.append(host)
        records += 1
        if not records % 100000:
            print(records)

    seen = set()
    sortneatdata = []
    for item in neatdata:
        t = tuple(item)
        if t not in seen:
            sortneatdata.append(item)
            seen.add(t)

    newhost = [i[2] for i in sortneatdata]

    for i in range(len(newhost)):
        tp_scan = ps_tp_type(es,start,stop,newhost[i])
        tpdata= []
        lat_scan = ps_pl_type(es,start,stop,newhost[i])
        latdata = []
        for tp in tp_scan:
            tpdata.append(tp)
        for lat in lat_scan:
            latdata.append(lat)
        if tpdata and latdata:
            sortneatdata[i].append('both')
        if not tpdata and latdata:
            sortneatdata[i].append('latency')
        if tpdata and not latdata:
            sortneatdata[i].append('throughput')
        if not tpdata and not latdata:
            sortneatdata[i].append('neither')

    sortedlogrecords = list(set(logrecords))
    sortedlogrecords.sort()
    for i in sortedlogrecords:
        print('Host ', i, ' does not have a geolocation in Elasticsearch')
    print('Query Grabbed ',records,' log files')
    df = pd.DataFrame(sortneatdata)
    filepath = '/etc/devtestpython/' + file_name
    df.to_csv(str(filepath),header=False,index=False)


now = datetime.utcnow()
nowform = now.strftime('%Y-%m-%dT%H:%M:%S.000Z')
threeweeks = datetime.utcnow() - timedelta(days=21)
threeweeksformat = threeweeks.strftime('%Y-%m-%dT%H:%M:%S.000Z')
print('Starting Query At',threeweeksformat)
print('Ending Query At',nowform)

elastic = es_login('username','password')
print(elastic)
save_data(elastic,threeweeksformat,nowform,'testhostscan.csv')
print('Done')
