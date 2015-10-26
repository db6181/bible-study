import sys
import time
import csv
import requests
import json
import os
import datetime

ELK_STACK = "http://10.11.8.181:9200"

def clean_value(value):
    val = value.strip()
    val = val.replace('\xc2','')
    val = val.replace('\xa0','')
    return val

def parse_zipcode_csv():
    zipMap = dict()
    with open("zipcode.csv") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            zipMap[row["zip"]] = {"lat": float(row["latitude"]), "lon": float(row["longitude"])}
    return zipMap

def produce_json_doc(filename, zipmap):
    output = list()
    geo_uri = "https://maps.googleapis.com/maps/api/geocode/json?key=AIzaSyDR6X6bLfgmjIdW0rcIijiPLKYw2_a0vV4&address="
    time = datetime.time(12,0,0)
    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            for key,val in row.iteritems():
                row[key] = clean_value(val)
            #resp = requests.get(geo_uri+row["zip"])
            #location = {"lat": 0, "lon": 0}
            #if resp.status_code == 200:
            #    resp_json = resp.json()
            #    if "location" in resp_json:
            #        location = resp_json["location"]
            #row["location"] = location
            row["address"] = (row["address"] + " " + row["address2"]).strip()
            # parse timestamp from filename
            split_name = filename.split("/")
            name_only = split_name[len(split_name)-1].split(".")[0]
            split_date = name_only.split("-")
            if len(split_date) == 3:
                date = datetime.date(int(split_date[0]),int(split_date[1]),int(split_date[2]))
                row["timestamp"] = datetime.datetime.combine(date, time).isoformat()
            else:
                row["timestamp"] = datetime.datetime.now().isoformat()
            if row["zip"] in zipmap:
                row["location"] = zipmap[row["zip"]]
            else:
                row["location"] = {"lat": 0, "lon": 0}
            output.append(row)
    return output

def send_to_elastic_search(data):
    headers = {'content-type': 'application/json'}
    bulk_request = ""
    for entry in data:
        action = {'index':{'_index':'students', '_type': "new_student"}}
        bulk_request += json.dumps(action) + "\r\n"
        bulk_request += json.dumps(entry) + "\r\n"
            
    url = '{}/_bulk'.format(ELK_STACK)
    r = requests.post(url=url, data=bulk_request, headers=headers)
    return r

if __name__ == "__main__":
    headers = {'content-type': 'application/json'}
    url = '{}/students'.format(ELK_STACK)
    requests.delete(url=url, headers=headers)
    url = '{}/students'.format(ELK_STACK)
    requests.post(url=url, headers=headers)
    url = '{}/students/_mapping/new_student'.format(ELK_STACK)
    mapping = {"new_student": {"_timestamp": {"enabled": True, "store": False}, "properties":{"location":{"type":"geo_point"}}}}
    requests.put(url=url, data=json.dumps(mapping), headers=headers)
    directory = "./history" 
    zipmap = parse_zipcode_csv()
    for fn in os.listdir(directory):
        filename = directory+"/"+fn
        if os.path.isfile(filename):
            print "Parsing " + filename + " ..."
            data = produce_json_doc(filename,zipmap)
            send_to_elastic_search(data)
