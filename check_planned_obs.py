import csv
import sys
import os
import time
import re
import json
from collections import Counter

try: # Python 3.x
    from urllib.parse import quote as urlencode
    from urllib.request import urlretrieve
except ImportError:  # Python 2.x
    from urllib import pathname2url as urlencode
    from urllib import urlretrieve

try: # Python 3.x
    import http.client as httplib
except ImportError:  # Python 2.x
    import httplib

from astropy.table import Table
import numpy as np

import pprint
pp = pprint.PrettyPrinter(indent=4)

PROPID_FILE = "proposalids_gto_ers.csv"
RESULTS_FILE = "testV230.csv"

def mastQuery(request):
    """Perform a MAST query.

        Parameters
        ----------
        request (dictionary): The MAST request json object

        Returns head,content where head is the response HTTP headers, and content is the returned data"""

    server='mast.stsci.edu'

    # Grab Python Version
    version = ".".join(map(str, sys.version_info[:3]))

    # Create Http Header Variables
    headers = {"Content-type": "application/x-www-form-urlencoded",
               "Accept": "text/plain",
               "User-agent":"python-requests/"+version}

    # Encoding the request as a json string
    requestString = json.dumps(request)
    requestString = urlencode(requestString)

    # opening the https connection
    conn = httplib.HTTPSConnection(server)

    # Making the query
    print("Sending MAST query...")
    conn.request("POST", "/api/v0/invoke", "request="+requestString, headers)

    # Getting the response
    resp = conn.getresponse()
    head = resp.getheaders()
    content = resp.read().decode('utf-8')

    # Close the https connection
    conn.close()

    return head,content

def count_planned_obs():
    mashupRequest = {"service":"Mast.Caom.Filtered.TestV230",
                     "format":"json",
                     "params":{
                         "columns":"COUNT_BIG(*)",
                         "filters":[
                             {"paramName":"calib_level",
                              "values":["-1"],
                             }
                         ]}}

    headers,outString = mastQuery(mashupRequest)
    countData = json.loads(outString)

    #pp.pprint(countData)
    data = countData['data']
    count = data[0]['Column1']
    return count

def get_planned_obs():
    mashupRequest = {"service":"Mast.Caom.Filtered.TestV230",
                     "format":"json",
                     "params":{
                         "columns":"*",
                         "filters":[
                             {"paramName":"calib_level",
                              "values":["-1"],
                             }
                         ]}}

    headers,outString = mastQuery(mashupRequest)
    countData = json.loads(outString)
    return countData

def write_to_csv(countData):
    headers = countData['fields']
    head = []
    for field in headers:
        head.append(field['name'])

    all_rows = []
    data = countData['data']
    for obs in data:
        row = []
        for entry in obs.keys():
            as_string = str(obs[entry])
            row.append(as_string)
        all_rows.append(row)

    saveas = "testv230.csv"
    saveas = os.path.abspath(saveas)
    with open(saveas, 'w') as output:
        writer = csv.writer(output)
        writer.writerow(head)
        writer.writerows(all_rows)
        output.close()
    return saveas

def collect_results():
    num = count_planned_obs()
    if num < 50000:
        results = get_planned_obs()
        as_csv = write_to_csv(results)
    print(".csv file written!")
    return as_csv

def extract_results(filename):
    filepath = os.path.abspath(filename)
    all_results = []
    with open(filepath, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for entry in reader:
            all_results.append(dict(entry))
        csvfile.close()
    return all_results

def generate_census(field, table):
    if isinstance(field, list):
        list0 = table[field[0]]
        list1 = table[field[1]]
        print(list0[0])
        print(list1[0])
        all_entries = list(zip(list0, list1))
        print(all_entries[0])
    else:
        all_entries = table[field]
    uniques = Counter(all_entries)
    #uniques = list(set(all_entries))
    print("Found {0} unique entries for '{1}'".format(len(uniques.keys()), field))
    filename = str(field) + ".csv"
    filepath = os.path.abspath(filename)
    head = [field, "occurrences"]
    with open(filepath, 'w') as result_file:
        writer = csv.writer(result_file)
        writer.writerow(head)
        for entry in sorted(uniques.keys()):
            row = [entry, uniques[entry]]
            writer.writerow(row)
    return uniques

def find_missing_entries(all_entries, found):
    #all_entries = list(all_entries.keys())
    found = list(found.keys())
    missing = []
    for entry in all_entries:
        if entry in found:
            continue
        else:
            missing.append(entry)
    print("Missing ID's: {0}".format(missing))

def analyze_stats(all_results):
    primer = all_results[0]
    by_column = {}
    for field in primer.keys():
        by_column[field] = []
    for entry in all_results:
        for current_field in entry:
            by_column[current_field].append(entry[current_field])
    proposals = generate_census('proposal_id', by_column)
    generate_census('proposal_pi', by_column)
    all_proposals = []
    proposal_file = os.path.abspath(PROPID_FILE)
    with open(proposal_file, 'r') as props:
        reader = csv.reader(props)
        for row in reader:
            all_proposals.append(row[0])
        props.close()
    find_missing_entries(all_proposals, proposals)
    generate_census('target_name', by_column)
    generate_census('proposal_type', by_column)
    generate_census('filters', by_column)
    generate_census(['s_ra', 's_dec'], by_column)

def analyze_results():
    results = extract_results(RESULTS_FILE)
    stats = analyze_stats(results)

if __name__ == "__main__":
    #collect_results()
    analyze_results()
