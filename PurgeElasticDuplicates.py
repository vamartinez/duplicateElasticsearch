#!/usr/bin/python3

import argparse
import http.client
import os
import json
import socket
import time

import sys


def main(elastic_index, elastic_type, elastic_address, field):
    print("")
    print(" ----- Getting Data ----- ")
    offset = 0

    for x in range(1, 99999):
        getGroup( field, elastic_address, elastic_index, elastic_type

    return


def getGroup( field, elastic_address,elastic_index, elastic_type):
    print(elastic_address)
    to_elastic_string = "{ \"from\": 0, \"size\": 0,  \"aggs\": {\"duplicateCount\": {\"terms\": {\"field\": \"" + field + ".keyword\",\"min_doc_count\": 2 }, \"aggs\": { \"duplicateDocuments\": {\"top_hits\": {}}}}}}"
    #SET THE PORT 
    connection = http.client.HTTPSConnection(elastic_address,443)
    headersHttp = {"Content-type": "application/json", "Accept": "text/plain"}
    #print("Body:", to_elastic_string)
    connection.request('GET', url="/"+elastic_index+"/"+elastic_type+"/_search", headers=headersHttp, body=to_elastic_string)
    try:
        response = connection.getresponse()
    except Exception as e:
        print(type(e))
        print(e)

    print("Returned status code:", response.status)
    q = response.read().decode("utf-8")
    print("Returned body", q)
    d = json.loads(q)
    #list_of_lists = []
    if(len(d["aggregations"]["duplicateCount"]['buckets']) > 0):
        for row in d["aggregations"]["duplicateCount"]['buckets']:
            #list_of_lists.append([row["duplicateDocuments"]["hits"]["hits"][1]["_id"],
            #                  row["duplicateDocuments"]["hits"]["hits"][1]["_source"]["transaction_id"]])
            print("Eliminados")
            headersHttp = {"Content-type": "application/json", "Accept": "text/plain"}
            # print("Body:", to_elastic_string)
            connection = http.client.HTTPSConnection(elastic_address, 443)
            connection.request('DELETE', url="/" + elastic_index + "/" + elastic_type + "/"+row["duplicateDocuments"]["hits"]["hits"][1]["_id"], headers=headersHttp,
                               body="")
            response = connection.getresponse()
            print("Returned Delete status code:", response.status)
            #print("Offset:",json.dumps(list_of_lists) )
    else:
        print("Terminamos!!!!!!")
        sys.exit(0)
    time.sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='  ElasticSearch master clean *O*')
    parser.add_argument('--elastic-index',
                        required=True,
                        type=str,
                        help='elastic index you want to put data in')
    parser.add_argument('--elastic-type',
                        required=False,
                        type=str,
                        default='test_type',
                        help='Your entry type for elastic')

    parser.add_argument('--elastic-address',
                        required=False,
                        type=str,
                        default='localhost:9200',
                        help='Your elasticsearch endpoint address')
    parser.add_argument('--field',
                        required=False,
                        type=str,
                        default="",
                        help='field to search')

    parsed_args = parser.parse_args()

    main(elastic_index=parsed_args.elastic_index, elastic_type=parsed_args.elastic_type, elastic_address=parsed_args.elastic_address, field=parsed_args.field)
