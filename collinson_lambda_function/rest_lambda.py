# -*- coding: utf-8 -*-
__author__ = "Akshay Nar"

# python dependencies
import json
from functions import utility

def fetch_rds_data(event, context):

    data = {"ERROR":[], "SUCCESS":[]}
    
    resp = utility.validate_request(event)
    print("Process Started ....")
    print("Request Validation ....")
    if not resp["status"]:
        data["ERROR"].append({"MESSAGE":resp["msg"], "STATUS_CODE":"", "PROCESS":"REQUEST VALIDATION"})
    else:
        data["SUCCESS"].append({"MESSAGE":resp["msg"], "STATUS_CODE": 200, "PROCESS":"REQUEST VALIDATION"})
    
    print("DB Validation ....")
    
    #2. Validate DB data
    resp, db_obj = utility.db_config()
    if not resp["status"]:
        data["ERROR"].append({"MESSAGE":resp["msg"], "STATUS_CODE": 10061, "PROCESS":"DB CONFIGURATION"})
    else:
        data["SUCCESS"].append({"MESSAGE":resp["msg"], "STATUS_CODE": 200, "PROCESS":"DB CONFIGURATION"})
    
    resp = utility.fetch_data(db_obj,event['query'],event['type'].upper())
    resp_data = resp['data']
    if not resp["status"]:
        data["ERROR"].append({"MESSAGE":resp["msg"], "STATUS_CODE": 10061, "PROCESS":"FETCH DATA"})
    else:
        data["SUCCESS"].append({"MESSAGE":resp["msg"], "STATUS_CODE": 200, "PROCESS":"FETCH DATA"})

    resp = utility.process_data(resp_data,event['records'])
    if not resp["status"]:
        data["ERROR"].append({"MESSAGE":resp["msg"], "STATUS_CODE": 10061, "PROCESS":"PROCESS DATA"})
    else:
        data["SUCCESS"].append({"MESSAGE":resp["msg"], "STATUS_CODE": 200, "PROCESS":"PROCESS DATA"})
        data["SUCCESS"].append({"QUERY_DATA" : resp["data"]})
    print(resp_data)
    # TODO implement
    return json.dumps(data)

