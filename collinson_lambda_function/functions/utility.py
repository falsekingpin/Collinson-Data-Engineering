# -*- coding: utf-8 -*-
__author__ = "Akshay Nar"

# python dependencies
import os
import pymysql

def validate_request(req_data):
    """
    req_data = {"query":"abc", "type" : "select", "records" : "one"}
    """
    resp = {"msg":"", "status":False}
    expected_keys = ['query', 'type','records']
    try:
        missing_keys = set(expected_keys) - set(req_data.keys())
        if len(req_data.keys()) == len(expected_keys) and len(list(missing_keys)) ==0 :
            resp["status"] = True
            resp["msg"] = "Requested data validated successfully"
        else:
            msg = "Data is missing in Requested data should be, {0} ".format(", ".join(list(missing_keys)))
            resp["msg"] = msg
    except:
        msg = "Invalid Requested data format"
        resp["msg"] = msg        
    return resp


def db_config():
    try:
        db_host = os.getenv('DB_HOST')
        db_user = os.getenv('DB_USER') 
        db_name = os.getenv('DB_NAME') 
        db_password = os.getenv('DB_PASSWORD')
        db_param = {
                    "db_host": db_host,
                    "db_user": db_user,
                    "db_database": db_name,
                    "db_password": db_password
                    }

        #Function call
        resp, db_obj = create_db_connection(db_param)
        return resp, db_obj
    except Exception as e:
        resp = {"msg":str(e), "status":False}
        return resp, "db_obj"

def create_db_connection(db_param):
    resp = {"msg":"", "status":False}
    db = None
    try:
        print(db_param)
        db = pymysql.connect(host=db_param["db_host"],
                            user=db_param["db_user"],
                            passwd=db_param["db_password"],
                            db=db_param["db_database"] 
                            )

        resp["msg"] = "MYSQL DB Connected Successfully !!"
        resp["status"] = True
    except Exception as e:
        resp["msg"] = str(e)
        resp["status"] = False #10061
    print(type(db))
    return resp, db

def fetch_data(db_obj,query,operation):
    resp = {"msg":"", "status":False,"data":""}
    # Function call
    try:
        db_resp = db_operation(query, db_obj, operation)
        if db_resp:
            resp["msg"] = "Record fetched successfully"
            resp["status"] = True
            resp["data"] = db_resp
        else:
            resp["msg"] = "Invalid query"
    except Exception as e:
        ##TODO
        pass
    return resp

def process_data(data,option):
    resp = {"msg":"", "status":False,"data":""}
    # Function call
    try:
        if option == "one":
            resp["msg"] = "One record sent successfully"
            resp["status"] = True
            resp["data"] = data[0]
        elif option == "many":
            resp["msg"] = "All records sent successfully"
            resp["status"] = True
            resp["data"] = data
        else:
            resp["msg"] = "Invalid record request type"
    except Exception as e:
        ##TODO
        pass
    return resp

def db_operation(query, db, operation):
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    results = ""
    
    try:
         # Execute the SQL command
        cursor.execute(query)
        # Commit your changes in the database
        if operation == "SELECT":
            try:
                results = cursor.fetchall()
                cursor.close()
            except:
                ##TODO
                return "error"
        else:
            try:
                cursor.close()
                # Commit your changes in the database
                db.commit()
                ##TODO
                return True
            except:
                #TODO
                return "error"

    except Exception as err:
        # Rollback in case there is any error
        print("exception occurred in db operation: ", err)
        db.rollback()
        ##TODO
    # disconnect from server
    return results
