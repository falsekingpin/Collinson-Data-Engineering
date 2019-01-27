# Collinson Data Engineering Assignment

This repo contains code corresponding to solution for the assignment given by Collinson.
The flow for the project:
    1. Given a folder, it has many sub-directories which have zipped files
    2. Each of the zip file contains information obtained from the twitter streaming api
    3. Individual zip file is extracted and read
    4. After reading from unzipped file, the content is cleaned for relevant data
    5. After data cleaning, it is inserted into the RDS
    6. For retrieving the data, there is an API Gateway with lambda integration
    7. User hits the API endpoint with a query and gets records in response to his query

## Prerequisites

- configparser==3.5.0
- PyMySQL==0.9.2
- requests==2.18.4


## Endpoint, Request & Response

ENDPOINT :  http://url/v1/fetch_rds_data

A typical Request to this service look something like this :

```json    
    {
    "query": "SELECT * from tweet_tags",
    "type": "select",
    "records": "one"
}
```
*Note:* The API fecthes all data.

A typical Response to this service look something like this :

```json
    {
  "SUCCESS": [
    {
      "PROCESS": "REQUEST VALIDATION",
      "STATUS_CODE": 200,
      "MESSAGE": "Requested data validated successfully"
    },
    {
      "PROCESS": "DB CONFIGURATION",
      "STATUS_CODE": 200,
      "MESSAGE": "MYSQL DB Connected Successfully !!"
    },
    {
      "PROCESS": "FETCH DATA",
      "STATUS_CODE": 200,
      "MESSAGE": "Record fetched successfully"
    },
    {
      "PROCESS": "PROCESS DATA",
      "STATUS_CODE": 200,
      "MESSAGE": "One record sent successfully"
    },
    {
      "QUERY_DATA": ["some_data"],
  "ERROR": []
}
```


### For inserting the data in RDS

```sh
python clean_parse_data.py "/path/to/sourcedirectoryforallsubdirectories"
```

### For querying data

```sh
python call_api.py
```

*Note:*

    1. For executing both the files, we need to have their .ini files in place
