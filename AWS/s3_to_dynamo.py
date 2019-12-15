import boto3, botocore.session, json, os, time
from decimal import *
from boto3.dynamodb.types import DYNAMODB_CONTEXT, TypeDeserializer
from botocore.exceptions import ClientError

def getS3Keys(BUCKET, PREFIX, download=False):
    '''Get a list of all keys in an S3 bucket
    BUCKET = 'your_s3_bucket'
    PREFIX = 'your_prefix'
    
    *Note: Including the date on 'PREFIX' is recommended not to overload the keys at once.
    '''
#     s3_client = test_session.client("s3")
    keyList = []
    kwargs = {'Bucket': BUCKET, 'Prefix': PREFIX, 
              'PaginationConfig':{'MaxItems': None,'PageSize': 1000}}
    while True:
        paginator = s3_client.get_paginator( "list_objects_v2" )
        page_iterator = paginator.paginate(**kwargs)        
        for page in page_iterator:
            if "Contents" in page:
                for key in page[ "Contents" ]:
                    keyString = key[ "Key" ]
                    keyList.append(keyString)
        try:
            kwargs['ContinuationToken'] = page['NextContinuationToken']
        except KeyError:
            break
    if download: 
        filename = '%s_%s_Keys.txt' %(BUCKET,PREFIX.replace('/','-'))
        with open(filename, 'w') as f:  
            f.writelines("%s\n" % k for k in keyList)
    return keyList

def downloadfile(lst, filename):
    with open(filename, 'w') as f: 
        f.writelines("%s\n" % l for l in lst)
    print({'location':os.getcwd(),'filename':filename})
    
def ReadfromS3(BUCKET, KEY, download=False):
    s3_object = s3_client.get_object(Bucket=BUCKET, Key=KEY) 
    s3_body = json.loads(s3_object["Body"].read().decode()) # read the data (not compressed)
    # 's3_body' output format: [{dict},{dict}, ... , {dict}]
    if download: # download the object
        s3_resource = test_session.resource('s3')
        try:
            s3_resource.Bucket(BUCKET).download_file(KEY, '%s_objects.txt' %BUCKET)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
    return {'Bucket': BUCKET, 'Key': KEY, 'Data':s3_body}

def DB_list_table():
    core_session = botocore.session.get_session()
    dynamodb = core_session.create_client('dynamodb')
    tableList = dynamodb.list_tables()['TableNames']
    tableRegion = dynamodb.describe_table(TableName=tableList[0])['Table']['TableArn'].split(':')[3]
    return {'region':tableRegion, 'list table':tableList}

def DB_read_table(TABLE_NAME):
    tableRegion = dynamodb.describe_table(TableName=TABLE_NAME)['Table']['TableArn'].split(':')[3]
    tableData = dynamodb.scan(TableName=TABLE_NAME)
    
    # re-format to "[{dict}, {dict}, ...]"
    deserializer = TypeDeserializer()
    python_data = [{k: deserializer.deserialize(v) for k,v in item.items()} for item in tableData['Items']]
    
    return {'region':tableRegion, 'table name':TABLE_NAME, 'size':len(python_data), 'data':python_data}

def DB_delete_table(del_TABLE_NAME):
    response = dynamodb.delete_table(TableName=del_TABLE_NAME)

    # wait for the table to exist
    print('Waiting for deleting "%s"' %TABLE_NAME, '...')
    waiter = dynamodb.get_waiter('table_not_exists')
    waiter.wait(TableName=TABLE_NAME)
    print('"%s" table has been deleted!' %TABLE_NAME)

def SaveNextkeys(keyList, lastkey, saveFile=False):
    KeyIdx = keyList.index(lastkey)
    NextkeyList = keyList[KeyIdx:]
    resp = 'No file'
    if saveFile:
        filename = '%s_%s_Keys.txt' %(BUCKET,lastkey.replace('/','-'))
        resp = downloadfile(NextkeyList, filename)
    return {'file':resp, 'lastKeyIndex':KeyIdx, 
            'size':len(NextkeyList), 'msg':'%s%% left' %str(round(len(NextkeyList)/len(keyList),2)*100)}

def CopytoDBtable(TABLE_NAME, keyList, filterkey='CarVi/test'):
    session = botocore.session.get_session()
    dynamodb = session.create_client('dynamodb')

    # parameters to create a new table in DynamoDB
    params = {
    'TableName' : TABLE_NAME,
    'KeySchema': [       
        { 'AttributeName': "camera_id", 'KeyType': "HASH"},    # Partition key
        { 'AttributeName': "time_stamp", 'KeyType': "RANGE" }   # Sort key
    ],
    'AttributeDefinitions': [       
        { 'AttributeName': "camera_id", 'AttributeType': "S" },
        { 'AttributeName': "time_stamp", 'AttributeType': "S" }
    ],
    'ProvisionedThroughput': {       
        'ReadCapacityUnits': 10, 
        'WriteCapacityUnits': 10
    }}

    try:
        # create the table in DynamoDB
        dynamodb.create_table(**params)

        # wait for the table to exist
        print('Waiting for creating "%s"' %TABLE_NAME, '...')
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=TABLE_NAME)
        print('"%s" table is created!' %TABLE_NAME)
    except dynamodb.exceptions.ResourceInUseException:
        # skip this if the table is existed
        print('Table "%s" already existed' %TABLE_NAME)
        pass

    # open the table to write the data
    dynamodb_resource = boto3.resource("dynamodb")
    table = dynamodb_resource.Table(TABLE_NAME)
    DYNAMODB_CONTEXT.traps[Inexact] = 0 # Inhibit Inexact Exceptions
    DYNAMODB_CONTEXT.traps[Rounded] = 0 # Inhibit Rounded Exceptions

    filtered_jsonDict = {}
    starttime = time.time()
    errorKeyList = []
    startKeyIdx = 0
    try:
        with table.batch_writer() as batch:
            for k in keyList:
                #if k.split('/')[5] in cameraID_lst: # filter the list by camera_ids
                #if str(k.split('/')[2]) >= 10: # condition to filter by date (i.e. after 09/10)
                if filterkey in k:
                    '''
                    Open the test env (access to S3 bucket in test env).
                    Load the objects from the S3 bucket.
                    *'s3_body' output format: [{dict},{dict}, ... , {dict}]
                    '''
                    s3_client = boto3.session.Session(profile_name='test').client("s3")
                    s3_object = s3_client.get_object(Bucket=BUCKET, Key=k)
                    try: s3_body = json.loads(s3_object["Body"].read().decode()) 
                    except UnicodeDecodeError as e: 
                        print("Some error occurred decoding file %s: %s" % (s3_object["Body"].read(), e))
                        pass
                    print('**Get objects**')
                    print('Key:', k)
                    '''
                    Open the qa env (access to DynamoDB table in qa env).
                    Dump the objects to DynamoDB table.
                    '''
                    for jsonDict in s3_body:
                        if isinstance(jsonDict, dict):
                            '''
                            Filter the data.
                            - exclude the sensor data
                            - transfer the data type (float,int to decimal)
                            - transfer the value type in a list (float,int to decimal)
                            - 
                            '''
                            filtered_jsonDict = {} # save the filtered data
                            for key, value in jsonDict.items():
                                if key not in SENSOR_NAME:
                                    # change the data type from float to decimal
                                    if type(value) == float: filtered_jsonDict[key] = Decimal(str(value))
                                    elif type(value) == int: filtered_jsonDict[key] = Decimal(str(value))
                                    elif type(value) == list and type(value[0]) in (int, float): 
                                        filtered_jsonDict[key] = [Decimal(str(v))for v in value]
                                    else: filtered_jsonDict[key] = value
                            try: batch.put_item(Item=filtered_jsonDict) # 'Item' has to be in a dict format
                            except ClientError as e: 
                                print('pass: duplicates', e)
                                print(filtered_jsonDict)
                                pass
                    print('upload succeeded! %d/%d' %(keyList.index(k)+1,len(keyList)))
                else: print('pass: Key does not contain required value "%s"' %filterkey)
    except KeyboardInterrupt:
        print('ERROR: KeyboardInterrupt')
        print('process is over at : ', k)
        endtime = time.time()
        totaltime = endtime - starttime
        resp = SaveNextkeys(keyList, k)
        print('Total time: ', totaltime)
        print('Total number of uploaded keys: ', len(keyList[:resp['lastKeyIndex']+1])-len(errorKeyList))
        print('Error keys: ', len(errorKeyList))
        return errorKeyList, k
    except: 
        if k == keyList[-1]: 
            print('Finished uploading ALL objects')
            endtime = time.time()
            totaltime = starttime - endtime
            print('Total time: ', totaltime)
            print('Total number of uploaded keys: ', len(keyList)-len(errorKeyList))
            print('Error keys: ', len(errorKeyList))
            return errorKeyList, k # return the last key
        else:
            print('Last Data:', filtered_jsonDict)
            resp = SaveNextkeys(keyList, k)
            print(resp)
            errorKeyList.append(keyList[startKeyIdx])
            pass
