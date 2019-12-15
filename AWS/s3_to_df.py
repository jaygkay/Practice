import boto3
def dump_s3_data(env, bucket, imei):
    env= boto3.session.Session(profile_name=str(env))
    s3 = env.client('s3')
    paginator = s3.get_paginator('list_objects')
    #for page in page_iter:
    if utc_start_hour != utc_end_hour:
        #print('NOT SAME')
        aws_hours = []
        for i in range(int(utc_start_hour), int(utc_end_hour)+1):
            aws_hours.append(str(i).zfill(2))

        prefix_lst = []
        prefix_lst.append(prefix_carvi_event + utc_start_hour)
        for i in aws_hours:
            prefix_lst.append(prefix_carvi_all+str(i))
            #### prefix_carvi_crash
        prefix_lst.append(prefix_carvi_end + utc_end_hour)
        prefix_lst
    else:
        #print('SAME')
        prefix_lst = [prefix_carvi_event + utc_start_hour,
                      prefix_carvi_all + utc_start_hour,
                      prefix_carvi_crash + utc_start_hour,
                      prefix_carvi_end + utc_start_hour]
    key_lst = []
#    print(prefix_lst)
    print(colored("S3 Bucket for This","blue"), colored(imei, "blue", attrs = ['bold']))
    print(colored("reading keys from S3 buckets", "green"))
    for prefix_i in prefix_lst:
        print(prefix_i)
        params = {'Bucket' : bucket, 'Prefix' : prefix_i}
        page_iter = paginator.paginate(Bucket = params['Bucket'],
                                       Prefix = params['Prefix'])
        for page in page_iter:
            try:
                key_lst += [(x['Key']) for x in page['Contents']]
            except KeyError:
                error = prefix_i.split('/')
                #print(error)
                print(colored(error[6],"blue",attrs = ['bold']), colored("does not exist in the bucket", "blue"))
                print(colored(prefix_i, "red",attrs = ['bold']))
                continue
    print(colored("Dumping S3 Data to a DataFrame", "green"))
    df2 = pd.DataFrame()
    for i in key_lst:
        #print(i)
        response = s3.get_object(Bucket = bucket, Key = i)
        result = response['Body'].read()
        #print("********", result)
        try:
            trip_json = json.loads(result)
            if isinstance(trip_json, dict):
                #if trip_json['trip_start'] == trip_start[:-1]: 
                df2 = df2.append(trip_json,ignore_index=True )
            else:
                for trip in trip_json:
#                     if trip['trip_start'] == trip_start[:-1]: 
                    df2 = df2.append(trip,ignore_index=True )
            
        except json.JSONDecodeError:
            error  = trip_json
            print(error)
        
#     df2 = df2.sort_values(['time_stamp'])
    df2 = df2.reset_index()
    return df2
