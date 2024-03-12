import sys
import requests
import json
import csv
import pandas as pd
import redshift_connector
import time
import concurrent.futures
import threading

# Set Redshift variables
host='default-workgroup.967842132715.us-east-2.redshift-serverless.amazonaws.com'
port = 5439
database = "redshift_demo"
schema = "public"
table = "demo_table_large"
connection = "redshift-connection-upass"
user='admin'
password='Redshift1!'
limit=20000
col_string = "column"

# Set Spark variables
url_batch = "https://excel.test.coherent.global/coherent/api/v4/batch"
bearer_token = "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJaeHVzMXg5eXo3MXNNd3JyVWVqNldwcWducms5cmZGWERINHRVNDN0QVBrIn0.eyJleHAiOjE3MDgwMTg1MjcsImlhdCI6MTcwODAxMTMyNywiYXV0aF90aW1lIjoxNzA4MDExMzIyLCJqdGkiOiI0ZDIwYTg5Yy1lMmIxLTRhNmYtODBiMC1kYzcyMTE0MGUzMjkiLCJpc3MiOiJodHRwczovL2tleWNsb2FrLnRlc3QuY29oZXJlbnQuZ2xvYmFsL2F1dGgvcmVhbG1zL2NvaGVyZW50IiwiYXVkIjpbInByb2R1Y3QtZmFjdG9yeSIsInRlc3QtY2xpZW50IiwiYWNjb3VudCJdLCJzdWIiOiIzYTNlZDA3YS0zNGZhLTRkODktODFiOS04YzgxYTU0MzVlZTIiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJwcm9kdWN0LWZhY3RvcnkiLCJub25jZSI6Ijk0Y2Q3ZjhhLWU2NzEtNGE2ZS1hYzViLWE0ZDRlMzk3NDg5YyIsInNlc3Npb25fc3RhdGUiOiI3YTJiMzI2OC1jYTAyLTQyZTMtYmE2Ni0xMzkwNTE2YTE4ODciLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbImh0dHBzOi8vbW9kZWxpbmctY2VudGVyLmRldi5jb2hlcmVudC5nbG9iYWwiLCJodHRwczovL3NhLmRldi5jb2hlcmVudC5nbG9iYWwiLCJodHRwczovL2NvcGlsb3Quc3RhZ2luZy5jb2hlcmVudC5nbG9iYWwiLCJodHRwczovL2NvcGlsb3QuZGV2LmNvaGVyZW50Lmdsb2JhbCIsImh0dHBzOi8vc3BhcmstdXNlci1tYW5hZ2VyLnRlc3QuY29oZXJlbnQuZ2xvYmFsIiwiaHR0cHM6Ly9zYS5zdGFnaW5nLmNvaGVyZW50Lmdsb2JhbCIsImh0dHBzOi8vc3BhcmsudGVzdC5jb2hlcmVudC5nbG9iYWwiLCJodHRwOi8vbG9jYWxob3N0OjMwMDAiLCJodHRwczovL3NwYXJrLXVzZXItbWFuYWdlci50ZXN0LnllbGxvdy5jb2hlcmVudC5nbG9iYWwiLCJodHRwczovL21vZGVsaW5nLWNlbnRlci5zdGFnaW5nLmNvaGVyZW50Lmdsb2JhbCJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiZGVmYXVsdC1yb2xlcy1jb2hlcmVudCIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJ0ZXN0LWNsaWVudCI6eyJyb2xlcyI6WyJ0ZXN0LXVzZXI6cGYiXX0sImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIGVtYWlsIHByb2ZpbGUiLCJzaWQiOiI3YTJiMzI2OC1jYTAyLTQyZTMtYmE2Ni0xMzkwNTE2YTE4ODciLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwibmFtZSI6Ik5pY2sgU3Rhbnppb25lIiwiZ3JvdXBzIjpbInVzZXI6cGYiXSwicmVhbG0iOiJjb2hlcmVudCIsInByZWZlcnJlZF91c2VybmFtZSI6Im5pY2suc3Rhbnppb25lQGNvaGVyZW50Lmdsb2JhbCIsImdpdmVuX25hbWUiOiJOaWNrIiwiZmFtaWx5X25hbWUiOiJTdGFuemlvbmUiLCJlbWFpbCI6Im5pY2suc3Rhbnppb25lQGNvaGVyZW50Lmdsb2JhbCJ9.ZqSZryoBluZPeWfgnkHr_4Ve8o4dicfg53tT_pJClI6d3cSVRbAPyUjMjZXNRz_hhaKXBcYpC4UVSEE4mI920EaNoAB4HAb79_8oKzfre5bPRMhk1JM_MY4Khl8ZF-actsmzj7zMiXn0hBbfGJKWn6DKda3Ag9TrAs-D5pxHdk53VRDvbAE4sdsdEZUDNFxslIaDU3ErBJXqHc2W-UtJv97Q4sLuHGyAsOXd-ING6BL1Q-vcIQ0I_5hGnkdc5kMMbPH_rcqswWnmE4TQnyYZIFhfjKIxBeBxrtrnW12KjcmZ9du1vkX23rs5gplUy93BYbe2kvUf26-vNi2yswWIUQ"
spark_service = "DemoStanz/basic_term_sample"
version_id = "eeef4f7b-c191-46f7-831d-41b26291e58f"
headers_batch = {
    'Content-Type': 'application/json',
    'Authorization': bearer_token
}

payload_batch = {
    'service_uri': spark_service,
    'version_id': version_id,
    'source_system': 'StanzRedshift', 
    'call_purpose': 'batch_api'
}

def main():
    
    run_start = time.time()
    
    # Connects to Redshift cluster using AWS credentials
    start = time.time()
    conn = redshift_connector.connect(
        host=host,
        port=port,
        database=database,
        user='admin',
        password='Redshift1!',
    )

    print(conn) 
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {database}.{schema}.{table} LIMIT {limit};")
    result = cursor.fetchall()
    cursor.execute(f"""select "column" from pg_table_def where tablename = '{table}'""")
    columns = cursor.fetchall()

    end = time.time()
    print(f"Get Redshift Data: {(end-start)}s")


    # print(result, type(result))
    # print(columns, type(columns))

    col_arr = [i[0] for i in list(columns)]
    df = pd.DataFrame(list(result), columns=col_arr)
    print(len(df))
    print(df.info())
    
    batch = generate_batch(df)
    print(batch[:10])
    print(len(batch))

    start = time.time()
    outputs = run_batch(batch)
    print(len(outputs))
    end = time.time()
    print(f"Spark: {(end-start)}s")

    for i in range(len(outputs)):
        print(len(outputs[i]))
        
    print(len(outputs[0][0]))

    run_end = time.time()
    print(f"Total: {(run_end-run_start)}s")


def create_batch():
    """
    Helper to both create the batch and send input data
    Returns batch_id to be used in downstream functions
    
    """
    create_response = requests.post(url_batch, headers=headers_batch, json=payload_batch)
    cr = create_response.json()
    batch_id = cr['id']
    
    return batch_id


def send_batch_inputs(batch_id, inputs):
    """
    Helper to send input data
    Returns records submitted to be used in downstream functions
    
    """
    input_url = f'{url_batch}/{batch_id}/data'
    input_payload = {
        'inputs': inputs
    }
    input_response = requests.post(input_url, headers=headers_batch, json=input_payload)
    ir = input_response.json()
    rs = ir['record_submitted']
    
    return rs, ir


def get_batch_status(batch_id):
    """
    Helper to grab the current status of the batch
    
    """
    status_url = f'{url_batch}/{batch_id}/status'
    status_response = requests.get(status_url, headers=headers_batch)
    sr = status_response.json()
    
    return sr


def get_batch_results(batch_id):
    """
    Helper to grab the outputs from the batch run
    
    """
    output_url = f'{url_batch}/{batch_id}/result'
    output_response = requests.get(output_url, headers=headers_batch, params={'max':100000})
    out = output_response.json()
    count = out['count']
    outputs = out['outputs']
    
    return count, outputs, out


def close_batch(batch_id):
    """
    Helper to close the batch
    
    """
    close_url = f'{url_batch}/{batch_id}/'
    close = {"batch_status" : "closed"}
    close_response = requests.patch(close_url, headers=headers_batch, json=close)
    cr = close_response.json()
    status = cr['batch_status']
    
    return status


# Get JSON into V4 format
def json_to_arr(js):
    """
    Helper to transform JSON into V4 format
    
    """
    output = []
    cols = list(js[0].keys())
    output.append(cols)
    for i in range(len(js)):
        rec = []
        dict = js[i]
        keys = dict.keys()
        for k in keys:
            val = dict[k]
            rec.append(val)
        output.append(rec)
    
    return output

def to_json(inputs, request_meta):
    """
     Turn dataframe into JSON data structure
    
    """
    data_js = inputs.to_json(orient='records')
    data_ls = eval(data_js)
    #Create array of JSON requests
    req = []
    for i in range(len(inputs)):
        request_data = {}
        y = data_ls[i]
        request_data['inputs'] = y
        request = {
            'request_data': request_data,
            'request_meta': request_meta
        }
        req.append(request)
    return req

def pd_to_arr(df, flag=0):
    """
    Helper to transform dataframe into V4 format
    Specific function to inner.py

    Note the specificity is around the position (last column) of the scenario in the dataframe
    Future imporvement: Make the position of the json columns as an input array into the function 
    
    """
    output = [] # Initialize array output
    cols = df.columns.tolist() # Gather columns from dataframe
    output.append(cols)
    for i in range(len(df)): # Loop through records of dataframe
        rec = df.iloc[i].tolist() # Convert current row elements into a list
        if flag == -1: # Specifc for GenerateInners.py. Converts the first element in row into an embedded array.
            yc = rec.pop(0) # Grab the first element of the list
            js_yc = json.loads(yc)
            y = json_to_arr(js_yc)
            rec.insert(0, y)           
        if flag == 1: # Specifc for inner.py. Converts the last element in row into an embedded array.
            scen = rec.pop() # Grab the last element of the list
            js = json.loads(scen)
            x = json_to_arr(js)
            rec.append(x)
        output.append(rec)   
    
    return output


def generate_batch(df):
    df = df.astype({'ageentry': 'float', 'policyterm': 'float','sumassured': 'float'})
    batch = pd_to_arr(df)
    return batch



def run_batch(batch):

    batch_id = create_batch()

    # Intitialize variables for batch output loop
    
    records_submitted = 0
    records_completed = 0
    batch_records_gathered = 0
    
    # Not Threading: UAT, loop every 100K
    outputs = []
    if len(batch) <= 100000:
        records_submitted, input_response = send_batch_inputs(batch_id, batch)
        print(input_response)

        while batch_records_gathered < records_submitted:
            try:
                records_gathered, output, out_response = get_batch_results(batch_id)
                output.pop(0)
                outputs.append(output)        
                batch_records_gathered += records_gathered
                print(f'Records Gathered: {batch_records_gathered}')

            except KeyError or NameError:
                sr = get_batch_status(batch_id)
                print(f'Get Failed: {sr}')

    else:
        cols = batch[0]
        batch_size = 100000
        for i in range(0, len(batch), batch_size):
            print(i)
            sub_batch = batch[i+1:i+1+batch_size]
            sub_batch.insert(0, cols)
            records_submitted, input_response = send_batch_inputs(batch_id, sub_batch)
            print(input_response)

            while batch_records_gathered < records_submitted:
                try:
                    records_gathered, output, out_response = get_batch_results(batch_id)
                    output.pop(0)
                    outputs.append(output)        
                    batch_records_gathered += records_gathered
                    print(f'Records Gathered: {batch_records_gathered}')

                except KeyError or NameError:
                    sr = get_batch_status(batch_id)
                    print(f'Get Failed: {sr}')


    sr = get_batch_status(batch_id)
    rc = sr['records_completed']
    records_completed = rc

    # Check to ensure number of inputs = number processed = number retrieved
    if records_submitted == records_completed and records_completed == batch_records_gathered:
        print("Run Completed! Inputs = Outputs.")
    else:
        print("Error I/O Mismatch")
        print(records_submitted, records_completed, batch_records_gathered)

    # Close batch
    status = close_batch(batch_id)
    print(status)

    return outputs

main()


    
# def get_batch_results_with_lock(batch_id, api_lock):
#     with api_lock:
#         return get_batch_results(batch_id)

# def send_and_get_batch_results(batch_id, batch):
#     # Send batch inputs in a separate thread
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         send_future = executor.submit(send_batch_inputs, batch_id, batch)
#         records_submitted, input_response = send_future.result()  # Wait for the send_batch_inputs function to complete
#         print(input_response)
    
#     # Get batch results in multiple threads
#     outputs = []
#     batch_records_gathered = 0
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         get_futures = [executor.submit(get_batch_results, batch_id) for _ in range(50)]

#         # Wait for all get_batch_results threads to complete
#         concurrent.futures.wait(get_futures)
        
#         # Retrieve results
#         for future in get_futures:
#             try:
#                 output = future.result()
#                 outputs.append(output[1])
#                 batch_records_gathered += output[0]
#             except Exception as e:
#                 print(f"Error in get_batch_results: {e}")

#     return records_submitted, outputs, batch_records_gathered

# def threads_get_batch(batch_id):
#     # Get batch results in multiple threads
#     outputs = []
#     records_gathered = 0
#     api_lock = threading.Lock()
#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         get_futures = [executor.submit(get_batch_results_with_lock, batch_id, api_lock) for _ in range(50)]

#         # Wait for all get_batch_results threads to complete
#         concurrent.futures.wait(get_futures)
        
#         # Retrieve results
#         for future in get_futures:
#             try:
#                 output = future.result()
#                 outputs.append(output[1])
#                 records_gathered += output[0]
#             except Exception as e:
#                 print(f"Error in get_batch_results: {e}")

#     return outputs, records_gathered



    ## Threading: Download Only
    # outputs = []
    # cols = batch[0]
    # batch_size = 500000
    # for i in range(0, len(batch), batch_size):
    #     print(i)
    #     sub_batch = batch[i+1:i+1+batch_size]
    #     sub_batch.insert(0, cols)
    #     records_submitted, input_response = send_batch_inputs(batch_id, sub_batch)
    #     print(input_response)
    #     while batch_records_gathered < records_submitted:
    #         output, records_gathered = threads_get_batch(batch_id)
    #         outputs.append(output)
    #         batch_records_gathered += records_gathered
    #         print(f'Records Gathered: {batch_records_gathered}')


    ## Threading: Full input and output in same function
    # records_submitted, outputs, batch_records_gathered = send_and_get_batch_results(batch_id, batch)


    # ## Test: send 500K loop, fetch every 10K
    # outputs = []
    # if len(batch) <= 500000:
    #     records_submitted, input_response = send_batch_inputs(batch_id, batch)
    #     print(input_response)
    # else:
    #     cols = batch[0]
    #     batch_size = 500000
    #     for i in range(0, len(batch), batch_size):
    #         print(i)
    #         sub_batch = batch[i+1:i+1+batch_size]
    #         sub_batch.insert(0, cols)
    #         records_submitted, input_response = send_batch_inputs(batch_id, sub_batch)
    #         print(input_response)

    #         while batch_records_gathered < records_submitted:
    #             try:
    #                 # records_gathered, output, out_response = get_batch_results(batch_id)
    #                 # output.pop(0)
    #                 output, records_gathered = threads_get_batch(batch_id)
    #                 outputs.append(output)        
    #                 batch_records_gathered += records_gathered
    #                 print(f'Records Gathered: {batch_records_gathered}')

    #             except KeyError or NameError:
    #                 sr = get_batch_status(batch_id)
    #                 print(f'Get Failed: {sr}')
