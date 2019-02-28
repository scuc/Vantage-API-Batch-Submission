
import json
import pprint
import pymongo
import requests

from datetime import datetime
from pymongo import MongoClient, InsertOne, DeleteOne, ReplaceOne

client = pymongo.MongoClient("mongodb://localhost:27017")

def create_doc(job_id, api_endpoint):

    db = client.vantage
    collection = db.dalet

    document_get = requests.get('http://' + str(api_endpoint) + ':8676/REST/Jobs/' + job_id)

    document = document_get.json()

    document = document["Job"]

    print("DOCUMENT1: " + str(document))

    document = set_values(document)

    print("DOCUMENT3: " + str(document))

    try:
        collection.insert_one(document)
    except Exception as e:
        print("Unexpected Error in create_doc():", type(e), e)

    return document

def set_values(job):

    print("DOCUMENT2: " + str(job))

    state = job['State']
    started = job['Started']
    updated = job['Updated']

    if state is 0:
        state = "In Process"
    if state is 4:
        state = "Failed"
    if state is 5:
        state = "Complete"
    if state is 6:
        state = "Waiting"
    if state is 7:
        state = "Stopped by User"
    if state is 8:
        state = "Waiting to Retry"

    started_slice = started[6:19]
    timestamp = (int(started_slice)/1000)
    started_dt = str((datetime.utcfromtimestamp(timestamp)))
    job['Started'] = started_dt

    updated_slice = updated[6:19]
    timestamp = (int(updated_slice)/1000)
    updated_dt = str((datetime.utcfromtimestamp(timestamp)))
    job['Updated'] = updated_dt

    return job

def update_db(api_endpoint, target_workflow_id):

    db = client.vantage
    collection = db.dalet

    doc_count = collection.count_documents({})
    print("DOC_COUNT: " + str(doc_count))

    if doc_count is not 0:

        job_get = requests.get('http://' + str(api_endpoint) + ':8676/REST/Workflows/' + target_workflow_id + '/Jobs/?filter={All}')

        # with open('joblist.json') as json_data:
        #     d = json.load(json_data)

        job_json = job_get.json()

        print("GET ALL JOBS")
        pprint.pprint(job_json)

        for job in job_json['Jobs']:
            job = set_values(job)
            print("DOCUMENT4: " + str(job))

            print("")
            pprint.pprint(job)
            print("")

            identifier = job['Identifier']
            ismonitor = job['IsMonitor']
            name = job['Name']
            started = job['Started']
            state = job['State']
            updated = job['Updated']

            print(identifier)
            print(ismonitor)
            print(name)
            print(started)
            print(state)
            print(updated)

            try:
                count = collection.count_documents({"Identifier": identifier})

                query = {"Identifier": identifier}
                values = {"$set": {"State": state, "Updated": updated}}

                if count is not 0:
                    collection.update_one(query, values)
                    print('updating now')
                else:
                    create_doc(job['Identifier'], api_endpoint)

                if state == 4:
                    error_get = requests.get('http://' + str(api_endpoint) + ':8676/REST/Jobs/' + identifier + '/ErrorMessage')
                    error_dict = error_get.json()
                    error_msg = error_dict['JobErrorMessage']
                    values = {"$set": {"ErrorMessage": error_msg}}
                    collection.update_one(query, values)

                if state in [5,6,7,8]:
                    metrics_get = requests.get('http://' + str(api_endpoint) + ':8676/REST/Jobs/' + identifier + '/Metrics')
                    metrics_blob = metrics_get.json()
                    total_queue_time = metrics_blob['TotalQueueTimeInSeconds']
                    total_run_time = metrics_blob['TotalRunTimeInSeconds']
                    values = {"$set": {"metrics": [{"TotalQueueTimeInSeconds": total_queue_time, 'TotalRunTimeInSeconds': total_run_time}],"State": state, "Updated": updated}}
                    collection.update_one(query, values)
                else:
                    pass
            except Exception as err:
                print("Unexpected Error in update_db:", type(err), err)
            break
        else:
            pass
    return

# update_db("lightspeed1","31441afe-a641-48b8-a34c-40bdb2b03672/")

'''
{'Identifier': '0566d9d0-515f-4590-b3be-a718e5c9f530',
'IsMonitor': False,
'Name': '8228266.mov',
'Started': '/Date(1549019142830-0500)/',
'State': 5,
'Updated': '/Date(1549019770647-0500)/'}]}


Identifier = Guid  The unique identifier for the job.

IsMonitor =
        Boolean  A Boolean value which indicates whether the job corresponds to a Monitor type transaction (i.e.: a Supervisor task which is ultimately responsible for the creation of other jobs). A value of true indicates this is a Monitor job, while a value of false implies that the job in question is a more traditional (normal) Vantage job.
Name = String  The job name
       (the name which appears in the Vantage Workflow Designer)
Started = DateTime The timestamp of when the job was created.
State = JobState The current state of the job (see below)
Updated DateTime = The timestamp of when the job was last updated.
'''

'''Job State Value Meaning

0 = In Process - The job is currently active and in-process (it contains actions which are currently being processed).

4 = Failed - The job has failed.

5 = Complete - The job has successfully completed.

6 = Waiting - The job is active, but is currently waiting for a resource. More specifically the job contains actions which are all awaiting being allocated to a service for processing.

7 = Stopped by User - The job was stopped by a user/Administrator.

8 = Waiting to Retry
The job has entered a state where the remaining actions are waiting to be retried. This is typically the result of a Retry rules being applied to one or more actions in a job (eg: retry an FTP transfer after 10 minutes if the target site is not accessible).'''


# job_info = {'Attachments': [],
#  'JobName': '9469852.mov',
#  'Labels': [],
#  'Medias': [{'Data': 'If sending in-band data (eg: CML); place a UTF8 BASE64 '
#                      'encoded version of the data in this field and do not '
#                      'send a file path.',
#              'Description': 'The original version of content encountered or '
#                             'created by Vantage.',
#              'Files': ['Z://ProxyFiles/renamed_PROD_PROXY_MOV/9469852.mov'],
#              'Identifier': '3c73367b-059c-45aa-ad7a-72a84ae40921',
#              'Name': 'Original'}],
#  'Priority': 0,
#  'Variables': []}
