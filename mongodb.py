
import datetime
import json
import pprint
import pymongo
import requests

from pymongo import MongoClient, InsertOne, DeleteOne, ReplaceOne

client = pymongo.MongoClient("mongodb://localhost:27017")

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

def create_doc(document):
    db = client.vantage
    collection = db.dalet

    # pprint.pprint(job_info)
    # print()

    # job_name = job_info['JobName']
    # files = job_info['Files']
    # identifier = job_info['Identifier']

    # document = {'JobName': job_info['JobName'],
    #             'Fies': job_info['Files'],
    #             'Identifier': job_info['Identifier']
    #             }

    # pprint.pprint(document)
    # print(type(document))

    try:
        db.dalet.insert_one(document)
        # db.dalet.find_one()
    except Exception as e:
        print("Unexpected Error in create_doc():", type(e), e)

def update_doc(root_uri, target_workflow_id):

    db = client.vantage
    collection = db.dalet

    job_get = requests.get(root_uri + '/REST/Workflows/' + target_workflow_id + '/Jobs/?filter={All}')

    job_blob = job_get.json()

    for job in job_blob['Jobs']:
        job = dict(job)

        identifier = job['Identifier']
        ismonitor = job['IsMonitor']
        name = job['Name']
        started = job['Started']
        state = job['State']
        updated = job['Updated']

        # print(" ")
        # pprint.pprint(job)
        # print(" ")

        try:
            count = collection.count_documents({"Identifier": identifier})

            query = {"Identifier": identifier}
            values = {"$set": {"State": state},
                    "Updated": updated}

            if count is not 0:
                collection.update_one(query, values)

            if state == 4:
                error_get = requests.get(root_uri + '/REST/Jobs/' + identifier + '/ErrorMessage')
                error_blob = error_get.json()
                error_msg = error_blob['JobErrorMessage']
                values = {"$set": {"ErrorMessage": error_msg}}
                collection.update_one(query, values)

            if state in [5,6,7,8]:
                metrics_get = requests.get(root_uri + '/REST/Jobs/' + identifier + '/Metrics')
                metrics_blob = metrics_get.json()
                total_queue_time = metrics_blob['TotalQueueTimeInSeconds']
                total_run_time = metrics_blob['TotalRunTimeInSeconds']
                values = {"$set": {"metrics": [{"TotalQueueTimeInSeconds": total_queue_time, 'TotalRunTimeInSeconds': total_run_time}],"State": state, "Updated": updated}}
                collection.update_one(query, values)
            else:
                pass
        except Exception as err:
            print("Unexpected Error in update_doc:", type(e), e)


'''
{'Identifier': '984cf6a0-83e9-4373-af6e-fff135468d4c',
'IsMonitor': False,
'Name': '8228266.mov',
'Started': '/Date(1549019142830-0500)/',
'State': 5,
'Updated': '/Date(1549019770647-0500)/'}]}


Identifier
        Guid  The unique identifier for the job.

IsMonitor
        Boolean  A Boolean value which indicates whether the job corresponds to a Monitor type transaction (i.e.: a Supervisor task which is ultimately responsible for the creation of other jobs). A value of true indicates this is a Monitor job, while a value of false implies that the job in question is a more traditional (normal) Vantage job.
Name
    String  The job name (the name which appears in the Vantage Workflow Designer)
Started
        DateTime The timestamp of when the job was created.
State
        JobState The current state of the job (see below)
Updated DateTime
        The timestamp of when the job was last updated.
'''

'''Job State Value Meaning

0 = In Process - The job is currently active and in-process (it contains actions which are currently being processed).

4 = Failed - The job has failed.

5 = Complete - The job has successfully completed.

6 = Waiting - The job is active, but is currently waiting for a resource. More specifically the job contains actions which are all awaiting being allocated to a service for processing.

7 = Stopped by User - The job was stopped by a user/Administrator.

8 = Waiting to Retry
The job has entered a state where the remaining actions are waiting to be retried. This is typically the result of a Retry rules being applied to one or more actions in a job (eg: retry an FTP transfer after 10 minutes if the target site is not accessible).'''
