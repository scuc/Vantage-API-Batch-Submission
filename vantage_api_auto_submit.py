#!/usr/bin/env python3

import inspect
import logging
import os
import platform
import pprint
import re
import requests
import time

import mongodb as db

from datetime import datetime
from itertools import product
from operator import itemgetter
from pathlib import Path, PurePosixPath, PureWindowsPath
from subprocess import call
from time import localtime, strftime


api_endpoint_list = ['LIGHTSPEED1', 'LIGHTSPEED2', 'LIGHTSPEED3',
                    'LIGHTSPEED4','LIGHTSPEED5', 'LIGHTSPEED6', 'LIGHTSPEED7',
                    'FNDC-VANLSG6-08','FNDC-VANLSG6-09', 'FNDC-VANLSG6-10',
                    'FNDC-VANLSG6-11'
                    ]
root_dir_win = 'T:\\\\'
root_dir_posix = '/Volumes/Quantum2/'

logger = logging.getLogger(__name__)


def clear():
    '''check and make call for specific operating system'''
    _ = call('clear' if os.name =='posix' else 'cls')


# ==================== API SUBMIT STARTS HERE ============================= #


def api_submit(total_duration, submit_frequency, jobs_per_submit, sources_in_rotation, source_dir, api_endpoint, target_workflow_id):

    jobs_per_hour = (60 / submit_frequency) * jobs_per_submit
    total_jobs = jobs_per_hour * total_duration

    print('\n' + 'This script will submit a total of ' + str(int(total_jobs)) + ' files during the next ' + str(total_duration) + ' hours\n')

    list_number = 0
    files_submitted = 0
    files_skipped = 0

    os_platform = platform_check()

    if os_platform == 'Darwin':
        posix_path = make_posix_path(source_dir)
        p = Path(posix_path)
    else:
        pass

    file_list = [x.name for x in p.glob('*.mov') if x.is_file()]
    sorted_list = sorted(file_list)

    for files_submitted in range(int(total_jobs)):
        '''Submit batches of jobs at set intervals for the duration specified.'''
        try:
            file = sorted_list[list_number]
            file_match = re.match('TEST_' + r'([0-9]{7})' + '.mov', file)
            # file_match = re.match(r'([0-9]{7})'+'.mov', file)


            if files_submitted != 0 and files_submitted % jobs_per_submit == 0:
                print('Waiting ' + str(submit_frequency) + ' minutes\n')

                time.sleep(submit_frequency * 60)

                sub_files_msg = f"Submitting Files {str(files_submitted + 1)} to {str(jobs_per_submit + files_submitted)} at {str(strftime('%H:%M:%S', localtime()))}"
                logger.info(sub_files_msg)
                print(sub_files_msg)

            if file_match is not None:
                file_submit_msg = f"Submitting: {file}"
                print(file_submit_msg)
                job_submit(target_workflow_id, source_dir, api_endpoint, file)
                files_submitted += 1
                list_number += 1
            else:
                file_skip_msg = f"Skipping: {file}"
                logger.debug(file_skip_msg)
                print(file_skip_msg)
                files_skipped += 1
                list_number += 1
                continue

        except Exception as excp:
            if excp is IndexError:
                break
            else:
                apisubmit_excp_msg = f"Exception raised on a Vantage API Submit."
                logger.exception(apisubmit_excp_msg)
                print(apisubmit_excp_msg)
                print(str(excp))
                break

    jobs_complete(files_submitted, files_skipped)


def job_submit(target_workflow_id, source_dir, api_endpoint, file):
    '''Submit the file to the workflow, using the REST API.'''

    api_endpoint = api_endpoint_check(api_endpoint)
    api_endpoint = check_vantage_status(target_workflow_id, api_endpoint)

    root_uri = "http://" + api_endpoint + ":8676"

    while True:
        try:
            job_get = requests.get(root_uri + '/REST/Workflows/' + target_workflow_id + '/JobInputs')
            if job_get is not None:
                    job_dict = job_get.json()
                    job_dict['JobName'] = file
                    job_dict['Medias'][0]['Files'][0] = source_dir + file
            else:
                continue

            job_post = requests.post(root_uri + '/REST/Workflows/' + target_workflow_id + '/Submit',json=job_dict)

            job_post_response = job_post.json()

            job_id = job_post_response['JobIdentifier']
            job_id_msg = f"Submitting {file} | job id: {job_id}"
            logger.info(job_id_msg)

            # sleep gives Vantage job time to set values.
            time.sleep(1)
            document = db.create_doc(job_id, api_endpoint)

            document_msg = f"{document}"
            logger.info("Job values submitted to db: " + document_msg)

            break

        except requests.exceptions.RequestException as excp:
            jobsubmit_excp_msg = f"Exception raised on a Vantage Job Submit."
            logger.exception(jobsubmit_excp_msg)
            print(jobsubmit_excp_msg)
            print(str(excp))
            api_endpoint = api_endpoint_failover(api_endpoint)
            job_submit(target_workflow_id, source_dir, api_endpoint, file)
            break

    return api_endpoint
