#!/usr/bin/env python3

import logging
import os
import re
import requests
import time

import config as cfg
import mongodb as db

from datetime import datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from time import localtime, strftime

import input_validation as inpval
import system_checks as sysch

config = cfg.get_config()

endpoint_list = config['endpoint_list']
root_dir_win = config['paths']['root_dir_win']
root_dir_posix = config['paths']['root_dir_posix']

endpoint = sysch.get_endpoint()

logger = logging.getLogger(__name__)


def submit_control(submit_frequency, jobs_per_submit, source_dir, target_workflow_id):
    """
    Submit batches of jobs to Vantage at set intervals. 
    """

    list_number = 0
    files_submitted = 0
    files_skipped = 0

    os_platform = inpval.platform_check()

    if os_platform == 'Darwin':
        posix_path = inpval.make_posix_path(source_dir)
        p = Path(posix_path)
    else:
        pass

    file_list = [x.name for x in p.glob('*.mov') if x.is_file()]
    sorted_list = sorted(file_list)

    total_jobs = len(sorted_list)
    jobs_per_hour = (60 / submit_frequency) * jobs_per_submit
    total_duration = (total_jobs / jobs_per_hour)

    total_jobs_msg = f"Submitting a total of {str(total_jobs)} files during the next {str(total_duration)} hours\n"
    logger.info(total_jobs_msg)

    for files_submitted in range(int(total_jobs)):
        try:
            vid_file = sorted_list[list_number]
            file_match = re.match('TEST_' + r'([0-9]{7})' + '.mov', vid_file)
            # file_match = re.match(r'([0-9]{7})'+'.mov', vid_file)


            if files_submitted != 0 and files_submitted % jobs_per_submit == 0:
                print('Waiting ' + str(submit_frequency) + ' minutes\n')

                time.sleep(submit_frequency * 60)

                sub_files_msg = f"Submitting Files {str(files_submitted + 1)} to {str(jobs_per_submit + files_submitted)} at {str(strftime('%H:%M:%S', localtime()))}"
                logger.info(sub_files_msg)
                print(sub_files_msg)

            if file_match is not None:
                file_submit_msg = f"Submitting: {vid_file}"
                print(file_submit_msg)
                job_submit(target_workflow_id, source_dir, endpoint, vid_file)
                files_submitted += 1
                list_number += 1
            else:
                file_skip_msg = f"Skipping: {vid_file}"
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


def job_submit(target_workflow_id, source_dir, endpoint, vid_file):
    '''Submit the file to the workflow, using the REST API.'''

    # endpoint = endpoint_check(endpoint)
    # endpoint = check_vantage_status(target_workflow_id, endpoint)

    root_uri = "http://" + endpoint + ":8676"

    while True:
        try:
            job_get = requests.get(root_uri + '/REST/Workflows/' + target_workflow_id + '/JobInputs')
            if job_get is not None:
                    job_dict = job_get.json()
                    job_dict['JobName'] = vid_file
                    job_dict['Medias'][0]['Files'][0] = source_dir + vid_file
            else:
                continue

            job_post = requests.post(root_uri + '/REST/Workflows/' + target_workflow_id + '/Submit',json=job_dict)

            job_post_response = job_post.json()

            job_id = job_post_response['JobIdentifier']
            job_id_msg = f"Submitting {vid_file} | job id: {job_id}"
            logger.info(job_id_msg)

            # sleep gives Vantage job time to set values.
            time.sleep(1)
            document = db.create_doc(job_id, endpoint)

            document_msg = f"{document}"
            logger.info("Job values submitted to db: " + document_msg)

            break

        except requests.exceptions.RequestException as excp:
            jobsubmit_excp_msg = f"Exception raised on a Vantage Job Submit."
            logger.exception(jobsubmit_excp_msg)
            # endpoint = endpoint_failover(endpoint)
            # job_submit(target_workflow_id, source_dir, endpoint, vid_file)
            break

    return endpoint


if __name__ == '__main__':
    submit()
