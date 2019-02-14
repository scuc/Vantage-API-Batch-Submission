#!/usr/bin/env python3

import inspect
import logging
import os
import platform
import re
import requests
import time

from datetime import datetime
from itertools import product
from operator import itemgetter
from pathlib import Path, PurePosixPath, PureWindowsPath
from subprocess import call
from time import localtime, strftime

global global_api_endpoint
global api_endpoint_list
global domain_load_val
global root_uri
global sorted_serviceload_list

api_endpoint_list = ['LIGHTSPEED1', 'LIGHTSPEED2', 'LIGHTSPEED3',
                    'LIGHTSPEED4','LIGHTSPEED5', 'LIGHTSPEED6', 'LIGHTSPEED7',
                    'FNDC-VANLSG6-08','FNDC-VANLSG6-09', 'FNDC-VANLSG6-10',
                    'FNDC-VANLSG6-11'
                    ]
root_dir_win = 'T:\\\\'
root_dir_posix = '/Volumes/Quantum2/'


def clear():
    '''check and make call for specific operating system'''
    _ = call('clear' if os.name =='posix' else 'cls')


# =================== BEGIN PROMPT FOR USER INPUT =================== #


def print_intro(logger):

    print("=========================================================== " + "\n"
          + '''           Vantage Workflow Submission Script \n
                Version 2.0, January 16, 2019\n
    This script will use the Vantage REST API to submit files\n
    to a workflow at set intervals. The user defines the \n
    duration, frequency, and total number of jobs submitted.''' + "\n"
    + "================================================================")


    while True:
        st = str(input("Job Startime, 24hr Clock (YYYYMMDDhhmm or Year,Month,Day,hour,minute): "))

        present = datetime.now()

        try:
            clean_st = clean_datetimes(st)

            if clean_st == 0:
                continue

            else:
                year = int(clean_st[0:4])
                month = int(clean_st[4:6])
                day = int(clean_st[6:8])
                hour = int(clean_st[8:10])
                minute = int(clean_st[10:12])

                print(year, month, day, hour, minute)

                start_time = datetime(year, month, day, hour, minute)
                logger.debug("testlog: " + str(start_time))

            if start_time < present:
                print("Start Time must be a time in the future, try again.")
                continue
            else:
                break
        except ValueError:
            print("ValueError, try again")
            continue

    while True:
        global total_duration
        total_duration = str(input("Total Duration (hrs): "))
        try:
            if total_duration.find(".")== 1:
                total_duration = float(total_duration)
            else:
                total_duration = int(total_duration)
            break
        except ValueError:
            print("{} is not a valid entry for total duration, try again.".format(total_duration))
            continue

    while True:
        global submit_frequency
        submit_frequency = str(input("Job Submission Interval (min): "))
        try:
            if submit_frequency.find(".")== 1:
                submit_frequency = float(submit_frequency)
            else:
                submit_frequency = int(submit_frequency)
            break
        except ValueError:
            print("{} is not a valid entry for total duration, try again.".format(submit_frequency))
            continue

    while True:
        global jobs_per_submit
        jobs_per_submit = str(input("Number of Files to Submit per Interval: "))
        try:
            jobs_per_submit = int(jobs_per_submit)
            break
        except ValueError:
            print("{} is not a valid entry for total duration, try again.".format(jobs_per_submit))
            continue

    while True:
        sources_in_rotation = str(input("Total Number of Source Files to Submit: "))
        try:
            sources_in_rotation = int(sources_in_rotation)
            break
        except ValueError:
            print("{} is not a valid entry for total duration, try again.".format(sources_in_rotation))
            continue

    while True:
        global source_dir
        source_dir = str(input("Watch folder file path (Absolute Windows Path): "))
        valid_path = path_validation(source_dir)
        if valid_path is False:
            print("\n{}   is not a valid directory path, try again.\n".format(source_dir))
            continue
        else:
            break

    while True:
        api_endpoint = str(input("The Vantage API Endpoint (Hostname): "))
        try:
            if api_endpoint.upper() not in api_endpoint_list:
                print("\n\n{} is not a valid entry for the API Endpoint, try again.".format(api_endpoint))
            else:
                api_endpoint_status = api_endpoint_check(api_endpoint)
                if api_endpoint_status == True:
                    break
                else:
                    if "[Errno 61] Connection refused'" in api_endpoint_status:
                        print('Error: Please verify that the Vantage SDK Service is started and reachable on {}.'.format(api_endpoint) + " Or choose another endpoint." + '\n\n')
                    else:
                        print("API Status: " + api_endpoint_status)
                    continue
        except requests.exceptions.RequestException as err:
            print('\nError: Please verify that the Vantage SDK Service is started and reachable on {}.'.format(api_endpoint) + '\n\n' + 'Error Message: ' + str(err) + '\n\n')
            continue


    while True:
        root_uri = 'http://'+ api_endpoint + ':8676'
        global target_workflow_id
        target_workflow_id = str(input("The Vantage Workflow ID: "))
        try:
            id_request = requests.get(root_uri + '/REST/workflows/'+ target_workflow_id)

            if id_request.json() == {'Workflow': None}:
                print("{} is not a valid entry for Job ID, try again.".format(target_workflow_id))
                continue
            else:
                clear()
                break
        except requests.exceptions.RequestException as err:
            print('\nError: Please verify that the Vantage SDK Service is started and reachable on {}.'.format(api_endpoint) + '\n\n' + 'Error Message: ' + str(err) + '\n\n')

    while True:
        start_message = f"\
        ================================================================\n \
            Starting the Vantage workflow with these values : \n \
            Jobs will start on :   {str(start_time)} \n \
            Total Batch Duration (hrs) :   {str(total_duration)} \n \
            Submission Frequency (min) :   {str(submit_frequency)} \n \
            Jobs per Submission :   {str(jobs_per_submit)} \n \
            Jobs in Rotation :   {str(sources_in_rotation)} \n \
            Watch Folder Path (Win):   {str(source_dir)} \n \
            Vantage API Endpoint :   {str(api_endpoint)} \n \
            Vantage Job ID :    {str(target_workflow_id)} \n \
        ===========================================================\n"

        logger.debug(start_message)

        print(start_message)

        workflow_confirm = str(input("Submit All Parameters Now? (y/n) : "))

        if workflow_confirm.lower() in ('yes', 'y'):
            break
        elif workflow_confirm.lower() in ('no', 'n'):
            clear()
            print_intro(logger)
        else:
            print("{} is not a valid choice.".format(workflow_confirm))
            down_convert = down_convert = str(input("Please select Yes or No (Y/N): "))
            continue

    clear()

    return [start_time, total_duration, submit_frequency, jobs_per_submit, sources_in_rotation, source_dir, api_endpoint, target_workflow_id]


# ========================= USER INPUT VALIDATION =========================== #


def platform_check():
    '''Get the OS of the server executing the code.'''
    os_platform = platform.system()
    logger.debug("os platform detected: " + os_platform)
    return os_platform

def api_endpoint_check(api_endpoint):
    '''check the online status of an api endpoint'''
    root_uri = 'http://'+ api_endpoint + ':8676'

    try:
        source_frame = inspect.stack()[1]
        frame,filename,line_number,function_name,lines,index = source_frame
        source_func = source_frame[3]

        if source_func == 'print_intro':
            try:
                domain_check = requests.get(root_uri + '/REST/Domain/Online')
                domain_check_rsp = domain_check.json()
                api_endpoint_status = domain_check_rsp['Online']

                if api_endpoint_status is not True:
                    api_endpoint_status = "\n\n{} is not active or unreachable, please check the Vantage SDK service on the host and try again.".format(api_endpoint.upper()) + "\n\n" + str(err) + "\n\n"
                else:
                    pass

            except requests.exceptions.RequestException as excp:
                excp_msg1 = f"Exeception raised on API endpoint check."
                logger.debug(excp_msg1)
                api_endpoint_status = str(excp)
                print("Exception Message #1:" + eexcp_msg1)
                print(api_endpoint_status)


            return api_endpoint_status


        elif source_func in ['check_vantage_status', 'check_domain_load', 'check_job_queue', 'api_submit', 'job_submit']:

            try:
                domain_check = requests.get(root_uri + '/REST/Domain/Online')
                domain_check_rsp = domain_check.json()
                api_endpoint_status = domain_check_rsp['Online']

                if api_endpoint_status == True:
                    return api_endpoint

                else:
                    api_endpoint = api_endpoint_failover(api_endpoint)

            except requests.exceptions.RequestException as excp:
                excp_msg2 = f"Exeception raised on API endpoint check."
                logger.debug(excp_msg2)
                api_endpoint_status = str(excp)
                print(excp_msg2)
                print("Exception Message #2:" + eapi_endpoint_status)
                api_endpoint = api_endpoint_failover(api_endpoint)
                return api_endpoint

        else:
            api_endpoint = api_endpoint_failover(api_endpoint)

    except Exception as excp:
        excp_msg3 = f"Exeception raised on API endpoint check."
        logger.debug(excp_msg3)
        api_endpoint_status = str(excp)
        print("Exception Message #3:" + excp_msg3)
        print(api_endpoint_status)


def clean_datetimes(date_str):
    '''Validate and clean user input for the start time.'''
    date_str = date_str.replace(",", "")

    while True:
        try:
            if re.search(r'[a-zA-Z]', date_str) is not None:
                print("Start Time cannot include and letter characters, try again.")
                clean_st = 0
                break

            if date_str.find(" ") > 0:
                print("Start Time can include only numbers and commas, try again. \n examples: 201810010730 or 2018,10,1,7,30 ")
                clean_st = 0
                break

            if len(date_str) != 12:
                print("Not a valid start time, try again. \n examples: 201810010730 or 2018,10,1,7,30 ")
                clean_st = 0
                break

            else:
                clean_st = date_str
                break
        except ValueError:
            print("{} is not a valid entry for start time, try again.".format(date_str))
            continue

    return clean_st


def make_posix_path(source_dir):
    '''Create a valid POSIX path for the watch folder.'''
    source_dir_list = re.findall(r"[\w']+", source_dir)
    posix_path = root_dir_posix + "/".join(source_dir_list[1:])
    return posix_path


def path_validation(source_dir):
    '''Validate the user input for the watch folder file path.'''
    global os_platform
    os_platform = platform_check()
    source_dir_list = re.findall(r"[\w']+", source_dir)

    posix_path = root_dir_posix + "/".join(source_dir_list[1:])
    windows_path = PureWindowsPath(source_dir)

    if os_platform == 'Darwin':
        p = posix_path
    else:
        p = str(windows_path)
    if p is None or os.path.isdir(p) is not True:
        valid_path = False
    else:
        valid_path = True

    return valid_path


def countdown(start_time):
    '''Create a visible countdownin the terminal window based on the start time of the user input.'''
    present = datetime.now()
    td = start_time - present
    tds = td.total_seconds()

    while tds > 0:
        mins, secs = divmod(tds, 60)
        hours, mins = divmod(mins, 60)
        timeformat = '{:02d}:{:02d}:{:02d}'.format(int(hours), int(mins), int(secs))
        print("Job Sumission Starts In: " + str(timeformat), end='\r')
        time.sleep(1)
        tds -= 1
    time.sleep(1)
    clear()
    print("")
    print("\n================ Starting Now =================\n")
    print("========= "+ str(strftime("%A, %d %B %Y %I:%M%p", localtime())) + " ==========\n")
    return


# ===================== DOMAIN AND API ENPOINT CHECKS ======================= #


def check_vantage_status(target_workflow_id, api_endpoint):

    '''
    active_jobs_val
    0 num of active jobs is below threshold
    1 num of active jobs is above threshold, check count remainder != 0
    2 num of active jobs is above threshold, check count remainder == 0

    domain_load_val
    0 no domain services are above set threshold level.
    1 one or more of the domain services are above set threshold level.

    job check count
    0 check count is zero
    1 check count is a multple of 10
    2 check count is not a multiple of 10
    '''

    global job_check_count
    job_check_count = 0


    while True:

        try:
            api_endpoint = api_endpoint_check(api_endpoint)
            domain_load = check_domain_load(job_check_count, api_endpoint)
            job_queue = check_job_queue(target_workflow_id, api_endpoint, job_check_count)

            if job_check_count == 0:
                job_count_val = 0
            elif job_check_count % 10 is 0:
                job_count_val = 1
            else:
                job_count_val = 2

            status_val = [job_queue[0], domain_load[0], job_count_val]

            # break_list = [[0,0,0],[0,0,1],[1,0,1],[0,0,2],[1,0,2],[1,0,0]]
            # msg1_list = [[0,1,0],[0,1,1],[1,1,0]]
            # msg2_list = [[0,1,2],[1,1,2]]
            # msg3_list = [[1,1,1]]

            break_list = [[0,0,0],[0,0,1],[0,0,2]]
            msg1_list = [[0,1,0],[0,1,1],[1,0,0],[1,1,0]]
            msg2_list = [[0,1,2],[1,0,2],[1,1,2]]
            msg3_list = [[1,0,1],[1,1,1]]

            # print("")
            # print("STATUS START" + str(status_val))
            # print("")

            if status_val in msg1_list:
                msg1 = f"\n\
                ===========================================================\
                {str(strftime('%A, %d. %B %Y %I:%M%p', localtime()))}\
                Active Job Count:  {str(job_queue[1])} \
                Domain Load:  {str(domain_load[1])} \
                Job submission is paused until the system load decreases.\
                ===========================================================\n"
                logger.debug(msg1)
                print(msg1)

            elif status_val in msg2_list:
                msg2 =f"\n\
                Job Check Count:  {str(job_check_count)}\
                Active Job Count:  {str(job_queue[1])}\
                Domain Load:   {str(domain_load[1])}\
                "
                logger.debug(msg2)
                print(msg2)

            elif status_val in msg3_list:
                msg3 =f"\n\
                ===========================================================\
                {str(strftime('%A, %d. %B %Y %I:%M%p', localtime()))}\
                ******* System Load - Status Update *******\
                Active Job Count:  {str(job_queue[1])}\
                Domain Load:  {str(domain_load[1])}\
                ===========================================================\n\
                "
                logger.debug(msg3)
                print(msg3)

            else:
                break

            time.sleep(60)
            job_check_count += 1

        except Exception as excp:
            vanstatus_excp_msg = f"Exeception raised on a Vantage Dominan Status check."
            logger.debug(vanstatus_excp_msg)
            print(vanstatus_excp_msg)
            print(str(excp))

            if Exception is requests.exceptions.RequestException:
                api_endpoint = api_endpoint_check(api_endpoint)
                continue
            else:
                break

    return api_endpoint


def check_domain_load(job_check_count, api_endpoint):
    '''Get a Domain Load based on Transcode, CPU, Edit, and Analysis'''

    try:
        api_endpoint = api_endpoint_check(api_endpoint)
        global root_uri
        root_uri = "http://" + str(api_endpoint) + ":8676/"

        cpu = requests.get(root_uri + '/Rest/Domain/Load/CPU')
        transcode = requests.get(root_uri + '/Rest/Domain/Load/Transcode')
        analysis = requests.get(root_uri + '/Rest/Domain/Load/Analysis')
        edit = requests.get(root_uri + '/Rest/Domain/Load/edit')

        service_list = ['cpu','transcode','analysis','edit']

        load_list = [cpu.json(),transcode.json(),analysis.json(),edit.json()]

        count = 0
        service_load_list = []
        load_str = ""

        for service in load_list:
            service_load = service['Load']
            serv_name = service_list[count]
            service_load_list.append([serv_name,service_load])
            count += 1

        get_load = itemgetter(1)
        global sorted_serviceload_list
        sorted_serviceload_list = sorted(service_load_list, key=get_load, reverse=True)

        high_load_list = []
        low_load_list = []

        for service_num in sorted_serviceload_list:
                # print("SERVICE NUM:" + str(service_num[1]))
                if service_num[1] > 80:
                    high_load_list.append(service_num)
                else:
                    low_load_list.append(service_num)

        global domain_load_val
        domain_load_val = 0

        if len(high_load_list) > 0:
            domain_load_val = 1
        else:
            domain_load_val = 0

    except requests.exceptions.RequestException as excp:
        domainck_excp_msg = f"Exeception raised on a Vantage Dominan Load check."
        logger.debug(domainck_excp_msg)
        print(domainck_excp_msg)
        print(str(excp))

        api_endpoint = api_endpoint_failover(api_endpoint)
        check_domain_load(job_check_count, api_endpoint)

    return [domain_load_val, sorted_serviceload_list]

def check_job_queue(target_workflow_id, api_endpoint, job_check_count):
    '''Check for the number of the jobs running  in the given workflow, prevent the script from overloading the Vantage system.'''

    while True:
        try:
            api_endpoint = api_endpoint_check(api_endpoint)
            global root_uri
            root_uri = "http://" + str(api_endpoint) + ":8676/"

            get_job_status = requests.get(root_uri + '/REST/Workflows/' + target_workflow_id + '/Jobs/?filter=Active')

            active_jobs_json = get_job_status.json()
            active_job_count = len(active_jobs_json['Jobs'])

            if active_job_count <= 50:
                job_queue_val = 0
                break

            elif active_job_count >= 50:
                job_queue_val = 1
                break

            else:
                job_queue_val = 0
                # print("PASS JOB QUEUE VAL")
                pass

        except requests.exceptions.RequestException as excp:
            jobqueue_excp_msg = f"Exeception raised on a Vantage Job Queue check."
            logger.debug(jobqueue_excp_msg)
            print(jobqueue_excp_msg)
            print(str(excp))

            api_endpoint = api_endpoint_failover(api_endpoint)
            check_domain_load(job_check_count, api_endpoint)

    return [job_queue_val, active_job_count]


def api_endpoint_failover(api_endpoint):

        while True:
            try:
                api_fail = f"\
                =======================================================\
                {str(strftime("%A, %d %B %Y %I:%M%p", localtime()))} \
                Removing {api_endpoint} from the list of available api endpoints.\
                Attempting to switch to a new API Endpoint now.\
                =======================================================\
                "
                logger.debug(api_fail)
                print(api_fail)

                new_api_endpoint = api_endpoint_list[0]
                api_endpoint = new_api_endpoint
                root_uri = "http://" + api_endpoint + ":8676/"

                api_new = f"\
                Switching to new API Endpoint:  {api_endpoint}"
                logger.debug(api_new)
                print(api_new)
                break

            except requests.exceptions.RequestException as err:
                print("\n{} is not active or unreachable, trying another api_endpoint.".format(new_api_endpoint) + "\n" + str(err))
                if len(api_endpoint_list) > 1:
                    api_endpoint_list.remove(api_endpoint)
                    continue
                else:
                    print("Vantage cannot find an available api endpoint. Please check the Vantage SDK service on the Lighspeed servers is started and reachable.\n")
                    print("After confirming an available API Enpoint, please hit return to contine.")
                    continue
        # print("\nRETURN NEW API ENDPOINT: " + api_endpoint + "\n")
        return api_endpoint


# ==================== JOB STATES and LOGGING ========================= #


def check_job_state(files_submitted, jobs_per_submit):
    '''CURRENTLY UNSUSED -- Count the total number of sucessful and failed jobs at the end of the batch.'''

    failed_jobs = 0
    sucessful_jobs = 0

    for job in job_list:
        if files_submitted >= jobs_per_submit * 2:
            job_status_get = requests.get(root_uri + '/REST/Jobs/' + job_id)
            job_state = job_status_get.json()

            if job_state['State'] == 0:
                pass
            elif job_state['State'] == 4:
                failed_jobs += failed_jobs + 1
            elif job_state['State'] == 5:
                sucessful_jobs += sucessful_jobs + 1
            else:
                pass
        else:
            pass

    return failed_jobs, sucessful_jobs

def jobs_complete(files_submitted, files_skipped):
    '''Print a summary message in the terminal window at the end of the batch run.'''
    complete_msg = f"\n\
    ==================================================================\
                           Jobs Complete!                      \
    {str(strftime("%A, %d %B %Y %I:%M%p", localtime()))} \
    {str(files_submitted - files_skipped)} files were submitted. \
    {str(files_skipped)} files were skipped. \
    ==================================================================\
    "
    logger.debug(complete_msg)
    print(complete_msg)

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

    file_list = [x.name for x in p.glob('*.MOV') if x.is_file()]
    sorted_list = sorted(file_list)

    for files_submitted in range(int(total_jobs)):
        '''Submit batches of jobs at set intervals for the duration specified.'''
        try:
            file = sorted_list[list_number]
            # file_match = re.match('TEST_'+ r'([0-9]{7})'+'.mov', file)
            file_match = re.match(r'([0-9]{7})'+'.MOV', file)


            if files_submitted != 0 and files_submitted % jobs_per_submit == 0:
                print('Waiting ' + str(submit_frequency) + ' minutes\n')

                time.sleep(submit_frequency * 60)

                sub_files_msg = f"\
                Submitting Files {str(files_submitted + 1)} to {str(jobs_per_submit + files_submitted)} at {str(strftime('%H:%M:%S', localtime()))}"
                logger.debug(sub_files_msg)
                print(sub_files_msg)

            if file_match is not None:
                file_submit_msg = f"Submitting: {file}"
                logger.debug(file_submit_msg)
                print(file_submit_msg)
                api_endpoint = job_submit(target_workflow_id, source_dir, api_endpoint, file)
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
                apisubmit_excp_msg = f"Exeception raised on a Vantage API Submit."
                logger.debug(apisubmit_excp_msg)
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
                    job_blob = job_get.json()
                    job_blob['JobName'] = file
                    job_blob['Medias'][0]['Files'][0] = source_dir + file
            else:
                continue

            job_post_msg = f"posting job with values: {job_blob}"
            logger.debug(job_post_msg)

            job_post = requests.post(root_uri + '/REST/Workflows/' + target_workflow_id + '/Submit',json=job_blob)

            job_post_response = job_post.json()
            job_id = job_post_response['JobIdentifier']
            job_id_msg = f"posted job with id: {job_id}"
            break

        except requests.exceptions.RequestException as excp:
            jobsubmit_excp_msg = f"Exeception raised on a Vantage Job Submit."
            logger.debug(jobsubmit_excp_msg)
            print(jobsubmit_excp_msg)
            print(str(excp))
            api_endpoint = api_endpoint_failover(api_endpoint)
            job_submit(target_workflow_id, source_dir, api_endpoint, file)
            break

    return api_endpoint
