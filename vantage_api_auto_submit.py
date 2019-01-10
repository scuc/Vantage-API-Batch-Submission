#!/usr/bin/env python3

import logging
import os
import platform
import re
import requests
import time

from datetime import datetime
from itertools import product
from pathlib import Path, PurePosixPath, PureWindowsPath
from subprocess import call
from time import localtime, strftime

API_ENDPOINT_LIST = ['LIGHTSPEED1', 'LIGHTSPEED2', 'LIGHTSPEED3', 'LIGHTSPEED4'
                ,'LIGHTSPEED5', 'LIGHTSPEED6', 'LIGHTSPEED7', 'FNDC-VANLSG6-08'
                ,'FNDC-VANLSG6-09', 'FNDC-VANLSG6-10', 'FNDC-VANLSG6-11'
                ]
JOB_LIST = []
PLATFORM = None
ROOT_DIR_WIN = 'T:\\\\'
ROOT_DIR_POSIX = '/Volumes/Quantum2/'
ROOT_URI = None


def clear():
    '''check and make call for specific operating system'''
    _ = call('clear' if os.name =='posix' else 'cls')


# =================== BEGIN PROMPT FOR USER INPUT =================== #


def print_intro():

    print("=========================================================== " + "\n"
          + '''           Vantage Workflow Submission Script \n
                Version 1.2, December 21, 2018\n
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

            if start_time < present:
                print("Start Time must be a time in the future, try again.")
                continue
            else:
                break
        except ValueError:
            print("ValueError, try again")
            continue

    while True:
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
        source_dir = str(input("Watch folder file path (Absolute Windows Path): "))
        valid_path = path_validation(source_dir)
        if valid_path is False:
            print("\n{}   is not a valid directory path, try again.\n".format(source_dir))
            continue
        else:
            break

    while True:
        api_endpoint = str(input("The Vantage API Endpoint (Hostname): "))
        global ROOT_URI
        ROOT_URI = 'http://'+ api_endpoint + ':8676'
        try:
            if api_endpoint not in API_ENDPOINT_LIST:
                print("\n\n{} is not a valid entry for the API Endpoint, try again.".format(api_endpoint))
            else:
                api_endpoint_status = api_endpoint_check(ROOT_URI, api_endpoint)
                if api_endpoint_status == True:
                    break
                else:
                    print(api_endpoint_status)
                    continue
        except requests.exceptions.RequestException as err:
            print('\nError: Please verify that the Vantage SDK Service is started and reachable on {}.'.format(api_endpoint) + '\n\n' + 'Error Message: ' + str(err) + '\n\n')
            continue


    while True:
        target_workflow_id = str(input("The Vantage Workflow ID: "))
        try:
            id_request = requests.get(ROOT_URI + '/REST/workflows/'+ target_workflow_id)

            if id_request.json() == {'Workflow': None}:
                print("{} is not a valid entry for Job ID, try again.".format(target_workflow_id))
                continue
            else:
                clear()
                break
        except requests.exceptions.RequestException as err:
            print('\nError: Please verify that the Vantage SDK Service is started and reachable on {}.'.format(api_endpoint) + '\n\n' + 'Error Message: ' + str(err) + '\n\n')

    while True:
        print('===========================================================')
        print('')

        print("Starting the Vantage workflow with these values : ")
        print('')

        print("Jobs will start on : " + str(start_time))
        print("Total Batch Duration (hrs) : " + str(total_duration))
        print("Submission Frequency (min) : " + str(submit_frequency))
        print("Jobs per Submission : " + str(jobs_per_submit))
        print("Total Jobs in Batch : " + str(sources_in_rotation))
        print("Watch Folder Path (Win): " + str(source_dir))
        print("Vantage API Endpoint : " + str(api_endpoint))
        print("Vantage Job ID : " + str(target_workflow_id))

        print('')
        print('===========================================================')
        print('')
        print('')

        workflow_confirm = str(input("Submit All Parameters Now? (y/n) : "))

        if workflow_confirm.lower() in ('yes', 'y'):
            break
        elif workflow_confirm.lower() in ('no', 'n'):
            clear()
            print_intro()
        else:
            print("{} is not a valid choice.".format(workflow_confirm))
            down_convert = down_convert = str(input("Please select Yes or No (Y/N): "))
            continue

    clear()

    return [start_time, total_duration, submit_frequency, jobs_per_submit, sources_in_rotation, source_dir, api_endpoint, target_workflow_id]


# ========================= USER INPUT VALIDATION =========================== #


def platform_check():
    '''Get the OS of the server executing the code.'''
    PLATFORM = platform.system()
    return PLATFORM

def api_endpoint_check(ROOT_URI, api_endpoint):
    '''check the online status of an api endpoint'''

    try:
        domain_check = requests.get(ROOT_URI + '/REST/Domain/Online')
        domain_check_rsp = domain_check.json()

        api_endpoint_status = domain_check_rsp['Online']

    except requests.exceptions.RequestException as err:
        api_endpoint_status = "\n\n{} is not active or unreachable, please check the Vantage SDK service on the host and try again.".format(api_endpoint) + "\n\n" + str(err) + "\n\n"

    return api_endpoint_status


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
    posix_path = ROOT_DIR_POSIX + "/".join(source_dir_list[1:])
    return posix_path


def path_validation(source_dir):
    '''Validate the user input for the watch folder file path.'''
    os_platform = platform_check()
    source_dir_list = re.findall(r"[\w']+", source_dir)

    posix_path = ROOT_DIR_POSIX + "/".join(source_dir_list[1:])
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

    job_check_count = 0

    while True:

        try:
            domain_load = check_domain_load(job_check_count, api_endpoint)
            job_queue = check_job_queue(target_workflow_id, api_endpoint, job_check_count)

            if job_check_count == 0:
                job_count_val = 0
            elif job_check_count % 5 is 0:
                job_count_val = 1
            else:
                job_count_val = 2

            status_val = [job_queue[0], domain_load[0], job_count_val]

            break_list = [[0,0,0], [0,0,1],[0,0,2]]
            msg1_list = [[0,1,0],[0,1,1],[1,0,0],[1,1,0]]
            msg2_list = [[0,1,2],[1,0,2],[1,1,2]]
            msg3_list = [[1,0,1][1,1,1]]

            if status_val in msg1_list:
                print("====================================================")
                print("JOB CHECK COUNT: " + str(job_check_count))
                print(str(strftime("%A, %d. %B %Y %I:%M%p", localtime())))
                print("Active Job Count: " + str(job_queue[1]))
                print("Domain Load: " + str(domain_load[1])) + str(domain_load[2])
                print("Job submission will pause until the system load decreases.")
                print("====================================================\n")
                continue
            elif status_val in msg2_list:
                print("")
                print("Job Check Count: " + str(job_check_count))
                print("Active Job Count: " + str(job_queue[1]))
                print("Domain Load: " + str(domain_load[1])) + str(domain_load[2]))
                print("")
                continue
            elif status_val in
                print("====================================================")
                print(str(strftime("%A, %d. %B %Y %I:%M%p", localtime())))
                print("*** System Load - Status Update***")
                print("Active Job Count: " + str(job_queue[1]))
                print("Domain Load: " + str(domain_load[1]) + str(domain_load[2]))
                print("Waiting for the service load to decrease.")
                print("====================================================\n")
                continue
            else:
                break

            time.sleep(60)
            job_check_count += 1

        except Exception as excp:
            print("check_vantage_status() - Error Message: " + str(excp))
            break

    return


def check_domain_load(job_check_count, api_endpoint):
    '''Get a Domain Load based on Transcode, CPU, Edit, and Analysis'''

    try:
        ROOT_URI = "http://" + str(api_endpoint) + ":8676/"

        cpu = requests.get(ROOT_URI + '/Rest/Domain/Load/CPU')
        transcode = requests.get(ROOT_URI + '/Rest/Domain/Load/Transcode')
        analysis = requests.get(ROOT_URI + '/Rest/Domain/Load/Analysis')
        edit = requests.get(ROOT_URI + '/Rest/Domain/Load/edit')

        service_list = ['cpu','transcode','analysis','edit']

        load_list = [cpu.json(),transcode.json(),analysis.json(),edit.json()]

        count = 0
        service_load_list = []
        load_str = ""

        for service in load_list:
            service_load = service['Load']
            serv_name = service_list[count]
            service_load_list.append({serv_name: service_load})
            count += 1

        high_load_list = []
        low_load_list = []

        for load_dict in service_load_list:
            for key, value in load_dict.items():
                if value > 5:
                    high_load_list.append((key, value))
                else:
                    low_load_list.append((key, value))

        if len(high_load_list) > 0:

            domain_load_val= 1

        else:
            domain_load_val = 0

    except requests.exceptions.RequestException as err:
        print("\n\n***********************************\n")
        print("check_domain_load() - Error Mesage: " + str(err) + "\n")
        print("Attempting to switch to a new API Endpoint now.\n")
        print("***********************************\n\n")
        api_endpoint = api_endpoint_failover(api_endpoint)

    return [domain_load_val, high_load_list, low_load_list]

def check_job_queue(target_workflow_id, api_endpoint, job_check_count):
    '''Check for the number of the jobs running  in the given workflow, prevent the script from overloading the Vantage system.'''

    try:
        ROOT_URI = "http://" + str(api_endpoint) + ":8676/"

        get_job_status = requests.get(ROOT_URI + '/REST/Workflows/' + target_workflow_id + '/Jobs/?filter=Active')

        active_jobs_json = get_job_status.json()
        active_job_count = len(active_jobs_json['Jobs'])

        if active_job_count <= 3:
            job_queue_val = 0

        elif active_job_count >= 3 and \
            job_check_count == 0:
            job_queue_val = 1

        elif active_job_count >= 3 and \
            job_check_count > 0 and \
            job_check_count % 5 is not 0:
            job_queue_val = 1

        elif active_job_count >= 3 and \
                job_check_count > 0 and \
                job_check_count % 5 == 0:
                job_queue_val = 2

        else:
            job_queue_val = 0
            print("PASS")
            pass

    except requests.exceptions.RequestException as err:
        print("\n\n***********************************\n")
        print("check_job_queue() - Error Mesage: " + str(err) + "\n")
        print("Attempting to switch to a new API Endpoint now.\n")
        print("***********************************\n\n")
        api_endpoint = api_endpoint_failover(api_endpoint)

    return [job_queue_val, active_job_count]


def api_endpoint_failover(api_endpoint):

        print("\n==================================================\n")
        print("                 Starting API FAILOVER Now          ")
        print("           "+ str(strftime("%A, %d %B %Y %I:%M%p", localtime())) + "           ")
        print("\n==================================================\n")

        while True:
            machine_name_list = []
            sdk_list = []

            try:
                print(API_ENDPOINT_LIST)
                print("\nREMOVING THIS ONE: " + api_endpoint + "\n")
                API_ENDPOINT_LIST.remove(api_endpoint)
                print(API_ENDPOINT_LIST)

                ROOT_URI = "http://" + str(API_ENDPOINT_LIST[0]) + ":8676/"

                new_api_endpoint = API_ENDPOINT_LIST[0]

                print("\nNEW API ENDPOINT: " + new_api_endpoint + "\n")

                api_endpoint = new_api_endpoint

                break

            except requests.exceptions.RequestException as err:
                print("\n\n{} is not active or unreachable, trying another api_endpoint.".format(new_api_endpoint) + "\n\n" + str(err))
                if len(API_ENDPOINT_LIST) > 1:
                    API_ENDPOINT_LIST.remove(api_endpoint)
                    continue
                else:
                    print("Vantage cannot find an available api endpoint. Please check the Vantage SDK service on the Lighspeed servers is started and reachable.\n")
                    print("After confirming an available API Enpoint, please hit return to contine.")
                    continue

        return api_endpoint


# ==================== JOB STATES and LOGGING ========================= #


def check_job_state(files_submitted, jobs_per_submit):
    '''CURRENTLY UNSUSED -- Count the total number of sucessful and failed jobs at the end of the batch.'''

    failed_jobs = 0
    sucessful_jobs = 0

    for job in job_list:
        if files_submitted >= jobs_per_submit * 2:
            job_status_get = requests.get(ROOT_URI + '/REST/Jobs/' + job_id)
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

def jobs_log():
    '''CURRENTLY UNUSED - create a log of output from the script.'''
    log_name = str(start_time) + "_VantageAPI_Log.txt"
    logging.basicConfig(
        filename=log_name,
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
        )


def jobs_complete(files_submitted, files_skipped):
    '''Print a summary message in the terminal window at the end of the batch run.'''
    print('\n===========================================')
    print('\nJobs Complete!')
    print(str(files_submitted - files_skipped) + ' files were submitted')
    print(str(files_skipped) + ' files were skipped')
    print('\n===========================================')
    # print(str(failed_jobs) + ' files failed.')
    # print(str(sucessful_jobs) + ' files were sucessful.')


# ==================== API SUBMIT STARTS HERE ============================= #


def api_submit(total_duration, submit_frequency, jobs_per_submit, sources_in_rotation, source_dir, api_endpoint, target_workflow_id):

    jobs_per_hour = (60 / submit_frequency) * jobs_per_submit
    total_jobs = jobs_per_hour * total_duration

    print('\n' + 'This script will submit a total of ' + str(int(total_jobs)) + ' files during the next ' + str(total_duration) + ' hours\n')

    list_number = 0
    files_submitted = 0
    files_skipped = 0

    PLATFORM = platform_check()

    if PLATFORM == 'Darwin':
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
            file_match = re.match('TEST_'+ r'([0-9]{7})'+'.mov', file)

            if files_submitted != 0 and files_submitted % jobs_per_submit == 0:
                print('Waiting ' + str(submit_frequency) + ' minutes\n')
                time.sleep(submit_frequency * 60)
                check_vantage_status(target_workflow_id, api_endpoint)

                print('Submitting Files ' + str(files_submitted + 1) + " to " +str(jobs_per_submit + files_submitted) + ' of ' +
                    str(int(total_jobs)) + ' at ' + str(strftime('%H:%M:%S', localtime())))

            if file_match is not None:
                print("Submitting: " + file)
                job_submit(target_workflow_id, source_dir, api_endpoint, file)
                files_submitted += 1
                list_number += 1
            else:
                print("Skipping: " + file)
                files_skipped += 1
                list_number += 1
                continue

        except IndexError:
            break

    jobs_complete(files_submitted, files_skipped)


def job_submit(target_workflow_id, source_dir, api_endpoint, file):
    '''Submit the file to the workflow, using the REST API.'''

    ROOT_URI = "http://" + api_endpoint + ":8676"

    # print("ROOT URI - Job Submit: " + str(ROOT_URI))

    while True:
        try:
            job_get = requests.get(ROOT_URI + '/REST/Workflows/' + target_workflow_id + '/JobInputs')
            if job_get is not None:
                    job_blob = job_get.json()
                    job_blob['JobName'] = file
                    job_blob['Medias'][0]['Files'][0] = source_dir + file
            else:
                api_endpoint = check_job_queue(target_workflow_id,api_endpoint)
                continue

            job_post = requests.post(ROOT_URI + '/REST/Workflows/' + target_workflow_id + '/Submit',json=job_blob)

            job_post_response = job_post.json()
            job_id = job_post_response['JobIdentifier']
            break

        except requests.exceptions.RequestException as err:

            print("\n\n***********************************\n")
            print("Error Mesage: " + str(err) + "\n")
            print("Attempting to switch to a new API Endpoint now.\n")
            print("***********************************\n\n")
            api_endpoint = api_endpoint_failover(api_endpoint)
            continue
