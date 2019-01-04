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
ROOT_DIR_WIN = 'T:\\\\'
ROOT_DIR_POSIX = '/Volumes/Quantum2/'
ROOT_URI = None
PLATFORM = None


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
                api_endpoint_status = api_endpoint_check(ROOT_URI)
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


def platform_check():
    '''Get the OS of the server executing the code.'''
    PLATFORM = platform.system()
    return PLATFORM

def check_domain_load():
    '''Get a Domain Load based on Transcode and CPU'''
    check_count = 0

    while True:
        try:
            cpu = requests.get(ROOT_URI + '/Rest/Domain/Load/CPU')
            transcode = requests.get(ROOT_URI + '/Rest/Domain/Load/Transcode')
            analysis = requests.get(ROOT_URI + '/Rest/Domain/Load/Analysis')
            edit = requests.get(ROOT_URI + '/Rest/Domain/Load/edit')

            service_list = ['cpu','transcode','analysis','edit']

            load_list = [cpu.json(),transcode.json(),analysis.json(),edit.json()]

            print(load_list)

            count = 0
            service_load_list = []

            for service in load_list:
                service_load = service['Load']
                serv_name = service_list[count]
                service_load_list.append({serv_name: service_load})
                count += 1

            high_load_list = []

            for load_dict in service_load_list:
                for key, value in load_dict.items():
                    if value > 70:
                        high_load_list.append(key)
                        print(key)
                    else:
                        continue

            if len(high_load_list) > 0:
                print("CHECK COUNT: " + str(check_count))

                if len(high_load_list) > 0 and \
                    check_count == 0:
                    print('\n===========================================')
                    print(str(strftime("%A, %d. %B %Y %I:%M%p", localtime())))
                    print("The Vantage Domain load for the {} service(s) is currently under heavy load.\n".format(high_load_list) + "Job submission will pause until the service load decreases.")
                    print('===========================================\n')
                elif len(high_load_list) > 0 and \
                    check_count > 0 and \
                    check_count % 5 == 0:
                    print('\n===========================================')
                    print("***Job Queue Update***\n" +
                        str(strftime("%A, %d. %B %Y %I:%M%p", localtime())))
                    print("{} service(s) remain under heavy load.".format(high_load_list))
                    print('===========================================\n')
                elif check_count >= 0 and \
                    check_count % 5 is not 0:
                    pass
                else:
                    continue

                time.sleep(60)
                check_count += 1

            else:
                print("BREAK")
                break

        except Exception as excp:
            print("ERROR: " + str(excp))



def api_endpoint_check(ROOT_URI):
    '''check the online status of an api endpoint'''

    try:
        domain_check = requests.get(ROOT_URI + '/REST/Domain/Online')
        domain_check_rsp = domain_check.json()

        api_endpoint_status = domain_check_rsp['Online']

    except requests.exceptions.RequestException as err:
        api_endpoint_status = "\n\n{} is not active or unreachable, please check the Vantage SDK service on the host try again.".format(api_endpoint) + "\n\n" + str(err)

    return api_endpoint_status

def api_endpoint_failover(api_endpoint):
        machine_name_list = []
        sdk_list = []

        API_ENDPOINT_LIST.remove(api_endpoint)

        ROOT_URI = "http://" + str(API_ENDPOINT_LIST[0]) + ":8676/"

        get_machine_names = requests.get(ROOT_URI + 'REST/Machines')
        active_machines_json = get_machine_names.json()
        get_services = requests.get(ROOT_URI + 'REST/Services')
        active_services_json = get_services.json()

        for service in active_services_json["Services"]:
            if service["ServiceTypeName"].lower() != "sdk":
                pass
            else:
                sdk_list.append(service["Machine"])

        machines = [[d['Identifier'],d['Name']] for d in active_machines_json["Machines"]]

        for a,b in product(sdk_list,machines):
            if a == b[0]:
                machine_name_list.append(b[1])
            else:
                pass

        new_api_endpoint  = machine_name_list[0]
        return new_api_endpoint

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
    print("\n========= Starting Now ==========\n")
    print("")
    return


def check_job_queue(target_workflow_id):
    '''Check for the number of the jobs running  in the given workflow, prevent the script from overloading the Vantage system.'''

    job_check_count = 0

    while True:
        try:
            get_job_status = requests.get(ROOT_URI + '/REST/Workflows/' + target_workflow_id + '/Jobs/?filter=Active')

            active_jobs_json = get_job_status.json()
            active_job_count = len(active_jobs_json['Jobs'])

            print("active job count: " + str(active_job_count))
            print("job check count: " + str(job_check_count))
            print("")

            if active_job_count <= 15:
                break

            elif active_job_count >= 15 and \
                job_check_count == 0:

                print('\n====================================================')
                print(str(strftime("%A, %d. %B %Y %I:%M%p", localtime())))
                print("There are currently {} active jobs in this workflow.\n".format(active_job_count) + "Job submission will pause until the job queue clears up.")
                print('====================================================\n')

            elif active_job_count >= 10 and \
                    job_check_count > 0 and \
                    job_check_count % 5 == 0:

                print('\n===========================================')
                print("***Job Queue Update***\n" +
                    str(strftime("%A, %d. %B %Y %I:%M%p", localtime())))
                print("{} active jobs remain.".format(active_job_count))
                print('===========================================\n')

            elif job_check_count >= 0 and \
                job_check_count % 5 is not 0:
                pass

            else:
                continue

            time.sleep(60)
            job_check_count += 1

        except requests.exceptions.RequestException as err:
            print("Error: Please verify that the Vantage SDK Service is reachable at " + ROOT_URI + '/REST/'+ '\n\n' +
                "Error Mesage: " + str(err) + "\n\n")
            input("Once SDK Service is verified, Press enter to continue")
            continue

    return

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


# ================= API SUBMIT STARTS HERE ====================== #


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
                check_job_queue(target_workflow_id)

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

    try:
        job_get = requests.get(ROOT_URI + '/REST/Workflows/' + target_workflow_id + '/JobInputs')

    except requests.exceptions.RequestException as err:

        print("\n\nEXP#1\n\n")
        API_ENDPOINT_LIST = API_ENDPOINT_LIST.remove(api_endpoint)

        while True:
            try:
                api_endpoint = API_ENDPOINT_LIST[0]

                ROOT_URI = "http://" + str(api_endpoint) + ":8676"

                print("")
                print("\n\n**** Switching API endpoint to " + new_api_endpoint + "****\n\n")
                print("")

                job_get = requests.get(ROOT_URI + '/REST/Workflows/' + target_workflow_id + '/JobInputs')
                break

            except requests.exceptions.RequestException as err:
                print("\n\nEXP#2\n\n")
                continue

            else:
                print("\n\n" + "api_endpoint: "  + api_endpoint + "\n\n")
                print(
                'Error on GET: Please verify that the Vantage SDK Service is reachable at ' + ROOT_URI + "/REST/" + '\n\n' +
                "Error Mesage: " + str(err) + "\n\n")

                input("Once SDK Service is verified, Press enter to continue \n\n")
                continue

    job_blob = job_get.json()
    job_blob['JobName'] = file
    job_blob['Medias'][0]['Files'][0] = source_dir + file

    try:
        job_post = requests.post(ROOT_URI + '/REST/Workflows/' + target_workflow_id + '/Submit',json=job_blob)

        job_post_response = job_post.json()
        job_id = job_post_response['JobIdentifier']

    except requests.exceptions.RequestException as err:
        print("\n\nEXP#1\n\n")
        print("Error on POST: Please verify that the Vantage SDK Service is reachable at " + ROOT_URI + '/REST/'+ '\n\n' +
            "Error Mesage: " + str(err) + "\n\n")
        input("Once SDK Service is verified, Press enter to continue\n\n")


