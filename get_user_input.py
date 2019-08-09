
import inspect
import os
import platform
import re
import requests
import time

from datetime import datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from time import localtime, strftime


# =================== BEGIN CONSOLE PROMPT FOR USER INPUT =================== #


def print_intro():
    """
    CONSOLE PROMPT FOR USER INPUT
    """

    intro_banner = f"\n\
    ===========================================================\n\
               Vantage Workflow Submission Script \n\
                Version 3.0, February 25, 2019\n\
    This script will use the Vantage REST API to submit files\n\
    to a workflow at set intervals. The user defines the \n\
    duration, frequency, and total number of jobs submitted. \n\
    ================================================================\n"

    print(intro_banner)

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
        start_message = f"\n\
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

        logger.info(start_message)

        print(start_message)

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
    os_platform = platform.system()
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
                excp_msg1 = f"Exception raised on API endpoint check."
                logger.exception(excp_msg1)
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
                excp_msg2 = f"Exception raised on API endpoint check."
                logger.exception(excp_msg2)
                api_endpoint_status = str(excp)
                print(excp_msg2)
                print("Exception Message #2:" + eapi_endpoint_status)
                api_endpoint = api_endpoint_failover(api_endpoint)
                return api_endpoint

        else:
            api_endpoint = api_endpoint_failover(api_endpoint)

    except Exception as excp:
        excp_msg3 = f"Exception raised on API endpoint check."
        logger.exception(excp_msg3)
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
