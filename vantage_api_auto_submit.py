#!/usr/bin/env python3

import logging
import os
import platform
import re
import requests
import time

from datetime import datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from subprocess import call
from time import localtime, strftime

JOB_LIST = []
ROOT_URI = 'http://LIGHTSPEED1:8676'
ROOT_DIR_WIN = 'T:\\\\'
ROOT_DIR_POSIX = '/Volumes/Quantum2/'
PLATFORM = None

def clear():
    # check and make call for specific operating system
    _ = call('clear' if os.name =='posix' else 'cls')


def print_intro():

    print("=========================================================== " + "\n"
          + '''           Vantage Workflow Submission Script \n
                Version 1.1, Nov 14, 2018\n
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

    # while True:
    #     st = str(input("Time to Start Job Submits, 24hr Clock (YYYY,M,D,h,m): "))
    #     try:
    #         if st.find(",") == 0:
    #             print("Start Time values must be separated with a comma, try again.")
    #             continue
    #         else:
    #             st = st.replace(" ","").split(",")
    #             present = datetime.now()
    #             start_time = datetime(int(st[0]), int(st[1]), int(st[2]), int(st[3]), int(st[4]))
    #             if start_time < present:
    #                 print("Start Time must be a time in the future, try again.")
    #                 continue
    #             else:
    #                 break
    #     except ValueError or IndexError:
    #         print("{} is not a valid entry for start time, try again.".format(st))
    #         continue

    while True:
        total_duration = str(input("Total Duration (hrs) : "))
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
        submit_frequency = str(input("Job Submission Interval (min) : "))
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
        jobs_per_submit = str(input("Number of Files to Submit per Interval : "))
        try:
            jobs_per_submit = int(jobs_per_submit)
            break
        except ValueError:
            print("{} is not a valid entry for total duration, try again.".format(jobs_per_submit))
            continue

    while True:
        sources_in_rotation = str(input("Total Number of Source Files to Submit : "))
        try:
            sources_in_rotation = int(sources_in_rotation)
            break
        except ValueError:
            print("{} is not a valid entry for total duration, try again.".format(sources_in_rotation))
            continue

    while True:
        source_dir = str(input("Watch folder file path (Absolute Windows Path) : "))
        valid_path = path_validation(source_dir)
        if valid_path is False:
            print("\n{}   is not a valid directory path, try again.\n".format(source_dir))
            continue
        else:
            break


    while True:
        target_workflow_id = str(input("The Vantage Workflow ID : "))
        try:
            id_request = requests.get(ROOT_URI + '/REST/workflows/'+ target_workflow_id)

            if id_request.json() == {'Workflow': None}:
                print("{} is not a valid entry for Job ID, try again.".format(target_workflow_id))
                continue
            else:
                clear()
                break
        except ConnectionError:
            print('Error: Please verify that the Vantage SDK Service is reachable.')
            continue

    while True:
        print('===========================================================')
        print('')

        print("Starting the Vantage workflow with these values : ")
        print('')

        print("Jobs will start on : " + str(start_time))
        print("Total Batch Duration (hrs) : " + str(total_duration))
        print("Submission Frequency (min) : " + str(submit_frequency))
        print("Jobs per Submission : " + str(jobs_per_submit))
        print("Jobs in Rotation : " + str(sources_in_rotation))
        print("Watch Folder Path (Win): " + str(source_dir))
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

    return [start_time, total_duration, submit_frequency, jobs_per_submit, sources_in_rotation, source_dir, target_workflow_id]


def platform_check():
    PLATFORM = platform.system()
    return PLATFORM


def clean_datetimes(date_str):

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
    source_dir_list = re.findall(r"[\w']+", source_dir)
    posix_path = ROOT_DIR_POSIX + "/".join(source_dir_list[1:])
    return posix_path


def path_validation(source_dir):
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
        # source_dir = str(windows_path)
        # if source_dir.endswith('\\') is not True:
        #     source_dir += "\\"

    return valid_path


def check_job_queue(target_workflow_id):

    job_check_count = 0

    while True:
        try:
            get_job_status = requests.get(ROOT_URI + '/REST/Workflows/' + target_workflow_id + '/Jobs/?filter=Active')

            active_jobs_json = get_job_status.json()
            active_job_count = len(active_jobs_json['Jobs'])

            if active_job_count >= 10 and job_check_count % 5 != 0:

                print('\n===========================================')
                print("Current time: " + str(strftime('%H:%M:%S', localtime())))
                print("There are currently {} active jobs in this workflow.\n".format(active_job_count)) + "Job submission will pause until the job queue clears up."
                print('===========================================\n')


                time.sleep(1)
                job_check_count += 1

                continue

            elif active_job_count >= 10 and job_check_count % 5 == 0:

                print('\n===========================================')
                print("***Job Queue Update***\n"
                    "Current time: " + str(strftime('%H:%M:%S', localtime())))
                print("{} active jobs remain.\n".format(active_job_count))
                print('===========================================\n')

            else:
                break

        except ConnectionError:
            print('Error: Please verify that the Vantage SDK Service is reachable at ' + ROOT_URI)
            print('REST get request: ' + ROOT_URI +
                  '/REST/Workflows/' + target_workflow_id + '/JobInputs')
            raw_input("Once SDK Service is verified, Press enter to continue")
            continue

    return

# def check_job_state(files_submitted, jobs_per_submit):

#     failed_jobs = 0
#     sucessful_jobs = 0

#     for job in job_list:
#         if files_submitted >= jobs_per_submit * 2:
#             job_status_get = requests.get(ROOT_URI + '/REST/Jobs/' + job_id)
#             job_state = job_status_get.json()

#             if job_state['State'] == 0:
#                 pass
#             elif job_state['State'] == 4:
#                 failed_jobs += failed_jobs + 1
#             elif job_state['State'] == 5:
#                 sucessful_jobs += sucessful_jobs + 1
#             else:
#                 pass
#         else:
#             pass

#     return failed_jobs, sucessful_jobs

def jobs_log():
    log_name = str(start_time) + "_VantageAPI_Log.txt"
    logging.basicConfig(
        filename=log_name,
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
        )


def jobs_complete(files_submitted, files_skipped):

    print('\n===========================================')
    print('\nJobs Complete!')
    print(str(files_submitted - files_skipped) + ' files were submitted')
    print(str(files_skipped) + ' files were skipped')
    print('\n===========================================')
    # print(str(failed_jobs) + ' files failed.')
    # print(str(sucessful_jobs) + ' files were sucessful.')


# ================= API SUBMIT STARTS HERE ====================== #


def api_submit(total_duration, submit_frequency, jobs_per_submit, sources_in_rotation, source_dir, target_workflow_id):

    # Determine the total number of jobs to submit
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


    # Submit batches of jobs at set intervals for the duration specified.
    for files_submitted in range(int(total_jobs)):

        try:
            file = sorted_list[list_number]
            file_match = re.match('TEST_' + r'([0-9]{7})'+'.mov', file)

            if files_submitted != 0 and files_submitted % jobs_per_submit == 0:
                print('Waiting ' + str(submit_frequency) + ' minutes\n')
                time.sleep(submit_frequency * 60)

                print('Submitting Files ' + str(files_submitted + 1) + " to " + str(jobs_per_submit + files_submitted) + ' of ' +
                    str(int(total_jobs)) + ' at ' + str(strftime('%H:%M:%S', localtime())))

                check_job_queue(target_workflow_id)

            if file_match is not None:
                print("Submitting: " + file)
                job_submit(target_workflow_id, source_dir, file)
                files_submitted += 1
                list_number += 1
            else:
                print("Skipping: " + file)
                files_skipped += 1

        except IndexError:
            print("FUCK THIS")
            jobs_complete(files_submitted, files_skipped)
            break

    jobs_complete(files_submitted, files_skipped)


def job_submit(target_workflow_id, source_dir, file):

    try:
        job_get = requests.get(ROOT_URI + '/REST/Workflows/' + target_workflow_id + '/JobInputs')
    except TypeError:
        print(
            'Error: Please verify that the Vantage SDK Service is reachable at ' + ROOT_URI)
        print('REST get request: ' + ROOT_URI +
              '/REST/Workflows/' + target_workflow_id + '/JobInputs')
        raw_input("Once SDK Service is verified, Press enter to continue")

    job_blob = job_get.json()
    job_blob['JobName'] = file
    job_blob['Medias'][0]['Files'][0] = source_dir + file

    try:
        job_post = requests.post(ROOT_URI + '/REST/Workflows/' + target_workflow_id + '/Submit',json=job_blob)

        job_post_response = job_post.json()
        job_id = job_post_response['JobIdentifier']
        # JOB_LIST.append(job_id)
        # print(job_id)

    except ConnectionError:
        print(
            'Error: Please verify that the Vantage SDK Service is reachable at ' + ROOT_URI)
        print('REST post request: ' + ROOT_URI + '/REST/Workflows/' +
              target_workflow_id + '/Submit with the following json blob:')
        print(job_blob)
        raw_input("Once SDK Service is verified, Press enter to continue")

    # return JOB_LIST

