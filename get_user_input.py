#!/usr/bin/env python3

import logging
import os
import platform
import re
import requests

from datetime import datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from subprocess import call
from time import localtime, strftime

import system_checks as syschk
import input_validation as inpval

logger = logging.getLogger(__name__)


# =================== BEGIN CONSOLE PROMPT FOR USER INPUT =================== #


def intro():
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
            clean_st = inpval.clean_datetimes(st)

            if clean_st == 0:
                continue

            else:
                year = int(clean_st[0:4])
                month = int(clean_st[4:6])
                day = int(clean_st[6:8])
                hour = int(clean_st[8:10])
                minute = int(clean_st[10:12])

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
        submit_frequency = str(input("Job Submission Interval (min): "))
        try:
            if submit_frequency.find(".")== 1:
                submit_frequency = float(submit_frequency)
            else:
                submit_frequency = int(submit_frequency)
            break
        except ValueError:
            print(f"{submit_frequency} is not a valid entry for total duration, try again.")
            continue

    while True:
        jobs_per_submit = str(input("Number of Files to Submit per Interval: "))
        try:
            jobs_per_submit = int(jobs_per_submit)
            break
        except ValueError:
            print(f"{jobs_per_submit} is not a valid entry for total duration, try again.")
            continue

    while True:
        source_dir = str(input("Watch folder file path (Absolute Windows Path): "))
        valid_path = inpval.path_validation(source_dir)
        if valid_path is False:
            print(
                f"\n{source_dir}   is not a valid directory path, try again.\n")
            continue
        else:
            break

    while True:
        endpoint = syschk.get_endpoint()
        root_uri = 'http://'+ endpoint + ':8676'
        target_workflow_id = str(input("The Vantage Workflow ID: "))
        try:
            id_request = requests.get(root_uri + '/REST/workflows/'+ target_workflow_id)

            if id_request.json() == {'Workflow': None}:
                print(
                    f"{target_workflow_id} is not a valid entry for Job ID, try again.")
                continue
            else:
                clear()
                break
        except requests.exceptions.RequestException as err:
            print(
                f"\nError: Please verify that the Vantage SDK Service is started and reachable on {endpoint}.\n\n\
                    Error Message: {str(err)} \n\n")

    while True:
        start_message = f"\n\
        ================================================================\n \
            Starting the Vantage workflow with these values : \n \
            Jobs will start on :   {str(start_time)} \n \
            Submission Frequency (min) :   {str(submit_frequency)} \n \
            Jobs per Submission :   {str(jobs_per_submit)} \n \
            Watch Folder Path (Win):   {str(source_dir)} \n \
            Vantage Job ID :    {str(target_workflow_id)} \n \
        ===========================================================\n"

        logger.info(start_message)

        print(start_message)

        workflow_confirm = str(input("Submit All Parameters Now? (y/n) : "))

        if workflow_confirm.lower() in ('yes', 'y'):
            break
        elif workflow_confirm.lower() in ('no', 'n'):
            clear()
            
        else:
            print(f"{workflow_confirm} is not a valid choice.")
            down_convert = down_convert = str(input("Please select Yes or No (Y/N): "))
            continue

    return [start_time, submit_frequency, jobs_per_submit, source_dir, target_workflow_id]


def clear():
    """
    Issue a command to clear the screen based on a check for specific operating system.
    """
    _ = call('clear' if os.name == 'posix' else 'cls')


if __name__ == '__main__':
    intro()
