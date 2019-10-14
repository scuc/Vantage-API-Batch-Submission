#!/usr/bin/env python3

import logging
import logging.config
import os
import yaml

import config as cfg
import get_user_input as gui
import api_submit as api

from datetime import datetime
from subprocess import call
from time import localtime, strftime, sleep

logger = logging.getLogger(__name__)

config = cfg.get_config()


def set_logger():
    """Set up the logging configuration.
    """
    path = 'logging.yaml'

    with open(path, 'rt') as f:
        config = yaml.safe_load(f.read())
        logger = logging.config.dictConfig(config)

    return logger


def clear():
    """
    Issue a command to clear the screen based on a check for specific operating system.
    """
    _ = call('clear' if os.name == 'posix' else 'cls')


def countdown(start_time):
    """
    Create a visible countdownin the terminal window based on the start 
    time of the user input.
    """
    present = datetime.now()
    td = start_time - present
    tds = td.total_seconds()

    while tds > 0:
        mins, secs = divmod(tds, 60)
        hours, mins = divmod(mins, 60)
        timeformat = '{:02d}:{:02d}:{:02d}'.format(
            int(hours), int(mins), int(secs))
        print("Job Sumission Starts In: " + str(timeformat), end='\r')
        sleep(1)
        tds -= 1
    sleep(1)
    clear()
    print("")
    print("\n================ Starting Now =================\n")
    print("========= " +
          str(strftime("%A, %d %B %Y %I:%M%p", localtime())) + 
          " ==========\n")
    return


def vantage_main():
    """
    Set the variables for the script.
    """

    gui_Vars = gui.intro()

    clear()

    start_time = gui_Vars[0]
    submit_frequency = gui_Vars[1]
    jobs_per_submit = gui_Vars[2]
    source_dir = gui_Vars[3]
    target_workflow_id = gui_Vars[4]

    start_message = f"\
    ================================================================\n \
        Starting the Vantage workflow with these values : \n \
        Jobs will start on :   {str(start_time)} \n \
        Submission Frequency (min) :   {str(submit_frequency)} \n \
        Jobs per Submission :   {str(jobs_per_submit)} \n \
        Watch Folder Path (Win):   {str(source_dir)} \n \
        Vantage Job ID :    {str(target_workflow_id)} \n \
    ===========================================================\n"

    print(start_message)

    countdown(start_time)

    api.submit_control(
                submit_frequency, 
                jobs_per_submit, 
                source_dir, 
                target_workflow_id
                )


if __name__ == '__main__':
    set_logger()
    vantage_main()
