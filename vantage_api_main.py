#!/usr/bin/env python3

import logging
import logging.config
import os
import yaml

import get_user_input as gui
import vantage_api_auto_submit as vn


def set_logger():
    """Setup logging configuration
    """
    path = 'logging.yaml'

    with open(path, 'rt') as f:
        config = yaml.safe_load(f.read())
        logger = logging.config.dictConfig(config)

    return logger


def vantage_main():
    '''set the variables for the script.'''

    set_logger()

    gui_Vars = gui.print_intro()

    start_time = gui_Vars[0]
    total_duration = gui_Vars[1]
    submit_frequency = gui_Vars[2]
    jobs_per_submit = gui_Vars[3]
    sources_in_rotation = gui_Vars[4]
    source_dir = gui_Vars[5]
    api_endpoint = gui_Vars[6]
    target_workflow_id = gui_Vars[7]

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

    print(start_message)

    gui.countdown(start_time)

    vn.api_submit(total_duration, submit_frequency, jobs_per_submit, sources_in_rotation, source_dir, api_endpoint, target_workflow_id)

if __name__ == '__main__':
    set_logger()
    vantage_main()
