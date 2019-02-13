#!/usr/bin/env python3


import vantage_api_auto_submit as vn

import logging
# import logging.config
import os
import yaml

from logging.handlers import TimedRotatingFileHandler
from time import strftime


def setup_logging():
    """Setup logging configuration
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler = TimedRotatingFileHandler(filename='vantage_api', when='midnight', encoding="utf8")
    handler.suffix = '_' + '%Y%m%d%H%M'+'.log'
    formatter = logging.Formatter("%(asctime)s _%(levelname)s_ Module:%(module)s Function:%(funcName)s(), %(message)s")

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def vantage_main():
    '''set the variables for the script.'''

    logger = setup_logging()
    vn_Vars = vn.print_intro(logger)

    start_time = vn_Vars[0]
    total_duration = vn_Vars[1]
    submit_frequency = vn_Vars[2]
    jobs_per_submit = vn_Vars[3]
    sources_in_rotation = vn_Vars[4]
    source_dir = vn_Vars[5]
    api_endpoint = vn_Vars[6]
    target_workflow_id = vn_Vars[7]

    start_message = f"===========================================================\n  Starting the Vantage workflow with these values : \n                 Jobs will start on : {str(start_time)}"


    logger.debug(start_message)

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
    print("Vantage API Endpoint : " + str(api_endpoint))
    print("Vantage Job ID : " + str(target_workflow_id))

    print('')
    print('===========================================================')

    vn.countdown(start_time)

    vn.api_submit(total_duration, submit_frequency, jobs_per_submit, sources_in_rotation, source_dir, api_endpoint, target_workflow_id)


vantage_main()
