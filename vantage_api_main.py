#!/usr/bin/env python3


import vantage_api_auto_submit as vn

import logging
import os
import yaml

from time import strftime


def setup_logging(
    default_path='logging_config.yaml',
    default_level=logging.DEBUG
    ):

    """Setup logging configuration
    """
    path = default_path

    with open(path, 'rt') as f:
        config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)
    logger = logging.getLogger(__name__)

    return logger


def vantage_main():
    '''set the variables for the script.'''

    logger = setup_logging()
    logger.debug('This message should go to the log file')
    vn_Vars = vn.print_intro()

    start_time = vn_Vars[0]
    total_duration = vn_Vars[1]
    submit_frequency = vn_Vars[2]
    jobs_per_submit = vn_Vars[3]
    sources_in_rotation = vn_Vars[4]
    source_dir = vn_Vars[5]
    api_endpoint = vn_Vars[6]
    target_workflow_id = vn_Vars[7]

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
