#!/usr/bin/env python3

import logging
import vantage_api_auto_submit as vn

from logging.handlers import TimedRotatingFileHandler

def set_logger():
    """Setup logging configuration
    """
    print("SET LOGGER")
    logger = logging.getLogger("vantage_api_auto_submit")
    logger.setLevel(logging.DEBUG)
    handler = TimedRotatingFileHandler(filename='vantage_api', when='midnight', encoding="utf8")
    handler.suffix = '_' + '%Y%m%d%H%M'+'.log'
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | Function: %(funcName)s() | Line %(lineno)s | %(message)s")

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

def vantage_main():
    '''set the variables for the script.'''

    set_logger()

    vn_Vars = vn.print_intro()

    start_time = vn_Vars[0]
    total_duration = vn_Vars[1]
    submit_frequency = vn_Vars[2]
    jobs_per_submit = vn_Vars[3]
    sources_in_rotation = vn_Vars[4]
    source_dir = vn_Vars[5]
    api_endpoint = vn_Vars[6]
    target_workflow_id = vn_Vars[7]

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

    vn.countdown(start_time)

    vn.api_submit(total_duration, submit_frequency, jobs_per_submit, sources_in_rotation, source_dir, api_endpoint, target_workflow_id)

if __name__ == '__main__':
    vantage_main()
