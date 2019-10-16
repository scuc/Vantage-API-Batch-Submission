#!/usr/bin/env python3

import inspect
import logging
import requests

import config as cfg

from operator import itemgetter
from time import localtime, strftime

logger = logging.getLogger(__name__)

config = cfg.get_config()

endpoint_list = config['endpoint_list']


# ===================== DOMAIN AND API ENPOINT CHECKS ======================= #


def check_vantage_status(target_workflow_id, endpoint):
    """
    active_jobs_val
    0 num of active jobs is below threshold
    1 num of active jobs is above threshold, check count remainder != 0
    2 num of active jobs is above threshold, check count remainder == 0

    domain_load_val
    0 no domain services are above set threshold level.
    1 one or more of the domain services are above set threshold level.

    job check count
    0 check count is zero
    1 check count is a multple of 10
    2 check count is not a multiple of 10
    """

    job_check_count = 0

    while True:

        try:
            domain_load = check_domain_load(job_check_count, endpoint)
            job_queue = check_job_queue(target_workflow_id, endpoint, job_check_count)

            db.update_db(endpoint, target_workflow_id)

            print("")
            print("DB UPDATED")
            print("")

            if job_check_count == 0:
                job_count_val = 0
            elif job_check_count % 10 is 0:
                job_count_val = 1
            else:
                job_count_val = 2

            status_val = [job_queue[0], domain_load[0], job_count_val]

            # break_list = [[0,0,0],[0,0,1],[1,0,1],[0,0,2],[1,0,2],[1,0,0]]
            # msg1_list = [[0,1,0],[0,1,1],[1,1,0]]
            # msg2_list = [[0,1,2],[1,1,2]]
            # msg3_list = [[1,1,1]]

            break_list = [[0,0,0],[0,0,1],[0,0,2]]
            msg1_list = [[0,1,0],[0,1,1],[1,0,0],[1,1,0]]
            msg2_list = [[0,1,2],[1,0,2],[1,1,2]]
            msg3_list = [[1,0,1],[1,1,1]]

            # print("")
            # print("STATUS START" + str(status_val))
            # print("")

            if status_val in msg1_list:
                msg1 = f"\n\
                ===========================================================\n\
                {str(strftime('%A, %d. %B %Y %I:%M%p', localtime()))}\n\
                Active Job Count:  {str(job_queue[1])} \n\
                Domain Load:  {str(domain_load[1])} \n\
                Job submission is paused until the system load decreases.\n\
                ===========================================================\n"
                logger.info(msg1)

            elif status_val in msg2_list:
                msg2 =f"\n\
                Job Check Count:  {str(job_check_count)}\n\
                Active Job Count:  {str(job_queue[1])}\n\
                Domain Load:   {str(domain_load[1])}\n\
                "
                logger.info(msg2)

            elif status_val in msg3_list:
                msg3 =f"\n\
                ===========================================================\n\
                {str(strftime('%A, %d. %B %Y %I:%M%p', localtime()))}\n\
                ******* System Load - Status Update *******\n\
                Active Job Count:  {str(job_queue[1])}\n\
                Domain Load:  {str(domain_load[1])}\n\
                ===========================================================\n\
                "
                logger.info(msg3)

            else:
                break

            time.sleep(60)
            job_check_count += 1

        except Exception as excp:
            vanstatus_excp_msg = f"Exception raised on a Vantage Dominan Status check."
            logger.exception(vanstatus_excp_msg)

            if Exception is requests.exceptions.RequestException:
                endpoint = endpoint_check(endpoint)
                continue
            else:
                break

    return endpoint


def check_domain_load(job_check_count, endpoint):
    '''Get a Domain Load based on Transcode, CPU, Edit, and Analysis'''

    try:
        endpoint = endpoint_check(endpoint)
        root_uri = "http://" + str(endpoint) + ":8676/"

        cpu = requests.get(root_uri + '/Rest/Domain/Load/CPU')
        transcode = requests.get(root_uri + '/Rest/Domain/Load/Transcode')
        analysis = requests.get(root_uri + '/Rest/Domain/Load/Analysis')
        edit = requests.get(root_uri + '/Rest/Domain/Load/edit')

        service_list = ['cpu','transcode','analysis','edit']

        load_list = [cpu.json(),transcode.json(),analysis.json(),edit.json()]

        count = 0
        service_load_list = []
        load_str = ""

        for service in load_list:
            service_load = service['Load']
            serv_name = service_list[count]
            service_load_list.append([serv_name,service_load])
            count += 1

        get_load = itemgetter(1)
        sorted_serviceload_list = sorted(service_load_list, key=get_load, reverse=True)

        high_load_list = []
        low_load_list = []

        for service_num in sorted_serviceload_list:
                # print("SERVICE NUM:" + str(service_num[1]))
                if service_num[1] > 80:
                    high_load_list.append(service_num)
                else:
                    low_load_list.append(service_num)

        domain_load_val = 0

        if len(high_load_list) > 0:
            domain_load_val = 1
        else:
            domain_load_val = 0

    except requests.exceptions.RequestException as excp:
        domainck_excp_msg = f"Exception raised on a Vantage Dominan Load check."
        logger.exception(domainck_excp_msg)

    return [domain_load_val, sorted_serviceload_list]


def check_job_queue(target_workflow_id, endpoint, job_check_count):
    '''Check for the number of the jobs running  in the given workflow, prevent the script from overloading the Vantage system.'''

    while True:
        try:
            endpoint = endpoint_check(endpoint)
            global root_uri
            root_uri = "http://" + str(endpoint) + ":8676/"

            get_job_status = requests.get(root_uri + '/REST/Workflows/' + target_workflow_id + '/Jobs/?filter=Active')

            active_jobs_json = get_job_status.json()
            active_job_count = len(active_jobs_json['Jobs'])

            if active_job_count <= 3:
                job_queue_val = 0
                break

            elif active_job_count >= 3:
                job_queue_val = 1
                break

            else:
                job_queue_val = 0
                pass

        except requests.exceptions.RequestException as excp:
            jobqueue_excp_msg = f"Exception raised on a Vantage Job Queue check."
            logger.exception(jobqueue_excp_msg)
            print(jobqueue_excp_msg)
            print(str(excp))

            endpoint = get_endpoint()
            check_domain_load(job_check_count, endpoint)

    return [job_queue_val, active_job_count]


def check_job_state(files_submitted, jobs_per_submit):
    '''CURRENTLY UNSUSED -- Count the total number of sucessful and failed jobs at the end of the batch.'''

    failed_jobs = 0
    sucessful_jobs = 0

    for job in job_list:
        if files_submitted >= jobs_per_submit * 2:
            job_status_get = requests.get(root_uri + '/REST/Jobs/' + job_id)
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


def jobs_complete(files_submitted, files_skipped):
    '''Print a summary message in the terminal window at the end of the batch run.'''

    complete_msg = f"\n\
    ==================================================================\n\
                           Jobs Complete!                      \n\
    {str(strftime('%A, %d. %B %Y %I:%M%p', localtime()))} \n\
    {str(files_submitted - files_skipped)} files were submitted. \n\
    {str(files_skipped)} files were skipped. \n\
    ==================================================================\n\
    "
    logger.info(complete_msg)
    print(complete_msg)


# ===================== API ENPOINTS CHECKS ======================= #


def get_endpoint():
    """
    Select an api endpoint from the list of available Vantage servers.
    """
    for endpoint in endpoint_list: 
        try: 
            endpoint_status = endpoint_check(endpoint)

            if endpoint_status != True: 
                endpoint_status_msg = f"\n\n{endpoint.upper()} is not active or unreachable, \
                        please check the Vantage SDK service on the host.\n"
                continue
            else:
                print(endpoint)
                endpoint_status_msg = f"\n\n{endpoint.upper()} online status is confirmed.\n"
                return endpoint

            logger.info(endpoint_status_msg)

        except Exception as e:
            get_ep_exception_msg2 = f"Unable to reach any available Vantage Endpoints."
            logger.error(get_ep_exception_msg)


def endpoint_check(endpoint):
    '''check the online status of an api endpoint'''

    root_uri = 'http://'+ endpoint + ':8676'

    source_frame = inspect.stack()[1]
    frame,filename,line_number,function_name,lines,index = source_frame
    source_func = source_frame[3]

    if source_func in ['intro', 'get_endpoint', 'check_vantage_status', 'check_domain_load',
                        'check_job_queue', 'api_submit', 'job_submit']:
        try: 
            domain_check = requests.get(
                    root_uri + '/REST/Domain/Online')
            domain_check_rsp = domain_check.json()
            endpoint_status = domain_check_rsp['Online']

        except requests.exceptions.RequestException as excp:
            excp_msg2 = f"Exception raised on API endpoint check."
            logger.exception(excp_msg2)
            endpoint_status = ("error")

    else: 
        sourcefunc_msg = f"{source_func} is not in the list of endpoint functions."
        logger.info(sourcefunc_msg)

    return endpoint_status


if __name__ == '__main__':
    get_endpoint()
