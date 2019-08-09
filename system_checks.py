
import logging
import requests

from operator import itemgetter
from time import localtime, strftime

logger = logging.getLogger(__name__)

# ===================== DOMAIN AND API ENPOINT CHECKS ======================= #


def check_vantage_status(target_workflow_id, api_endpoint):
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

    global job_check_count
    job_check_count = 0

    while True:

        try:
            api_endpoint = api_endpoint_check(api_endpoint)
            domain_load = check_domain_load(job_check_count, api_endpoint)
            job_queue = check_job_queue(target_workflow_id, api_endpoint, job_check_count)

            print("API_ENDPOINT: " + api_endpoint)

            db.update_db(api_endpoint, target_workflow_id)

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
                print(msg1)

            elif status_val in msg2_list:
                msg2 =f"\n\
                Job Check Count:  {str(job_check_count)}\n\
                Active Job Count:  {str(job_queue[1])}\n\
                Domain Load:   {str(domain_load[1])}\n\
                "
                logger.info(msg2)
                print(msg2)

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
                print(msg3)

            else:
                break

            time.sleep(60)
            job_check_count += 1

        except Exception as excp:
            vanstatus_excp_msg = f"Exception raised on a Vantage Dominan Status check."
            logger.exception(vanstatus_excp_msg)
            print(vanstatus_excp_msg)
            print(str(excp))

            if Exception is requests.exceptions.RequestException:
                api_endpoint = api_endpoint_check(api_endpoint)
                continue
            else:
                break

    return api_endpoint


def check_domain_load(job_check_count, api_endpoint):
    '''Get a Domain Load based on Transcode, CPU, Edit, and Analysis'''

    try:
        api_endpoint = api_endpoint_check(api_endpoint)
        global root_uri
        root_uri = "http://" + str(api_endpoint) + ":8676/"

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
        global sorted_serviceload_list
        sorted_serviceload_list = sorted(service_load_list, key=get_load, reverse=True)

        high_load_list = []
        low_load_list = []

        for service_num in sorted_serviceload_list:
                # print("SERVICE NUM:" + str(service_num[1]))
                if service_num[1] > 80:
                    high_load_list.append(service_num)
                else:
                    low_load_list.append(service_num)

        global domain_load_val
        domain_load_val = 0

        if len(high_load_list) > 0:
            domain_load_val = 1
        else:
            domain_load_val = 0

    except requests.exceptions.RequestException as excp:
        domainck_excp_msg = f"Exception raised on a Vantage Dominan Load check."
        logger.exception(domainck_excp_msg)
        print(domainck_excp_msg)
        print(str(excp))

        api_endpoint = api_endpoint_failover(api_endpoint)
        check_domain_load(job_check_count, api_endpoint)

    return [domain_load_val, sorted_serviceload_list]


def check_job_queue(target_workflow_id, api_endpoint, job_check_count):
    '''Check for the number of the jobs running  in the given workflow, prevent the script from overloading the Vantage system.'''

    while True:
        try:
            api_endpoint = api_endpoint_check(api_endpoint)
            global root_uri
            root_uri = "http://" + str(api_endpoint) + ":8676/"

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

            api_endpoint = api_endpoint_failover(api_endpoint)
            check_domain_load(job_check_count, api_endpoint)

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


def api_endpoint_failover(api_endpoint):

        while True:
            try:
                api_fail = f"\n\
                =======================================================\
                {str(strftime('%A, %d. %B %Y %I:%M%p', localtime()))} \
                Removing {api_endpoint} from the list of available api endpoints.\
                Attempting to switch to a new API Endpoint now.\
                =======================================================\
                "
                logger.info(api_fail)
                print(api_fail)

                new_api_endpoint = api_endpoint_list[0]
                api_endpoint = new_api_endpoint
                root_uri = "http://" + api_endpoint + ":8676/"

                api_new = f"\
                Switching to new API Endpoint:  {api_endpoint}"
                logger.info(api_new)
                print(api_new)
                break

            except requests.exceptions.RequestException as err:
                print("\n{} is not active or unreachable, trying another api_endpoint.".format(new_api_endpoint) + "\n" + str(err))
                if len(api_endpoint_list) > 1:
                    api_endpoint_list.remove(api_endpoint)
                    continue
                else:
                    print("Vantage cannot find an available api endpoint. Please check the Vantage SDK service on the Lighspeed servers is started and reachable.\n")
                    print("After confirming an available API Enpoint, please hit return to contine.")
                    continue

        return api_endpoint
