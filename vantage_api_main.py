#!/usr/bin/env python3


import vantage_api_auto_submit as vn

from time import strftime


'''set the varibales for the script.'''

def vantage_main():

    vn_Vars = vn.print_intro()

    start_time = vn_Vars[0]
    total_duration = vn_Vars[1]
    submit_frequency = vn_Vars[2]
    jobs_per_submit = vn_Vars[3]
    sources_in_rotation = vn_Vars[4]
    source_dir = vn_Vars[5]
    target_workflow_id = vn_Vars[6]


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

    vn.api_submit(total_duration, submit_frequency, jobs_per_submit, sources_in_rotation, source_dir, target_workflow_id)


vantage_main()
