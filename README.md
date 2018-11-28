# Vantage-API-Batch-Submission

A Python script to make batch submissions to Vantage using a REST API. 


## Project Description 

This script is for automating the submission of a large batch of video files to the Telestream Vantage software. When executed, the script will prompt the user for a series of input: start time, duration, frequency, jobs per submission, total jobs to process, watch folder path, and
the Vantage job ID. Once the input is submitted the script will begin
submitting files for processing in the workflow, and continue to
do so for as long as the user has specified. 

**Use case examples:** <br>
A company may have a large archive (maybe 100K or more) of video files that are all in a .MOV container and they need conversion to a .MP4 container. It is impossible to submit tens of thousands of files to a watch folder all at the same time, as it would overload the system. Likewise, submtting the archive to the watchfolder in small batches is not practical either because it requires constant manual attention. The script can automate the process  by submiting files to the watch folder at set intervals, and it will check the job queue before each submission to ensue that the system is not getting overloaded. 

While this script is primarily intended for automated processing of large file sets, it can also be used for stress testing of a Vantage system. The script can submit files in large regular batches in order to determine the saturation point of the system - how many jobs can it handle at any one time. 


## Prerequisites 

* Python 3.6 or higher
* Python Requests module
* Telestream Vantage 7.1 UP1, or higher
* An activated Vantage workflow that is set up to Recieve API submissions. 
* Mac or Windows OS


## Files Included

* setup.py
* vantage\_api\_main.py
* vantage\_api\_auto_submit.py


## Getting Started

After the prerequisites are downloaded and installed. 

Execute commands: 

1. Install Python 3.6 or higher and add it to you PATH.
2. Run ` python3 setup.py` to install the Requests module. 
3. Run ` python3 vantage_api_main.py` to begin the script.  
