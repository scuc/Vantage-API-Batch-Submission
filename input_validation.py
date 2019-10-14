#!/usr/bin/env python3

import logging
import os
import platform
import re
import requests

import config as cfg

from operator import itemgetter
from pathlib import Path, PurePosixPath, PureWindowsPath
from time import localtime, strftime

logger = logging.getLogger(__name__)

config = cfg.get_config()

api_endpoint_list = config['endpoint_list']
root_dir_posix = config['paths']['root_dir_posix']


def platform_check():
    '''Get the OS of the server executing the code.'''
    os_platform = platform.system()
    return os_platform


def clean_datetimes(date_str):
    '''Validate and clean user input for the start time.'''
    date_str = date_str.replace(",", "")

    while True:
        try:
            if re.search(r'[a-zA-Z]', date_str) is not None:
                print("Start Time cannot include and letter characters, try again.")
                clean_st = 0
                break

            if date_str.find(" ") > 0:
                print(
                    "Start Time can include only numbers and commas, try again. \n examples: 201810010730 or 2018,10,1,7,30 ")
                clean_st = 0
                break

            if len(date_str) != 12:
                print(
                    "Not a valid start time, try again. \n examples: 201810010730 or 2018,10,1,7,30 ")
                clean_st = 0
                break

            else:
                clean_st = date_str
                break
        except ValueError:
            print(f"{date_str} is not a valid entry for start time, try again.")
            continue

    return clean_st


def make_posix_path(source_dir):
    '''Create a valid POSIX path for the watch folder.'''
    source_dir_list = re.findall(r"[\w']+", source_dir)
    posix_path = root_dir_posix + "/".join(source_dir_list[1:])
    return posix_path


def path_validation(source_dir):
    '''Validate the user input for the watch folder file path.'''
    os_platform = platform_check()
    source_dir_list = re.findall(r"[\w']+", source_dir)

    posix_path = root_dir_posix + "/".join(source_dir_list[1:])
    windows_path = PureWindowsPath(source_dir)

    if os_platform == 'Darwin':
        p = posix_path
    else:
        p = str(windows_path)
    if p is None or os.path.isdir(p) is not True:
        valid_path = False
    else:
        valid_path = True

    return valid_path


if __name__ == '__main__':
    clean_datetimes()
