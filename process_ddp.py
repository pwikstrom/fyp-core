#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Name: 
Description: 
Author: Patrik
Date: 
"""


from py_compile import compile
from importlib import reload
compile("fyp_main.py")
import fyp_main as fyp
reload(fyp)



def process_ddp():

    import json
    from os import listdir
    from os.path import join,basename,exists, getmtime, getctime
    import time
    from os import walk, remove
    import shutil
    import toml
    from datetime import datetime, timedelta
    import pandas as pd
    from os import devnull as os_devnull
    import subprocess


    cf = toml.load(fyp.CONFIG_PATH)

    # create necessary directories if they do not exist
    fyp.create_dirs()

    # define some paths
    ddp_dir = cf["activity_paths"]["ddp_path"]

    start_time = datetime.now()

    print("\n"+"*"*80)
    print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Processing data donation packages.")
    print("*"*80+"\n")

    item_list = []
    if ddp_dir != "":
        print(f"Loading all items from the DDP logs...")
        for u, j, k in walk(ddp_dir):
            for g in k:
                if g.endswith(".json"):
                    filename = join(u, g)
                    with open(filename) as f:
                        data_donation_package = json.load(f)
                        item_list += data_donation_package["Activity"]["Video Browsing History"]["VideoList"]
        #item_id_list = [[x for x in i["Link"].split("/") if len(x)>0][-1] for i in item_list]


    end_time = datetime.now()
    print(f"\n{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Process completed.")
    print("Done\n"+"*"*80+"\n")

    return item_list


if __name__ == "__main__":
    process_ddp()