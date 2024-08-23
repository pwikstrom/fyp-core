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


def get_baseline_log(the_script=None):
    from shutil import move
    from os.path import basename, join, exists
    import subprocess
    import toml
    from datetime import datetime

    cf = toml.load(fyp.CONFIG_PATH)

    fyp.create_dirs()

    start_time = datetime.now()
    print("\n"+"*"*100)

    if the_script is None:
        print("No script name provided. Looking for recent zeeshuimer files in the download directory")
        the_script = "zeeschuimer"
    else:
        if "." in the_script:
            the_script = the_script.split(".")[0]
    
        print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Harvesting TikTok logs w Zeeschuimer & '{the_script}.scrpt'")
        print("*"*100+"\n")

        print(f"Running '{the_script}.scrpt' to control Firefox w Zeeschuimer extension to visit TikTok and generate logs...")
        subprocess.run([
            "osascript",
            the_script+".scrpt"
        ])
        print(f"Done running '{the_script}.scrpt'.")

    recent_files = fyp.get_recent_files("/Users/wikstrop/Downloads",
                                        suffix=".ndjson",
                                        how_recent=30)
    print(f"Found {len(recent_files)} recent Zeeschuimer file(s).")

    if len(recent_files) > 0:
        print(f"Processing the raw log file...")
        item_list = fyp.read_ndjson_file(recent_files[0]["filename"])
        some_raw_posts_df = fyp.clean_up_baseline_log(item_list)
        new_ndjson_path = join(cf["activity_paths"]["zeeschuimer_path"], 
                  the_script+basename(recent_files[0]["filename"].replace("zeeschuimer-export", ""))
                  )
        move(recent_files[0]["filename"], new_ndjson_path)
        raw_posts_df = fyp.clean_up_baseline_log(new_ndjson_path, 
                                                    label = cf['misc']['label'],
                                                    zeeschuimer_path = cf['activity_paths']['zeeschuimer_path'])
        fine_fn = new_ndjson_path.replace(cf['activity_paths']['zeeschuimer_path']+"/","").replace("/","").replace(".ndjson",".pkl")
        r = 0
        while exists(join(cf["activity_paths"]["fine_logs_path"], fine_fn)):
            r += 1
            if r ==  1:
                fine_fn = fine_fn.replace(".pkl", f"_{r:04}.pkl")
            else:
                fine_fn = fine_fn.replace(f"_{r-1:04}.pkl", f"_{r:04}.pkl")

        print(f"Saving the log file as a DataFrame: '{fine_fn}'.")
        raw_posts_df.to_pickle(join(cf["activity_paths"]["fine_logs_path"], fine_fn))
        
        # print some info about what is in some_raw_posts_df
        print(fyp.get_baseline_info_as_string(some_raw_posts_df))
    else:
        print(f"Could not find a Zeeschuimer log file.")

    end_time = datetime.now()
    print(f"{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Harvest completed in {fyp.pretty_str_seconds((end_time-start_time).total_seconds())}.")    
    print("Done\n"+"*"*80+"\n")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        get_baseline_log(sys.argv[1])
    else:
        get_baseline_log(None)
