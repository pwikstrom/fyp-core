#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Name: 
Description: 
Author: Patrik
Date: 
"""

import fyp.fyp_main as fyp


def get_baseline_log(the_script=None, how_recent=30):
    from shutil import move
    from os.path import basename, join, exists
    import subprocess
    from datetime import datetime

    cf = fyp.init_project()

    start_time = datetime.now()
    print("\n"+"*"*100)

    if the_script is None:
        print(f"No script name provided. Looking for recent zeeschuimer files in {cf["paths"]["firefox_downloads"]}")
        the_script = "zeeschuimer"
    else:
        if the_script.endswith(".scrpt"):
            the_script = the_script.replace(".scrpt", "")
    
        print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Harvesting TikTok logs w Zeeschuimer & '{basename(the_script)}'")
        print("*"*100+"\n")

        print(f"Running '{basename(the_script)}' to control Firefox w Zeeschuimer extension to visit TikTok and generate logs...")
        subprocess.run([
            "osascript",
            the_script+".scrpt"
        ])
        end_time = datetime.now()
        print(f"{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Harvest w '{basename(the_script)}' completed in {fyp.pretty_str_seconds((end_time-start_time).total_seconds())}.")    

    the_script = basename(the_script)

    recent_files = fyp.get_recent_files(cf["paths"]["firefox_downloads"],
                                        suffix=".ndjson",
                                        how_recent=how_recent)
    if len(recent_files) > 0:
        print(f"Found {len(recent_files)} recent Zeeschuimer file(s).")

        if len(recent_files) > 1:
            print(f"I'm only processing the latest one. If you want to process the other files, \
                run the script again without any arguments.")

        # the filename of the latest zeeschuimer ndjson file in the firefox downloads folder
        latest_zee_ndjson_in_firefox_downloads = recent_files[0]["filename"]
        print(f"Processing the latest Zeeschuimer log file {latest_zee_ndjson_in_firefox_downloads}")

        # create a filename for the zeeschuimer ndjson file that is more readable
        better_zee_ndjson_fn = the_script+basename(latest_zee_ndjson_in_firefox_downloads.replace("zeeschuimer-export", ""))

        # move (and rename) the latest zeeschuimer ndjson file to the folder for raw zeeschuimer logs
        new_zee_ndjson_path = join(cf["paths"]["zeeschuimer_raw"], better_zee_ndjson_fn)
        move(latest_zee_ndjson_in_firefox_downloads, new_zee_ndjson_path)

        # read the zeeschuimer log file from the new location and clean up the data
        raw_zee_log = fyp.read_ndjson_file(new_zee_ndjson_path) # NOTE - only processing the latest file
        refined_zee_log = fyp.refine_zeeschuimer_log(raw_zee_log)

        # create a filename for the zeeschuimer pickle file by just replacing the suffix
        zee_pickle_fn = better_zee_ndjson_fn.replace(".ndjson",".pkl")

        # make sure the filename for the pickle file is unique
        r = 0
        while exists(join(cf["paths"]["zeeschuimer_refined"], zee_pickle_fn)):
            r += 1
            if r ==  1:
                zee_pickle_fn = zee_pickle_fn.replace(".pkl", f"_{r:04}.pkl")
            else:
                zee_pickle_fn = zee_pickle_fn.replace(f"_{r-1:04}.pkl", f"_{r:04}.pkl")

        # save the refined zeeschuimer log as a pickle file
        print(f"Saving the log file as a DataFrame: '{zee_pickle_fn}'.")
        refined_zee_log.to_pickle(join(cf["paths"]["zeeschuimer_refined"], zee_pickle_fn))
        
        # print some info about what is in refined_zee_log
        print(fyp.get_baseline_info_as_string(refined_zee_log))
    else:
        print(f"Could not find a Zeeschuimer ndjson file in the firefox downloads folder.")

    end_time = datetime.now()
    print(f"{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Process completed in {fyp.pretty_str_seconds((end_time-start_time).total_seconds())}.")    
    print("Done\n"+"*"*80+"\n")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        get_baseline_log(sys.argv[1])
    else:
        get_baseline_log(None)
