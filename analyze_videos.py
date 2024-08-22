#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Name: 
Description: 
Author: Patrik
Date: 
"""

import multiprocessing

from py_compile import compile
from importlib import reload
compile("fyp_main.py")
import fyp_main as fyp
reload(fyp)



'''
This function uses Gemini to analyze the videos in the GCP bucket.

Parameters:
- some_videos_to_analyze: None, int or list of item IDs. 
    If None, all available videos that haven't been analyzed yet will be analyzed. 
    If an integer is given, that number of random videos will be analyzed. 
    If a list of item IDs is given, those videos will be analyzed.
- experiment_mode: bool. If True, the function will not save the results to disk.
'''
def analyze_videos(some_videos_to_analyze=None,
                    experiment_mode=False):
    from os.path import join, basename, exists
    import pandas as pd
    import toml
    from datetime import datetime
    from numpy import random

    if experiment_mode:
        print("Running in experiment mode. No changes will be made to the analysis dataframe on disk.")

    # create necessary directories if they do not exist
    fyp.create_dirs()

    cf = toml.load(fyp.CONFIG_PATH)

    drop_previous_results_with_na = cf["gemini"]["drop_previous_results_with_na"]
    required_cols = cf["gemini"]["required_cols"]
    gemini_video_analysis_path = join(cf["result_paths"]["main_data_dir"],cf["result_paths"]["gemini_video_analysis_fn"])
    pyk_metadata_path = join(cf["result_paths"]["main_data_dir"],cf["result_paths"]["pyk_metadata_fn"])


    main_bucket = fyp.get_gcp_bucket(cf["video_storage"]["GCP_bucket"])
    if main_bucket is None:
        print("Could not connect to GCP bucket. Exiting.")
        return
    
    start_time = datetime.now()
    print("\n"+"*"*80)
    print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Gemini analysis of videos in the storage")
    print("*"*80+"\n")

    pyk_metadata = pd.read_pickle(pyk_metadata_path)
    videos_available_for_analysis = pyk_metadata[pyk_metadata.video_downloaded].item_id.unique()
    if len(videos_available_for_analysis)>0:
        print(f"Found {len(videos_available_for_analysis):,} videos in the metadata records.")
    else:
        print("No videos seem to be available for analysis.")
        print("Exiting.\n"+"*"*80+"\n")
        return


    # load the existing analysis data if available
    if exists(gemini_video_analysis_path):
        all_results_df = pd.read_pickle(gemini_video_analysis_path)
        all_results_df = all_results_df.sort_values("analysis_ts").drop_duplicates(subset=["item_id"],keep="last")
        all_results_df.item_id = all_results_df.item_id.astype(int)
        print(f"Found {len(all_results_df):,} previous Gemini video analyses on disk.")


        analyzed_item_ids = all_results_df.item_id.unique()

    else:
        all_results_df = pd.DataFrame()
        analyzed_item_ids = []
        print("No previous Gemini video analysis data found.")



    if isinstance(some_videos_to_analyze, list) and len(some_videos_to_analyze)>0:
        videos_to_analyze = list(set(some_videos_to_analyze) & set(videos_available_for_analysis))
        print(f"Selecting {len(videos_to_analyze):,} available videos for analysis from a list of item IDs (regardless if they already have been analysed or not).")
    elif isinstance(some_videos_to_analyze, int) and some_videos_to_analyze>0:
        videos_to_analyze = list(set(videos_available_for_analysis) - set(analyzed_item_ids))
        if some_videos_to_analyze>len(videos_to_analyze):
            some_videos_to_analyze = len(videos_to_analyze)
        videos_to_analyze = random.choice(videos_to_analyze, size=some_videos_to_analyze, replace=False)
        #videos_to_analyze = list(set(videos_to_analyze) & set(videos_available_for_analysis))
        print(f"Selecting {some_videos_to_analyze:,} random available videos for analysis (that haven't already been analyzed).")
    else:
        print(f"Selecting all available videos for analysis (that haven't already been analyzed).") 
        videos_to_analyze = list(set(videos_available_for_analysis) - set(analyzed_item_ids))

    if len(videos_to_analyze)==0:
        print("The selection rule resulted in zero videos to analyse.")
        print("Exiting.\n"+"*"*80+"\n")
        return


    print(f"Starting analysis of the {len(videos_to_analyze):,} selected videos.", end=" ", flush=True)
    if len(videos_to_analyze)>20: # There is no point estimating processing time unless there is a sufficient number of videos to be processed.
        est_proc_time = len(videos_to_analyze)*6
        print(f"Estimated processing time: {fyp.pretty_str_seconds(est_proc_time)}")
    else:
        print()

    # make a list of tuples with the video file number and the filename, to be able to follow the parallel processes a bit easier
    video_files_to_analyze_w_number = [(i+1, f"{vf}.mp4") for i,vf in enumerate(videos_to_analyze)]

    # Create a multiprocessing pool with the number of desired processes
    pool = multiprocessing.Pool(processes=8)

    # Apply the process_element function to each element in parallel
    results = pool.map(fyp.gemini_video_process, video_files_to_analyze_w_number)

    # Close the pool and wait for the processes to finish
    pool.close()
    pool.join()

    # Convert to a DataFrame
    some_results_df = pd.DataFrame(results)
    some_results_df["item_id"] = some_results_df["filename"].apply(lambda x: int(basename(x).split(".")[0]))
    some_results_df.drop("filename", axis=1, inplace=True)
    some_results_df = some_results_df.dropna()


    end_time = datetime.now()
    print(f"{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Gemini video analysis completed in {fyp.pretty_str_seconds((end_time-start_time).total_seconds())}.")


    if len(some_results_df)>0:
        print(f"{len(some_results_df)} videos successfully analysed.")
    else:
        print("No new videos processed.")
        print("Exiting.\n"+"*"*80+"\n")
        return
    
    some_results_df = some_results_df.dropna(subset=required_cols)

    print(f"Shape of the new results dataframe: {some_results_df.shape}. Shape of the good old dataframe: {all_results_df.shape}\n")

    # merge the new results with the existing ones
    all_results_df = pd.concat([all_results_df,some_results_df])

    if experiment_mode:
        print("The analysis was made in experiment mode. No changes made to the previously saved analyses on disk.")
        return some_results_df
    else:
        # backup the existing analysis data and move it to the backup folder
        fyp.back_this_up(gemini_video_analysis_path, move_the_file=True)

        all_results_df = all_results_df.dropna().sort_values("analysis_ts").drop_duplicates(subset=["item_id"], keep="last")
        print(f"Saving the combined old & new results ({len(all_results_df)} items) to {gemini_video_analysis_path}")
        all_results_df.to_pickle(gemini_video_analysis_path)

        print("Done\n"+"*"*80+"\n")


if __name__ == "__main__":
    analyze_videos()