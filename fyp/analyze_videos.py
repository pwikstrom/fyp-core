#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Name: 
Description: 
Author: Patrik
Date: 
"""

import fyp.fyp_main as fyp



def analyze_videos(some_videos_to_analyze=None,
                    experiment_mode=False,
                    verbose=False):
    """This function uses Gemini video analysis.

    Parameters:
    - some_videos_to_analyze: None, int or list of item IDs. 
        If None, all available videos that haven't been analyzed yet will be analyzed. 
        If an integer is given, that number of random videos will be analyzed. 
        If a list of item IDs is given, those videos will be analyzed.
    - experiment_mode: bool. If True, the function will not save the results to disk.
    """
    import multiprocessing
    from os.path import exists
    import pandas as pd
    from datetime import datetime
    from numpy import random

    if experiment_mode:
        print("Running in experiment mode. No changes will be made to the analysis dataframe on disk.")

    cf = fyp.init_project()

    start_time = datetime.now()
    print("\n"+"*"*80)
    print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Gemini analysis of videos in the storage")
    print("*"*80+"\n")



    # load the metadata
    pyk_metadata = pd.read_pickle(cf["paths"]["pyk_metadata"])
    videos_available_for_analysis = pyk_metadata[pyk_metadata.video_downloaded].item_id.unique()
    if len(videos_available_for_analysis)>0:
        print(f"Found {len(videos_available_for_analysis):,} videos in the metadata records.")
    else:
        print("No videos seem to be available for analysis.")
        print("Exiting.\n"+"*"*80+"\n")
        return



    # load the existing analysis data if available
    if exists(cf["paths"]["gemini_video_analysis"]):
        all_results_df = pd.read_pickle(cf["paths"]["gemini_video_analysis"])
        all_results_df.analysis_ts = all_results_df.analysis_ts.map(lambda x: int(x) if type(x)==float or type(x)==int else int(x.timestamp()))
        all_results_df = all_results_df.sort_values("analysis_ts").drop_duplicates(subset=["item_id"],keep="last")
        all_results_df.item_id = all_results_df.item_id.astype(int)
        print(f"Found {len(all_results_df):,} previous Gemini video analyses on disk.")
        analyzed_item_ids = all_results_df.item_id.unique()
    else:
        all_results_df = pd.DataFrame()
        analyzed_item_ids = []
        print("No previous Gemini video analysis data found.")





    # select the videos to analyze. There are three options:
    # 1. some_videos_to_analyze is a list of item IDs. In that case, all those item IDs are analyzed, regardless if they have been analyzed or not.
    # 2. some_videos_to_analyze is an integer. In that case, that number of videos are sampled randomly from the list of videos that have not been analyzed yet.
    # 3. some_videos_to_analyze is None. In that case, all videos that have not been analyzed yet are analyzed.
    if isinstance(some_videos_to_analyze, list) and len(some_videos_to_analyze)>0:
        videos_to_analyze = list(set(some_videos_to_analyze) & set(videos_available_for_analysis))
        print(f"Selecting {len(videos_to_analyze):,} available videos for analysis from a list of item IDs (regardless if they already have been analysed or not).")
    elif isinstance(some_videos_to_analyze, int) and some_videos_to_analyze>0:
        videos_to_analyze = list(set(videos_available_for_analysis) - set(analyzed_item_ids))
        if some_videos_to_analyze>len(videos_to_analyze):
            some_videos_to_analyze = len(videos_to_analyze)
        videos_to_analyze = random.choice(videos_to_analyze, size=some_videos_to_analyze, replace=False)
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

    # make a list of 3-element-lists containing the video file number, the filename and the config-dict. 
    # This is the argument package that is sent with each video to the gemini_video_process function.
    arg_list_for_gemini_video_process = [[i+1, f"{vf}.mp4", cf] for i,vf in enumerate(videos_to_analyze)]

    # let Gemini do its thing
    pool = multiprocessing.Pool(processes=8)
    results = pool.map(fyp.gemini_video_process, arg_list_for_gemini_video_process)
    pool.close()
    pool.join()

    # Convert to a DataFrame
    some_results_df = pd.DataFrame(results)
    some_results_df = some_results_df.dropna()






    end_time = datetime.now()
    print(f"{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Gemini video analysis completed in {fyp.pretty_str_seconds((end_time-start_time).total_seconds())}.")

    if len(some_results_df)>0:
        print(f"{len(some_results_df)} videos successfully analysed.")
    else:
        print("No new videos processed.")
        print("Exiting.\n"+"*"*80+"\n")
        return
    
    print(f"Shape of the new results dataframe: {some_results_df.shape}. Shape of the good old dataframe: {all_results_df.shape}\n")

    # merge the new results with the existing ones
    all_results_df = pd.concat([all_results_df,some_results_df])

    if experiment_mode:
        print("The analysis was made in experiment mode. No changes made to the previously saved analyses on disk.")
        return some_results_df
    else:
        # backup the existing analysis data and move it to the backup folder
        fyp.back_this_up(cf["paths"]["gemini_video_analysis"], move_the_file=True)

        all_results_df.analysis_ts = all_results_df.analysis_ts.map(lambda x: int(x) if type(x)==float or type(x)==int else int(x.timestamp()))
        all_results_df = all_results_df.dropna().sort_values("analysis_ts").drop_duplicates(subset=["item_id"], keep="last")
        print(f"Saving the combined old & new results ({len(all_results_df)} items) to {cf["paths"]["gemini_video_analysis"]}")
        all_results_df.to_pickle(cf["paths"]["gemini_video_analysis"])

        print("Done\n"+"*"*80+"\n")


if __name__ == "__main__":
    analyze_videos()