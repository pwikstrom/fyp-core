#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Name: 
Description: 
Author: Patrik
Date: 
"""


import fyp.fyp_main as fyp


def download_videos(skip_previously_downloaded_videos=True, 
                    get_the_videos=True,
                    extract_the_audio=True,
                    get_the_covers=True,
                    some_items_to_download=None,
                    experiment_mode=False,
                    verbose=False):
    """
    Downloads videos from TikTok using PykTok. It also downloads video covers and extracts audio from the downloaded videos and save the media to the main storage.

    Parameters:
    skip_previously_downloaded_videos: if True, items that have already been downloaded will be skipped.
    get_the_videos: if True, the videos will be downloaded.
    extract_the_audio: if True, the audio will be extracted from the downloaded videos.
    get_the_covers: if True, the video covers will be downloaded.
    some_items_to_download if None, all items from logs will be downloaded. If int, that many random items will be downloaded. if list, those items will be downloaded.
    experiment_mode: if True, the script will not make any changes to the PykTok metadata file or save any media to the main storage.
    """


    import json
    from os import listdir
    from os.path import join, exists
    import time
    from os import walk
    from numpy import random
    from datetime import datetime
    import pandas as pd
    from os import devnull as os_devnull
    import subprocess
    from copy import copy
    from time import sleep

    import fyp.mypyktok as pyk
    pyk.specify_browser('chrome')




    # initialize things
    if experiment_mode:
        print("Running in experiment mode. No changes will be made to the PykTok metadata file and no metadata will be saved in the main storage.")

    cf = fyp.init_project(clear_temp_dir=True)
    main_media_storage = fyp.init_media_storage(verbose=verbose)

    if cf["misc"]["analyze_as_soon_as_videos_are_downloaded"]:
        import concurrent.futures
        executor = concurrent.futures.ThreadPoolExecutor()

    # set default values for get_the_videos, extract_the_audio, and get_the_covers
    # I need to remember these since I'm going to overwrite them in the download loop
    get_the_videos_default = copy(get_the_videos)
    extract_the_audio_default = copy(extract_the_audio)
    get_the_covers_default = copy(get_the_covers)

    start_time = datetime.now()
    print("\n"+"*"*80)
    print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Downloading video files and metadata for items in baseline logs or DDP logs using PykTok")
    print("*"*80+"\n")





    # Load list of videos where previous download attempts have failed.
    failed_downloads = []
    if exists(cf["paths"]["failed_downloads"]):
        with open(cf["paths"]["failed_downloads"], 'r') as file:
            failed_downloads = json.load(file)
        print(f"Records of {len(failed_downloads):,} previously failed download attempts loaded from disk.")
    failed_downloads = list(map(lambda x:int(x), failed_downloads))
    n_failed_downloads_before = len(failed_downloads)





    # Load the PykTok metadata file
    if exists(cf["paths"]["pyk_metadata"]):
        # backup the pyk metadata
        pyk_metadata = pd.read_pickle(cf["paths"]["pyk_metadata"])
        
        if not experiment_mode:
            # backup the pyk metadata, remove duplicates and save back to disk
            fyp.back_this_up(cf["paths"]["pyk_metadata"])
            pyk_metadata = pyk_metadata.sort_values("createTime").drop_duplicates(subset=['item_id'],keep="last")
            pyk_metadata.to_pickle(cf["paths"]["pyk_metadata"])

        # get a list of item IDs that should not be modified
        n_unmodifiable_items_in_pyk_df = pyk_metadata[pyk_metadata.do_not_modify].item_id.unique()
        print(f"Number of video metadata records: {len(n_unmodifiable_items_in_pyk_df):,}.")
    else:
        pyk_metadata = pd.DataFrame()
        n_unmodifiable_items_in_pyk_df = []





    # there are three options to control what items are downloaded:
    # 1. some_items_to_download is a list of item IDs
    # 2. some_items_to_download is an integer > 0, in which case that many random items will be sampled from the logs
    # 3. some_items_to_download is None, in which case all items in the logs (that haven't already been downloaded) will be downloaded

    # option 1
    if isinstance(some_items_to_download, list) and len(some_items_to_download) > 0:
        print(f"Downloading {len(some_items_to_download)} items from the provided list of item IDs.")
        items_to_download = some_items_to_download
    
    # for both option 2 or 3, we need to figure out the items that can _potentially_ be downloaded
    # This involves scanning logs from both zeeschuimer and data donation packages
    else:
        unique_item_id_list = []

        # load items from Zeeschuimer logs
        if cf["paths"]["zeeschuimer_raw"] != "":
            print("Loading all items from refined Zeeschuimer logs...")
            many_dfs = [pd.read_pickle(join(cf["paths"]["zeeschuimer_refined"],fn)) for fn in listdir(cf["paths"]["zeeschuimer_refined"]) if fn.endswith(".pkl")]
            if len(many_dfs)>0:
                zee_logs_df = pd.concat(many_dfs)
                unique_item_id_list += list(zee_logs_df.item_id.unique())
                    
        # load items from data donation packages
        if cf["paths"]["ddp"] != "": 
            ddp_activities = []
            print(f"Loading all items from data donation packages...")
            for u, _, k in walk(cf["paths"]["ddp"]):
                for g in k:
                    if g.endswith(".json"):
                        filename = join(u, g)
                        ddp_activities += [fyp.get_ddp_activities(filename)]
            if len(ddp_activities)>0:
                ddp_activities = pd.concat(ddp_activities)
        
                # generate item IDs from the data donation packages
                ddp_items = []
                for u in ddp_activities.Link:
                    if isinstance(u,str) and "/video/" in u:
                        new_item = u.split("/video/")[1]
                        if new_item[-1] == "/":
                            new_item = new_item[:-1]
                        ddp_items.append(int(new_item))
                unique_item_id_list += list(set(ddp_items))

        # make sure all item IDs are integers
        unique_item_id_list = list(map(lambda x:int(x), set(unique_item_id_list)))
        print(f"Total (unique) items found in all activity logs: {len(unique_item_id_list):,}.")
        items_to_download = unique_item_id_list

        # don't download items that have already been downloaded?
        if skip_previously_downloaded_videos:
            print(f"Excluding previously items which have already been downloaded or failed to download.")
            items_to_download = list(set(items_to_download) - set(n_unmodifiable_items_in_pyk_df))
            items_to_download = list(set(items_to_download) - set(failed_downloads))

    # if option 2, sample that many items from the list of potentially downloadable items
    if isinstance(some_items_to_download, int) and some_items_to_download>0:
        if some_items_to_download>len(items_to_download):
            some_items_to_download = len(items_to_download)
        items_to_download = list(map(lambda x:int(x),random.choice(items_to_download, size=some_items_to_download, replace=False)))
        
    # if there are no items to download, exit
    if len(items_to_download) == 0:
        print("No items to download.")
        print("Exiting\n"+"*"*80+"\n")
        return






    # the download loop. It consists of four steps:
    # A. Download video & metadata using PykTok
    # B. If flag is set, asynchronously launch Gemini analysis of the video
    # C. If a video was downloaded, download the video cover and extract the audio
    # D. If flag is set, asynchronously launch Gemini analysis of the audio
    print(f"Downloading media+metadata for {len(items_to_download):,} items. Media will be saved in a {"GCP bucket" if cf['media_storage']['storage_type']=="GCP" else "local directory"}")

    # iterate over items to download
    for i,an_item in enumerate(items_to_download):
        an_item = int(an_item) # just in case
        print(f"{i:04} ({an_item})", end=": ", flush=True)
        a_url = f"https://www.tiktokv.com/share/video/{an_item}/"
        trouble = ""
        ttt = ""

        # reset the download flags to the default values
        get_the_videos = copy(get_the_videos_default)
        extract_the_audio = copy(extract_the_audio_default)
        get_the_covers = copy(get_the_covers_default)

        # check what media has already been downloaded and adjust the download flags accordingly
        if len(pyk_metadata) > 0:
            this_item_metadata = pyk_metadata[pyk_metadata.item_id==an_item]
            if len(this_item_metadata) > 0:
                if this_item_metadata.iloc[0]["video_downloaded"]:
                    print("Video already downloaded", end=" | ", flush=True)
                    get_the_videos = False
                if this_item_metadata.iloc[0]["cover_downloaded"]:
                    print("Cover already downloaded", end=" | ", flush=True)
                    get_the_covers = False
                if this_item_metadata.iloc[0]["audio_extracted"]:
                    print("Audio already extracted", end=" | ", flush=True)
                    extract_the_audio = False

        # A. Download video & metadata using PykTok
        item_metadata = pd.DataFrame()
        try:
            item_metadata = pyk.save_tiktok(
                a_url,
                save_video=get_the_videos,
                browser_name='chrome',
                save_path=fyp.temp_path())
            
            ttt = " & video" if get_the_videos else ""
            if len(item_metadata) == 0:
                trouble = f"Failed to download metadata{ttt}. Further download attempts for this item abandonded."
            else:
                print(f"Downloaded metadata{ttt}", end=" | ", flush=True)
        except:
            trouble = f"Failed to download metadata{ttt}. Further download attempts for this item abandonded."

        # B. If flag is set, asynchronously launch Gemini analysis of the video
        if cf["misc"]["analyze_as_soon_as_videos_are_downloaded"] and exists(fyp.temp_path(f"{an_item}.mp4")):
            executor.submit(fyp.gemini_analysis_from_video_filename, fyp.temp_path(f"{an_item}.mp4"))


        # C. If a video was downloaded, download the video cover and extract the audio
        if len(trouble) == 0:
            # download video cover
            if get_the_covers:
                try:
                    fyp.download_video_cover(item_metadata.iloc[0]["video_cover"], fyp.temp_path(), an_item)
                    print(f"Downloaded video cover", end=" | ", flush=True)
                except:
                    trouble += "Failed to download video cover. "
            
            # extract audio from downloaded video
            if extract_the_audio:
                try:
                    # Extract audio with ffmpeg
                    command = [
                        "ffmpeg",
                        "-i", fyp.temp_path(f"{an_item}.mp4"),
                        "-q:a", "0",
                        "-map", "a",
                        fyp.temp_path(f"{an_item}.mp3")
                    ]
                    with open(os_devnull, 'w') as devnull:
                        subprocess.run(command, check=True, stdout=devnull, stderr=devnull, timeout=10)
                    print(f"Extracted audio", end=" | ", flush=True)
                except:
                    trouble += "Failed to extract audio. "

        # D. If flag is set, asynchronously launch Gemini analysis of the audio
        if cf["misc"]["analyze_as_soon_as_videos_are_downloaded"] and exists(fyp.temp_path(f"{an_item}.mp3")):
            pass
            # TODO - add a function that is transcribing a single audio file in a corresponding way to the gemini analysis of the video above (~L228)
            #executor.submit(fyp.gemini_analysis_from_video_filename, fyp.temp_path(f"{an_item}.mp3"))


        # save/upload media to storage
        if not experiment_mode:
            if True: #try: #letting it crash if something goes wrong - Change later
                uploaded_something = False
                if exists(fyp.temp_path(f"{an_item}.jpg")):
                    uploaded_something = True
                    fyp.save_blob_to_storage(main_media_storage, f"{an_item}.jpg", source_dir=fyp.temp_path(), prefix=cf["media_storage"]["video_cover_prefix"])
                    item_metadata["cover_downloaded"] = True
                if exists(fyp.temp_path(f"{an_item}.mp4")):
                    uploaded_something = True
                    fyp.save_blob_to_storage(main_media_storage, f"{an_item}.mp4", source_dir=fyp.temp_path(), prefix=cf["media_storage"]["video_prefix"])
                    item_metadata["video_downloaded"] = True
                if exists(fyp.temp_path(f"{an_item}.mp3")):
                    uploaded_something = True
                    fyp.save_blob_to_storage(main_media_storage, f"{an_item}.mp3", source_dir=fyp.temp_path(), prefix=cf["media_storage"]["audio_prefix"])
                    item_metadata["audio_extracted"] = True
            
            #except:
            #    trouble += "Failed to save any media. "            
        
        print(trouble, end=" ", flush=True)

        # save metadata
        if not experiment_mode and uploaded_something:
            print(f"Saved media to storage", end=" | ", flush=True)
        else:
            print("No media to save", end=" | ", flush=True)

        if len(item_metadata) > 0:
            pyk_metadata = pd.concat([pyk_metadata, item_metadata])

            if not experiment_mode:
                pyk_metadata.to_pickle(cf["paths"]["pyk_metadata"])
            print(f"done {len(pyk_metadata):,}")
        else:
            if not an_item in failed_downloads:
                failed_downloads.append(an_item)
            print(f"Sleeping for 3s...")
            time.sleep(3)

    # end of the download loop





    # wait for all the threads (Gemini) to finish before wrapping up
    if cf["misc"]["analyze_as_soon_as_videos_are_downloaded"]:
        print("Everything is downloaded. Waiting for all Gemini processesto finish before wrapping up. One moment...")
        executor.shutdown(wait=True)

        # the outputs from the Gemini analyses are saved as a series of json files in the temp directory
        # this section checks if there are any new json files, transform them to a df to merge with the old results
        if not experiment_mode:
            n_json_files_in_temp_dir = len([fn for fn in listdir(fyp.temp_path()) if fn.endswith('.json')])
            if n_json_files_in_temp_dir > 0:
                print(f"Analysis completed. Merging the {n_json_files_in_temp_dir} new results with the old.")
                fyp.rescue_temp_gemini_results()
            else:
                print("Found no new Gemini results in the temp directory to merge.")

    print()





    # saving the updated failed downloads list
    if not experiment_mode and len(failed_downloads) > n_failed_downloads_before:
        
        fyp.back_this_up(cf["paths"]["failed_downloads"])

        with open(cf["paths"]["failed_downloads"], 'w') as file:
            # Write the list to the file as JSON
            json.dump(failed_downloads, file)
            print(f"Updating records of failed download attempts.")






    end_time = datetime.now()
    print(f"\n{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Process completed.")
    if not experiment_mode:
        items_in_pyk_metadata_df = pd.read_pickle(cf["paths"]["pyk_metadata"]).item_id.unique()
        print(f"There are now {len(items_in_pyk_metadata_df):,} unique items in the PykTok metadata file.")

    # clear the temp dir (it won't be creating any new directories)
    fyp.create_dirs(cf, clear_temp_dir=True)  

    print("Done\n"+"*"*80+"\n")

    if experiment_mode:
        return pyk_metadata, item_metadata, failed_downloads


if __name__ == "__main__":
    download_videos()