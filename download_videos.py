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


'''
Downloads videos from TikTok using PykTok. It also downloads video covers and extracts audio from the downloaded videos and uploads the media to a GCP bucket.

Parameters:
skip_previously_downloaded_videos: if True, items that have already been downloaded will be skipped.
get_the_videos: if True, the videos will be downloaded.
extract_the_audio: if True, the audio will be extracted from the downloaded videos.
get_the_covers: if True, the video covers will be downloaded.
some_items_to_download if None, all items from logs will be downloaded. If int, that many random items will be downloaded. if list, those items will be downloaded.
experiment_mode: if True, the script will not make any changes to the PykTok metadata file or upload any media to the GCP bucket.
'''
def download_videos(skip_previously_downloaded_videos=True, 
                    get_the_videos=True,
                    extract_the_audio=True,
                    get_the_covers=True,
                    some_items_to_download=None,
                    experiment_mode=False):

    import json
    from os import listdir
    from os.path import join, exists
    import time
    from os import walk, remove
    from numpy import random
    import toml
    from datetime import datetime
    import pandas as pd
    from os import devnull as os_devnull
    import subprocess
    from copy import copy

    get_the_videos_default = copy(get_the_videos)
    extract_the_audio_default = copy(extract_the_audio)
    get_the_covers_default = copy(get_the_covers)

    if experiment_mode:
        print("Running in experiment mode. No changes will be made to the PykTok metadata file and no metadata will be uploaded to the GCP bucket.")

    compile("mypyktok.py")
    import mypyktok as pyk
    reload(pyk)
    pyk.specify_browser('chrome')


    cf = toml.load(fyp.CONFIG_PATH)

    # create necessary directories if they do not exist
    fyp.create_dirs()

    # define some paths
    zeeschuimer_dir = cf["input_paths"]["zeeschuimer_path"]
    ddp_dir = cf["input_paths"]["ddp_path"]
    pyk_metadata_path = join(cf["result_paths"]["main_data_dir"],cf["result_paths"]["pyk_metadata_fn"])


    main_bucket = fyp.get_gcp_bucket(cf["video_storage"]["GCP_bucket"])
    if main_bucket is None:
        print("Could not connect to GCP bucket. Exiting.")
        return

    start_time = datetime.now()

    print("\n"+"*"*80)
    print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Downloading video files and metadata for items in baseline logs or DDP logs using PykTok")
    print("*"*80+"\n")

    # Some videos may have failed to download. We will keep track of these in a list.
    failed_items = []
    failed_items_path = join(cf["result_paths"]["main_data_dir"], cf["result_paths"]["pyk_failed_items_fn"])
    if exists(failed_items_path):
        with open(failed_items_path, 'r') as file:
            failed_items = json.load(file)
        print(f"Records of {len(failed_items):,} previously failed download attempts loaded from disk.")
    failed_items = list(map(lambda x:int(x), failed_items))
    n_failed_items_before = len(failed_items)

    if exists(pyk_metadata_path):
        # backup the pyk metadata
        pyk_metadata = pd.read_pickle(pyk_metadata_path)
        

        if not experiment_mode:
            fyp.back_this_up(pyk_metadata_path)
            pyk_metadata = pyk_metadata.sort_values("createTime").drop_duplicates(subset=['item_id'],keep="last")
            pyk_metadata.to_pickle(pyk_metadata_path)
        n_unmodifiable_items_in_pyk_df = pyk_metadata[pyk_metadata.do_not_modify].item_id.unique()
        print(f"Number of 'unmodifiable' video metadata records in {cf["result_paths"]["pyk_metadata_fn"]}: {len(n_unmodifiable_items_in_pyk_df):,}.")
    else:
        pyk_metadata = pd.DataFrame()
        n_unmodifiable_items_in_pyk_df = []


    print(f"Saving media in {cf['video_storage']['GCP_bucket']} and metadata in {cf["result_paths"]["pyk_metadata_fn"]}.")

    if isinstance(some_items_to_download, list) and len(some_items_to_download) > 0:
        print("Downloading {len(some_items_to_download)} items from a list of item IDs.")
        items_to_download = list(set(map(lambda x:int(x), some_items_to_download)))

    elif isinstance(some_items_to_download, int) and some_items_to_download>0:
        items_to_download = random.choice(items_to_download, size=some_items_to_download, replace=False)
        print(f"Downloading stuff for {some_items_to_download:,} random items.", end=" ", flush=True)

    else:
        unique_item_id_list = []
        if zeeschuimer_dir != "":
            print("Loading all items from Zeeschuimer logs...")
            raw_posts_df = pd.concat([pd.read_pickle(join(cf["input_paths"]["fine_logs_path"],fn)) for fn in listdir(cf["input_paths"]["fine_logs_path"]) if fn.endswith(".pkl")])
            unique_item_id_list += list(raw_posts_df.item_id.unique())
                    
        if ddp_dir != "":
            ddp_activities = []
            print(f"Loading all items from data donation packages...")
            for u, _, k in walk(ddp_dir):
                for g in k:
                    if g.endswith(".json"):
                        filename = join(u, g)
                        ddp_activities += [fyp.get_ddp_activities(filename)]
            ddp_activities = pd.concat(ddp_activities)
        
            ddp_items = []
            for u in ddp_activities.Link:
                if isinstance(u,str) and "/video/" in u:
                    new_item = u.split("/video/")[1]
                    if new_item[-1] == "/":
                        new_item = new_item[:-1]
                    ddp_items.append(int(new_item))
            unique_item_id_list += list(set(ddp_items))

        unique_item_id_list = list(map(lambda x:int(x), set(unique_item_id_list)))
        print(f"Total (unique) items found in all activity logs: {len(unique_item_id_list):,}.")
        items_to_download = unique_item_id_list

        # exclude items that have already been downloaded?
        if skip_previously_downloaded_videos:
            print(f"Excluding previously items which have already been downloaded or failed to download.")
            items_to_download = list(set(items_to_download) - set(n_unmodifiable_items_in_pyk_df))
            items_to_download = list(set(items_to_download) - set(failed_items))
        
        print(f"Downloading stuff for {len(items_to_download):,} items.")


    if len(items_to_download) == 0:
        print("No items to download.")
        print("Exiting\n"+"*"*80+"\n")
        return


    for i,an_item in enumerate(items_to_download):
        print(f"{i:04} ({an_item})", end=": ", flush=True)
        a_url = f"https://www.tiktokv.com/share/video/{an_item}/"
        trouble = ""
        ttt = ""

        get_the_videos = copy(get_the_videos_default)
        extract_the_audio = copy(extract_the_audio_default)
        get_the_covers = copy(get_the_covers_default)

        this_item_metadata = pyk_metadata[pyk_metadata.item_id==an_item]
        if len(this_item_metadata) > 0:
            #print(this_item_metadata.iloc[0]["video_downloaded"], this_item_metadata.iloc[0]["cover_downloaded"], this_item_metadata.iloc[0]["audio_extracted"])
            if this_item_metadata.iloc[0]["video_downloaded"]:
                print("Video already downloaded", end=" | ", flush=True)
                get_the_videos = False
            if this_item_metadata.iloc[0]["cover_downloaded"]:
                print("Cover already downloaded", end=" | ", flush=True)
                get_the_covers = False
            if this_item_metadata.iloc[0]["audio_extracted"]:
                print("Audio already extracted", end=" | ", flush=True)
                extract_the_audio = False

        # download video & metadata
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
        
        # upload stuff to GCP bucket
        if not experiment_mode:
            try:
                uploaded_something = False
                if exists(fyp.temp_path(f"{an_item}.jpg")):
                    uploaded_something = True
                    fyp.upload_blob(main_bucket, f"{an_item}.jpg", source_dir=fyp.temp_path(), prefix=join(cf["video_storage"]['prefix'], cf["video_storage"]['video_cover_prefix']))
                    remove(fyp.temp_path(f"{an_item}.jpg"))
                    item_metadata["cover_downloaded"] = True
                if exists(fyp.temp_path(f"{an_item}.mp4")):
                    uploaded_something = True
                    fyp.upload_blob(main_bucket, f"{an_item}.mp4", source_dir=fyp.temp_path(), prefix=cf['video_storage']['prefix'])
                    remove(fyp.temp_path(f"{an_item}.mp4"))
                    item_metadata["video_downloaded"] = True
                if exists(fyp.temp_path(f"{an_item}.mp3")):
                    uploaded_something = True
                    fyp.upload_blob(main_bucket, f"{an_item}.mp3", source_dir=fyp.temp_path(), prefix=join(cf["video_storage"]['prefix'], cf["video_storage"]['audio_sub_prefix']))
                    remove(fyp.temp_path(f"{an_item}.mp3"))
                    item_metadata["audio_extracted"] = True
            
            except:
                trouble += "Failed to upload any media. "            

        print(trouble, end=" ", flush=True)
        
        if not experiment_mode and uploaded_something:
            print(f"Uploaded media to {cf['video_storage']['GCP_bucket']}", end=" | ", flush=True)
        else:
            print("No media to upload", end=" | ", flush=True)

        if len(item_metadata) > 0:
            pyk_metadata = pd.concat([pyk_metadata, item_metadata])

            if not experiment_mode:
                pyk_metadata.to_pickle(pyk_metadata_path)
            print(f"done {len(pyk_metadata):,}")
        else:
            if not an_item in failed_items:
                failed_items.append(an_item)
            print(f"Sleeping for 3s...")
            time.sleep(3)


    print()
    # saving the updated failed items list
    if not experiment_mode and len(failed_items) > n_failed_items_before:
        
        fyp.back_this_up(failed_items_path)

        with open(failed_items_path, 'w') as file:
            # Write the list to the file as JSON
            json.dump(failed_items, file)
            print(f"Updating records of failed download attempts.")



    end_time = datetime.now()
    print(f"\n{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Process completed.")
    if not experiment_mode:
        items_in_pyk_metadata_df = pd.read_pickle(pyk_metadata_path).item_id.unique()
        print(f"There are now {len(items_in_pyk_metadata_df):,} unique items in the PykTok metadata file.")
    print("Done\n"+"*"*80+"\n")

    if experiment_mode:
        return pyk_metadata, item_metadata, failed_items


if __name__ == "__main__":
    download_videos()