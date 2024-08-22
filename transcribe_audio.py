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
This function transcribes audio files using the OpenAI API. It saves the transcriptions in a dictionary and then saves the dictionary as a JSON file.
The function can be run in experiment mode, where it will not save the transcriptions to disk, but will return all transcriptions as a dictionary.
The parameters are:
- some_items_to_transcribe: 
    - if None, all audio files that have not been transcribed will be transcribed
    - if an integer, that number of random audio files that have not been transcribed will be transcribed
    - if "gemini" it is will transcribe the audio files that have been analyzed by the Gemini model
    - if a list of item IDs, it will transcribe those audio files, regardless if they have been previously transcribed or not
'''
def transcribe_audio(some_items_to_transcribe=None,
                    experiment_mode=False):
    from os import remove, environ
    from os.path import join, exists
    from openai import OpenAI
    from pandas import read_pickle
    import json
    import toml
    from datetime import datetime
    from numpy import random

    if experiment_mode:
        print("Running in experiment mode. No changes will be made to the transcription data saved on disk.")

    # create necessary directories if they do not exist
    fyp.create_dirs()

    cf = toml.load(fyp.CONFIG_PATH)

    gemini_video_analysis_path = join(cf["result_paths"]["main_data_dir"],cf["result_paths"]["gemini_video_analysis_fn"])
    audio_transcription_path = join(cf["result_paths"]["main_data_dir"],cf["result_paths"]["audio_transcriptions_fn"])
    environ["OPENAI_API_KEY"] = cf["openai"]["key"]

    main_bucket = fyp.get_gcp_bucket(cf["video_storage"]["GCP_bucket"])
    if main_bucket is None:
        print("Could not connect to GCP bucket. Exiting.")
        return

    start_time = datetime.now()
    print("\n"+"*"*80)
    print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Transcribing audio files in storage")
    print("*"*80+"\n")

    audio_files = fyp.list_files_in_bucket(main_bucket, prefix=join(cf["video_storage"]['prefix'],cf["video_storage"]['audio_sub_prefix']), include_sub_prefixes=False, suffix=".mp3")
    audio_id_list = [int(fn.split(".")[0]) for fn in audio_files]
    #[vid.split(".")[0] for vid in listdir(audio_dir) if vid.endswith(".mp3")]
    print(f"There are {len(audio_id_list):,} audio files in storage.")

    audio_transcriptions = {}
    if exists(audio_transcription_path):
        with open(audio_transcription_path, "r") as f:
            audio_transcriptions = json.load(f)

    transcribed_audio_ids = list(map(lambda x:int(x), audio_transcriptions.keys()))

    if isinstance(some_items_to_transcribe, list) and len(some_items_to_transcribe)>0:
        audio_ids_to_transcribe = list(set(some_items_to_transcribe) & set(audio_id_list))
        print(f"Selecting {len(audio_ids_to_transcribe):,} available videos for transcription from a list of item IDs (regardless if they already have been transcribed or not).")
    elif isinstance(some_items_to_transcribe, int) and some_items_to_transcribe>0:
        audio_ids_to_transcribe = list(set(audio_id_list) - set(transcribed_audio_ids))
        if some_items_to_transcribe>len(audio_ids_to_transcribe):
            some_items_to_transcribe = len(audio_ids_to_transcribe)
        audio_ids_to_transcribe = random.choice(audio_ids_to_transcribe, size=some_items_to_transcribe, replace=False)
        print(f"Selecting {some_items_to_transcribe:,} random available videos for transcription (that haven't already been transcribed).")
    elif isinstance(some_items_to_transcribe, str) and some_items_to_transcribe=="gemini":
        gemini_results_df = read_pickle(gemini_video_analysis_path)
        audio_ids_to_transcribe = list(set(gemini_results_df.item_id.tolist()) & set(audio_id_list) - set(transcribed_audio_ids))
        print(f"Selecting {len(audio_ids_to_transcribe):,} videos that have been analysed by Gemini (that haven't already been transcribed).")
    else:
        print(f"Selecting all available videos for transcription (that haven't already been transcribed).") 
        audio_ids_to_transcribe = [the_id for the_id in audio_id_list if not the_id in audio_transcriptions]



    if len(audio_ids_to_transcribe) == 0:
        print("All audio files have already been transcribed.")
        print("Exiting\n"+"*"*80+"\n")
        return


    import sys
    try:
        client = OpenAI()
    except:
        print("Error: OpenAI API key. Exiting.")
        sys.exit(0)

    for i,the_audio_id in enumerate(audio_ids_to_transcribe):

        print(f"{i+1:05}: Transcribing audio for {the_audio_id}")
        fyp.download_blob(main_bucket,
                          f"{the_audio_id}.mp3",
                          prefix=join(cf["video_storage"]["prefix"],cf["video_storage"]["audio_sub_prefix"]),
                          dest_dir=fyp.temp_path())
        audio_file= open(fyp.temp_path(f"{the_audio_id}.mp3"), "rb")
        transcription = client.audio.transcriptions.create(
            model=cf["openai"]["model"], 
            file=audio_file
        )
        audio_transcriptions[the_audio_id] = transcription.text
        remove(fyp.temp_path(f"{the_audio_id}.mp3"))

        if (i+1) % 100 == 0:
            with open(audio_transcription_path, "w") as json_file:
                json.dump(audio_transcriptions, json_file)
            print(f"\n{len(audio_transcriptions):,} audio transcriptions saved as {audio_transcription_path}\n")

    end_time = datetime.now()
    print(f"{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Audio transcription completed.")

    if experiment_mode:
        print("Running in experiment mode. No changes will be made to the transcription data saved on disk.")
        return audio_transcriptions
    else:
        # Backup the audio transcriptions
        fyp.back_this_up(audio_transcription_path)

        # Save the dictionary as a JSON file
        with open(audio_transcription_path, "w") as json_file:
            json.dump(audio_transcriptions, json_file)

        print(f"\n{len(audio_transcriptions):,} audio transcriptions saved as {audio_transcription_path}\n")

    print("Done\n"+"*"*80+"\n")



if __name__ == "__main__":
    transcribe_audio()