#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Name: 
Description: 
Author: Patrik
Date: 
"""

import fyp.fyp_main as fyp


def transcribe_audio(some_items_to_transcribe=None,
                    experiment_mode=False):
    """
    This function transcribes audio files using Whisper from OpenAI. It saves the transcriptions in a dictionary and then saves the dictionary as a JSON file.
    The function can be run in experiment mode, where it will not save the transcriptions to disk, but will return all transcriptions as a dictionary.
    The parameters are:
    - some_items_to_transcribe: 
        - if None, all audio files that have not been transcribed will be transcribed
        - if an integer, that number of random audio files that have not been transcribed will be transcribed
        - if "gemini" it is will transcribe the audio files that have been analyzed by the Gemini model
        - if a list of item IDs, it will transcribe those audio files, regardless if they have been previously transcribed or not
    """
    from os import remove, environ
    from os.path import join, exists
    from openai import OpenAI
    from pandas import read_pickle
    import json
    from datetime import datetime
    from numpy import random
    import sys

    if experiment_mode:
        print("Running in experiment mode. No changes will be made to the transcription data saved on disk.")

    cf = fyp.init_project()
    main_media_storage = fyp.init_media_storage(cf)
    
    environ["OPENAI_API_KEY"] = cf["openai"]["key"]

    start_time = datetime.now()
    print("\n"+"*"*80)
    print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Transcribing audio files in storage")
    print("*"*80+"\n")

    try:
        client = OpenAI()
    except:
        print("Error: OpenAI API key. Exiting.")
        sys.exit(0)






    # get the list of audio files in storage
    audio_files = fyp.list_files_in_storage(main_media_storage, prefix=cf["media_storage"]["audio_prefix"], include_sub_prefixes=False, suffix=".mp3")
    audio_id_list = [int(fn.split(".")[0]) for fn in audio_files]

    # load the existing transcriptions from disk (if they exist)
    audio_transcriptions = {}
    if exists(cf["paths"]["audio_transcription"]):
        with open(cf["paths"]["audio_transcription"], "r") as f:
            audio_transcriptions = json.load(f)

    # refresh the audio_transcriptions keys to be integers
    refreshed_audio_transcription_keys = {}
    for k in audio_transcriptions.keys():
        refreshed_audio_transcription_keys[int(k)] = audio_transcriptions[k]
    audio_transcriptions = refreshed_audio_transcription_keys
    #transcribed_audio_ids = list(map(lambda x:int(x), audio_transcriptions.keys()))
    transcribed_audio_ids = list(audio_transcriptions.keys())

    audio_ids_to_transcribe = list(set(audio_id_list) - set(transcribed_audio_ids))

    print(f"There are {len(audio_id_list):,} audio files in storage. {len(audio_ids_to_transcribe):,} remain to be transcribed.")



    # select the audio files to transcribe. THere are four options:
    # 1. some_items_to_transcribe is a list of item IDs. In that case, all those item IDs are transcribed, regardless if they have been transcribed or not.
    # 2. some_items_to_transcribe is an integer. In that case, that number of audio files are sampled randomly from the list of audio files that have not been transcribed yet.
    # 3. some_items_to_transcribe is "gemini". In that case, all audio files that have been analyzed by the Gemini model are transcribed, that have not been transcribed yet.
    # 4. some_items_to_transcribe is None. In that case, all audio files that have not been transcribed yet are transcribed.
    if isinstance(some_items_to_transcribe, list) and len(some_items_to_transcribe)>0:
        audio_ids_to_transcribe = list(set(some_items_to_transcribe) & set(audio_id_list))
        print(f"Selecting {len(audio_ids_to_transcribe):,} available videos for transcription from a list of item IDs (regardless if they already have been transcribed or not).")
    elif isinstance(some_items_to_transcribe, int) and some_items_to_transcribe>0:
        #audio_ids_to_transcribe = list(set(audio_id_list) - set(transcribed_audio_ids))
        if some_items_to_transcribe>len(audio_ids_to_transcribe):
            some_items_to_transcribe = len(audio_ids_to_transcribe)
        audio_ids_to_transcribe = random.choice(audio_ids_to_transcribe, size=some_items_to_transcribe, replace=False)
        print(f"Selecting {some_items_to_transcribe:,} random available videos for transcription (that haven't already been transcribed).")
    elif isinstance(some_items_to_transcribe, str) and some_items_to_transcribe=="gemini":
        gemini_results_df = read_pickle(cf["paths"]["gemini_video_analysis"])
        audio_ids_to_transcribe = list(set(gemini_results_df.item_id.tolist()) & set(audio_id_list) - set(transcribed_audio_ids))
        print(f"Selecting {len(audio_ids_to_transcribe):,} videos that have been analysed by Gemini (that haven't already been transcribed).")
    else:
        #audio_ids_to_transcribe = [the_id for the_id in audio_id_list if not the_id in transcribed_audio_ids]
        print(f"Selecting {len(audio_ids_to_transcribe):,} (all not previously transcribed) videos for transcription.") 

    # I've had trouble with the type of the itemIDs, so I am a bit paranoid.
    audio_ids_to_transcribe = list(set(map(lambda x:int(x), audio_ids_to_transcribe)))

    if len(audio_ids_to_transcribe) == 0:
        print("All audio files have already been transcribed.")
        print("Exiting\n"+"*"*80+"\n")
        return




    # iterate over the audio files to transcribe
    for i,the_audio_id in enumerate(audio_ids_to_transcribe):

        print(f"{i+1:05}: Transcribing audio for {the_audio_id}")

        # load the audio file from storage
        fyp.load_blob_from_storage(main_media_storage,
                                   f"{the_audio_id}.mp3",
                                   prefix=cf["media_storage"]["audio_prefix"],
                                   dest_dir=fyp.temp_path())
        
        # transcribe the audio file and remove the temporary file
        audio_file = open(fyp.temp_path(f"{the_audio_id}.mp3"), "rb")
        transcription = client.audio.transcriptions.create(
            model = cf["openai"]["model"], 
            file = audio_file
        )
        audio_transcriptions[the_audio_id] = transcription.text
        remove(fyp.temp_path(f"{the_audio_id}.mp3"))

        # save the transcription to disk every 100 audio files
        if (i+1) % 100 == 0:
            with open(cf["paths"]["audio_transcription"], "w") as json_file:
                json.dump(audio_transcriptions, json_file)
            print(f"\n{len(audio_transcriptions):,} audio transcriptions saved as {cf["paths"]["audio_transcription"]}\n")

    # end of the loop over the audio files to transcribe





    end_time = datetime.now()
    print(f"{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Audio transcription completed.")

    if experiment_mode:
        print("Running in experiment mode. No changes will be made to the transcription data saved on disk.")
        return audio_transcriptions
    else:
        # Backup the audio transcriptions
        fyp.back_this_up(cf["paths"]["audio_transcription"])

        # Save the dictionary as a JSON file
        with open(cf["paths"]["audio_transcription"], "w") as json_file:
            json.dump(audio_transcriptions, json_file)

        print(f"\n{len(audio_transcriptions):,} audio transcriptions saved as {cf["paths"]["audio_transcription"]}\n")

    print("Done\n"+"*"*80+"\n")



if __name__ == "__main__":
    transcribe_audio()