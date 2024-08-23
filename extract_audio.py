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



def extract_audio(verbose=False):

    from os.path import splitext, join, basename, exists
    from os import devnull as os_devnull, remove, listdir
    import subprocess
    from datetime import datetime
    import toml

    cf = toml.load(fyp.CONFIG_PATH)


    main_video_storage = fyp.init_video_storage(verbose=verbose)


    start_time = datetime.now()

    print("\n"+"*"*80)
    print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Extraction of audio from video files in storage")
    print("*"*80+"\n")

    video_files_before = fyp.list_files_in_storage(main_video_storage, prefix=cf["video_storage"]['prefix'], include_sub_prefixes=False, suffix=".mp4")
    audio_files_before = fyp.list_files_in_storage(main_video_storage, prefix=join(cf["video_storage"]['prefix'],cf["video_storage"]['audio_sub_prefix']), include_sub_prefixes=False, suffix=".mp3")
    
    audio_files_before = [fn for fn in audio_files_before if splitext(basename(fn))[0]+".mp4" in video_files_before]

    video_files_to_process = [fn for fn in video_files_before if splitext(fn)[0]+".mp3" not in audio_files_before]

    print(f"There are {len(video_files_before)} videos in storage.")

    if len(video_files_to_process)==0:
        print("Audio has already been extracted from all these video files.")
        print("Exiting\n"+"*"*80+"\n")
        return
    
    if len(audio_files_before)>0:
        print(f"Audio has already been extracted from {len(audio_files_before)} of these.")
    
    # create necessary directories if they do not exist
    fyp.create_dirs()

    print(f"Extracting audio from {len(video_files_to_process)} video files")

    # Loop through all mp4 files in the input directory
    for filename in video_files_to_process:
        input_file = fyp.temp_path(filename)
        audio_filename = splitext(filename)[0] + ".mp3"
        output_file = fyp.temp_path(audio_filename)
        if exists(input_file):
            remove(input_file)
        if exists(output_file):
            remove(output_file)
        fyp.load_blob_from_storage(main_video_storage, filename, prefix=cf["video_storage"]['prefix'], dest_dir=fyp.temp_path())
        
        # Convert and save to the output directory
        print(f"Extracting audio from {filename}")
        command = [
            "ffmpeg",
            "-i", input_file,
            "-q:a", "0",
            "-map", "a",
            output_file
        ]
        with open(os_devnull, 'w') as devnull:
            subprocess.run(command, check=True, stdout=devnull, stderr=devnull)
        
        print(f"Saving audio in main storage...", end="", flush=True)
        fyp.save_blob_to_storage(main_video_storage, basename(output_file), source_dir=fyp.temp_path(), prefix=join(cf['video_storage']['prefix'],"extracted_audio"))
        if exists(input_file):
            remove(input_file)
        if exists(output_file):
            remove(output_file)
        print(" done")


    audio_files_after = fyp.list_files_in_storage(main_video_storage, prefix=join(cf["video_storage"]['prefix'],cf["video_storage"]['audio_sub_prefix']), include_sub_prefixes=False, suffix=".mp3")
    audio_files_after = [fn for fn in audio_files_after if splitext(basename(fn))[0]+".mp4" in video_files_before]
    end_time = datetime.now()
    print(f"{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Audio extraction completed.")
    print(f"There are now {len(audio_files_after)} audio files in storage.")

    print("Done\n"+"*"*80+"\n")

if __name__ == "__main__":
    extract_audio()
