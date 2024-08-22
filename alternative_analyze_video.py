#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Name: 
Description: 
Author: Patrik
Date: 

This is an alternative version of the analyze video module. I thought it would be faster, but it isn't.
Might be that I need to improve the multiprocessing thingy. So if anyone would like to look at that, please do...

"""


from py_compile import compile
from importlib import reload
compile("fyp_main.py")
import fyp_main as fyp
reload(fyp)


import toml
cf = toml.load(CONFIG_PATH)
my_gemini_api_key=cf["gemini"]["key"]

with open('gemini_prompt.txt', 'r') as file:
    gemini_prompt = file.read()

# Create the model
generation_config = {
    "temperature": cf["gemini"]["temperature"],
    "top_p": cf["gemini"]["top_p"],
    "top_k": cf["gemini"]["top_k"],
    "max_output_tokens": cf["gemini"]["max_output_tokens"],
    "response_mime_type": "application/json",
}





def upload_video_file(the_video_filename):
    from google.generativeai import upload_file, configure
    from os import remove

    configure(api_key=my_gemini_api_key)
    file = upload_file(the_video_filename, mime_type="video/mp4")
    remove(the_video_filename)
    return file







def analyze_this(this_file, verbose=False, timeout=20):
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
    from google.generativeai import GenerativeModel, configure
    from json import loads
    from google.generativeai import types as gemini_types
    from datetime import datetime
    from os.path import join
    from json import dump

    start_time = datetime.now()

    configure(api_key=my_gemini_api_key)

    if not isinstance(this_file,gemini_types.file_types.File):
        print("Not a gemini file object. Cannot analyse whatever this is.")
        return {}

    model = GenerativeModel(
        model_name=cf["gemini"]["model"],
        generation_config=generation_config,
        safety_settings={
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE}
    )
    
    try:
        response = model.generate_content([this_file, gemini_prompt],
                                        request_options={"timeout": timeout})
        response_received = True
    except Exception as e:
        if verbose:
            print(e)
        response_received = False

    raw_json = {}
    if response_received == False:
        if verbose:
            print(f"No response received")
    elif response.candidates[0].finish_reason != 1:
        if verbose:
            print(f"Finish reason: {response.candidates[0].finish_reason}")
    else:
        raw_string = response.candidates[0].content.parts[0].text
        if raw_string.endswith("\n"):
            raw_string = raw_string[:-1]
        try:
            raw_json = loads(raw_string)
        except:
            raw_json = {}
        if not raw_string.endswith('"}'):
            raw_string += '"}'
            try:
                raw_json = loads(raw_string)
            except:
                if verbose:
                    print("Couldn't convert response to JSON")
    
    good_strings = True
    if isinstance(raw_json, dict) and len(raw_json) > 0:
        for cc in cf["gemini"]["doublecheck_these_cols"]:
            if cc in raw_json:
                check_result = check_repetitive_patterns(raw_json[cc], min_pattern_length=5, min_repetitions=5, max_text_length=1000)
                if check_result == "String too long" and cc == "text_visible_in_video":
                    if verbose:
                        print(f"{cc} is too long - cutting and checking again.")
                    check_result = check_repetitive_patterns(raw_json[cc][:1000], min_pattern_length=5, min_repetitions=5, max_text_length=1000)
                if check_result != "Good string":
                    good_strings = False
    else:
        good_strings = False

    # add the display name to the raw_json, to avoid mixups
    raw_json["item_id"] = int(this_file.display_name.split(".")[0])
    raw_json["good_strings"] = good_strings
    raw_json["analysis_time"] = (datetime.now() - start_time).total_seconds()
    raw_json["processing_time"] = 0.0
    raw_json["analysis_ts"] = int(datetime.now().timestamp())

    # Save the result at this stage for this single video as a precaution if it all blows up
    temp_fn = join(temp_path(f"temp_gemini_results_{raw_json["item_id"]}_{"".join([ch for ch in str(datetime.now()) if ch in '1234567890'])}.json"))
    with open(temp_fn, 'w') as file:
        dump(raw_json,file)

    return raw_json







def refresh_gemini_file(fn):
    from google.generativeai import get_file, configure
    configure(api_key=my_gemini_api_key)
    the_gemini_file = get_file(fn)
    finers = (the_gemini_file.name, the_gemini_file.state.name)
    return finers





def analyze_videos(some_videos_to_analyze=None,
                   experiment_mode=False,
                   verbose=True):
    from datetime import datetime
    from google.cloud.storage import transfer_manager
    from google.generativeai import configure
    from os.path import exists, join
    from pandas import read_pickle, DataFrame, concat
    from numpy import random
    import multiprocessing
    import warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning)


    if experiment_mode:
        print("Running in experiment mode. No changes will be made to the analysis dataframe on disk.")

    configure(api_key=my_gemini_api_key)

    start_time = datetime.now()
    print("\n"+"*"*80)
    print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Gemini analysis of videos in the storage")
    print("*"*80+"\n")

    if verbose:
        print("Connecting to GCP bucket...")
    main_bucket = get_gcp_bucket(cf["video_storage"]["GCP_bucket"])
    if main_bucket is None:
        print("Could not connect to GCP bucket. Exiting.")
        return


    drop_previous_results_with_na = cf["gemini"]["drop_previous_results_with_na"]
    required_cols = cf["gemini"]["required_cols"]
    gemini_video_analysis_path = join(cf["result_paths"]["main_data_dir"],cf["result_paths"]["gemini_video_analysis_fn"])
    pyk_metadata_path = join(cf["result_paths"]["main_data_dir"],cf["result_paths"]["pyk_metadata_fn"])



    # Load the PykTok metadata
    if exists(pyk_metadata_path):
        pyk_metadata = read_pickle(pyk_metadata_path)
        videos_available_for_analysis = pyk_metadata[pyk_metadata.video_downloaded].item_id.unique()
        if verbose:
            print(f"Number of videos available for analysis: {len(videos_available_for_analysis):,}.")
    else:
        if verbose:
            print(f"Pyk metadata file {cf["result_paths"]["pyk_metadata_fn"]} not found. Exiting.")
        return


    # load the existing analysis data if available
    if exists(gemini_video_analysis_path):
        all_results_df = read_pickle(gemini_video_analysis_path)
        all_results_df = all_results_df.sort_values("analysis_ts").drop_duplicates(subset=["item_id"],keep="last")
        all_results_df.item_id = all_results_df.item_id.astype(int)
        print(f"Found {len(all_results_df):,} previous Gemini video analyses on disk.")
        #print(f"Found {len(all_results_df):,} previous Gemini video analyses - {len(all_results_df.dropna(subset=required_cols)):,} without NA in the required columns")

        # Drop or don't drop the previous results with NA values in the checked columns
        #if drop_previous_results_with_na:
        #    all_results_df = all_results_df.dropna(subset=required_cols)
        #    print(f"Discarding previous results with NA values in required <{'> + <'.join(required_cols)}> column{'s' if len(required_cols) > 1 else ''}. {len(all_results_df)} previous results remain.")
        #else:
        #    print(f"Keeping previous results, including the ones with NA values in required <{'> + <'.join(required_cols)}> column{'s' if len(required_cols) > 1 else ''}.")

        analyzed_item_ids = all_results_df.item_id.unique()
    else:
        all_results_df = DataFrame()
        analyzed_item_ids = []
        print("No previous Gemini video analysis data found.")



    # Select the videos to analyze
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

    # Download videos from the bucket to the temp directory
    the_bucket_filenames = [join(cf["video_storage"]["prefix"],f"{sdf}.mp4") for sdf in videos_to_analyze]
    if verbose:
        print("Downloading video objects from bucket...")
        print(datetime.now())
    results = transfer_manager.download_many_to_path(main_bucket,
                                                        the_bucket_filenames,
                                                        destination_directory=temp_path(),
                                                        max_workers=8)


    # Uploading the video objects from the temp directory to Gemini
    if verbose:
        print("Uploading video objects to Gemini...")
        print(datetime.now())
    video_files_in_temp_dir = [temp_path(fn) for fn in the_bucket_filenames]
    pool = multiprocessing.Pool()
    the_gemini_files = pool.map(upload_video_file, video_files_in_temp_dir)
    pool.close()
    pool.join()


    # Check if the files are ready for analysis
    if verbose:
        print("Waiting for files to be ready for analysis...")
        print(datetime.now())
    active_gemini_filenames = []

    todo_gemini_filenames = [gf.name for gf in the_gemini_files if gf.state.name == "PROCESSING"]
    active_gemini_filenames += [gf.name for gf in the_gemini_files if gf.state.name == "ACTIVE"]

    while len(todo_gemini_filenames)>0:

        pool = multiprocessing.Pool()
        results = pool.map(refresh_gemini_file, todo_gemini_filenames, chunksize=1)
        pool.close()
        pool.join()

        active_gemini_filenames += [gf[0] for gf in results if gf[1] == "ACTIVE"]
        todo_gemini_filenames = [gf[0] for gf in results if gf[1] == "PROCESSING"]
    print(f"{len(active_gemini_filenames)} file(s) ready for analysis.")

    # these are the gemini files we are able to analyse
    active_gemini_files = [gf for gf in the_gemini_files if gf.name in active_gemini_filenames]



    # Let Gemini do its thing to generate some results
    if verbose:
        print("Analyzing videos...")
        print(datetime.now())
    pool = multiprocessing.Pool()
    gemini_results = pool.map(analyze_this, active_gemini_files)
    pool.close()
    pool.join()



    # Filter out the results that were "not good"
    good_take1_results = []
    files_to_analyze_again = []
    for i,gf in enumerate(gemini_results):
        if gf.get("good_strings",False) == True:
            good_take1_results += [gf]
        else:
            files_to_analyze_again += [active_gemini_files[i]]
        

    # If there are files that need to be analyzed again, we will do that
    if len(files_to_analyze_again) == 0:
        if verbose:
            print("All results are good.")
    else:
        if verbose:
            print(f"Some of the results were not good. Re-analyzing {len(files_to_analyze_again)} files...")

        # same routine as in the first round
        pool = multiprocessing.Pool()
        gemini_take2_results = pool.map(analyze_this, files_to_analyze_again)
        pool.close()
        pool.join()


        # and then I add the redone results to the original results
        gemini_results = good_take1_results + gemini_take2_results


    some_results_df = DataFrame(gemini_results)
    some_results_df.drop("good_strings", axis=1, inplace=True)
    
    some_results_df = some_results_df.dropna()
    some_results_df["analysis_ts"] = some_results_df["analysis_ts"].map(lambda x:datetime.fromtimestamp(x))


    end_time = datetime.now()
    print(f"{end_time.strftime('%Y-%m-%d %H:%M:%S')}: Gemini video analysis completed in {pretty_str_seconds((end_time-start_time).total_seconds())}.")


    if len(some_results_df)>0:
        print(f"{len(some_results_df)} videos successfully analysed.")
    else:
        print("No new videos processed.")
        print("Exiting.\n"+"*"*80+"\n")
        return
    
    print(f"Shape of the new results dataframe: {some_results_df.shape}. Shape of the good old dataframe: {all_results_df.shape}\n")

    if experiment_mode:
        print("The analysis was made in experiment mode. No changes made to the previously saved analyses on disk.")
        return some_results_df
    else:
        # merge the new results with the existing ones
        all_results_df = concat([all_results_df,some_results_df])

        # backup the existing analysis data and move it to the backup folder
        back_this_up(gemini_video_analysis_path, move_the_file=True)

        all_results_df = all_results_df.dropna().sort_values("analysis_ts").drop_duplicates(subset=["item_id"], keep="last")
        print(f"Saving the combined old & new results ({len(all_results_df)} items) to {gemini_video_analysis_path}")
        all_results_df.to_pickle(gemini_video_analysis_path)

        print("Done\n"+"*"*80+"\n")


if __name__ == "__main__":
    analyze_videos()