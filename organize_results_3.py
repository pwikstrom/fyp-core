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


def organize_results():
    from os.path import join, basename, exists
    import pandas as pd
    from copy import copy
    from datetime import datetime
    from os import walk, listdir
    import toml
    import json

    cf = toml.load(fyp.CONFIG_PATH)
    audio_transcription_path = join(cf["result_paths"]["main_data_dir"], cf["result_paths"]["audio_transcriptions_fn"])
    baseline_path = join(cf["result_paths"]["main_data_dir"], cf["result_paths"]["baseline_results_fn"])
    pyk_metadata_path = join(cf["result_paths"]["main_data_dir"], cf["result_paths"]["pyk_metadata_fn"])
    gemini_video_analysis_path = join(cf["result_paths"]["main_data_dir"], cf["result_paths"]["gemini_video_analysis_fn"])
    all_static_metadata_path = join(cf["result_paths"]["main_data_dir"], cf["result_paths"]["all_static_metadata_fn"])
    website_metadata_path = join(cf["result_paths"]["main_data_dir"], cf["result_paths"]["website_metadata_fn"])
    baseline_logs_dir = cf["input_paths"]["zeeschuimer_path"]
    ddp_dir = cf["input_paths"]["ddp_path"]


    # create necessary directories if they do not exist
    fyp.create_dirs()

    start_time = datetime.now()

    print("\n"+"*"*80)
    print(f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}: Organizing metadata and analysis results into dataframes for website and further analysis.")
    print("*"*80+"\n")

    print(f"(1/4) Cleaning up baseline logs (if logs are available).")

    #item_list = []
    if exists(baseline_logs_dir):
        print("   Baseline logs available. Loading...")
        baseline_df = pd.concat([pd.read_pickle(join(cf["input_paths"]["fine_logs_path"],fn)) for fn in listdir(cf["input_paths"]["fine_logs_path"]) if fn.endswith(".pkl")])
        print(fyp.get_baseline_info_as_string(baseline_df))

        relevant_columns = ['item_id','timestamp_collected','source_platform_url','id',
        'data.author.id','data.author.nickname','data.author.signature','data.author.uniqueId','data.author.verified',
        'data.authorStats.diggCount','data.authorStats.followerCount','data.authorStats.followingCount','data.authorStats.heart','data.authorStats.heartCount','data.authorStats.videoCount',
        'data.challenges','data.digged','data.diversificationId','data.duetDisplay','data.duetEnabled','data.id',
        'data.music.authorName','data.music.duration','data.music.id','data.music.original','data.music.title',
        'data.officalItem','data.originalItem',
        'data.poi.address','data.poi.category','data.poi.city','data.poi.cityCode','data.poi.countryCode','data.poi.fatherPoiName',
        'data.poi.id','data.poi.name','data.poi.ttTypeNameMedium','data.poi.ttTypeNameSuper','data.poi.ttTypeNameTiny',
        'data.stats.collectCount','data.stats.commentCount','data.stats.diggCount','data.stats.playCount','data.stats.shareCount',
        'data.statsV2.collectCount','data.statsV2.commentCount','data.statsV2.diggCount','data.statsV2.playCount','data.statsV2.repostCount','data.statsV2.shareCount',
        'data.stickersOnItem','data.anchors','data.effectStickers','data.author.roomId','data.warnInfo',
        'source_url.device_id','source_url.region','source_url.tz_name','source_url.categoryType','log_script','label']
        relevant_columns = [c for c in relevant_columns if c in baseline_df.columns]

        baseline_df = copy(baseline_df[relevant_columns])
        baseline_df.to_csv(baseline_path,index=False)

        print(f"   {len(baseline_df):,} items in the baseline logs.")
    else:
        print(f"   No baseline logs available.")



    print(f"(2/4) Loading audio transcriptions and results from Gemini video analysis - merging with the PykTok metadata.")
    pyk_metadata = pd.read_pickle(pyk_metadata_path)

    gemini_analysis = pd.read_pickle(gemini_video_analysis_path)
    gemini_analysis = gemini_analysis.sort_values("analysis_ts").drop_duplicates(subset=["item_id"],keep="last")
    
    gemini_analysis.drop(columns=["analysis_time","processing_time","analysis_ts"],inplace=True, errors="ignore")

    gemini_analysis.columns = ["g_"+c if c != "item_id" else c for c in gemini_analysis.columns]

    print(f"   {len(gemini_analysis)} videos have Gemini analysis.")

    all_static_metadata = pd.merge(left=pyk_metadata, right=gemini_analysis, on="item_id", how="inner")

    # add the audio transcriptions to the metadata
    audio_transcriptions = {}
    if exists(audio_transcription_path):
        with open(audio_transcription_path, "r") as f:
            audio_transcriptions = json.load(f)
    all_static_metadata["audio_transcript"] = all_static_metadata.item_id.map(lambda x: audio_transcriptions.get(str(x), ""))
    n_transcribed_items = [len(x)>0 for x in all_static_metadata["audio_transcript"]].count(True)
    print(f"   Adding {n_transcribed_items:,} audio transcriptions.")

    desc_wo_hashtags = []
    all_hashtags = []
    for u in all_static_metadata['desc']:
        if isinstance(u,str) and "#" in u:
            hashtags = []
            for hh in u.split(" "):
                if len(hh)>0 and hh[0]=="#":
                    xx = hh.split("#")
                    hashtags += [xxx for xxx in xx if len(xxx)>0]
            hashtags = sorted(hashtags, key=lambda x: len(x), reverse=True)
            flurpy = u
            for h in hashtags:
                flurpy = flurpy.replace("#"+h, "")
            desc_wo_hashtags += [flurpy.strip()]
            all_hashtags += ["#"+" #".join(hashtags)]
        else:
            desc_wo_hashtags += [u]
            all_hashtags+= [""]

    all_static_metadata['desc'] = desc_wo_hashtags
    all_static_metadata['hashtags'] = all_hashtags


    print(f"   Saving static metadata {len(all_static_metadata):,} videos as {all_static_metadata_path}")
    all_static_metadata.to_csv(all_static_metadata_path,index=False)


    print(f"(3/4) Generating a single CSV with all data donation packages.")

    ddp_activities = []
    if ddp_dir != "":
        print(f"   Loading all items from data donation packages...")
        for u, j, k in walk(ddp_dir):
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
            else:
                ddp_items.append(0)
        ddp_activities["item_id"] = ddp_items
        ddp_activities.item_id = ddp_activities.item_id.astype(int)
        all_static_metadata.item_id = all_static_metadata.item_id.astype(int)

        print(f"   {len(ddp_activities):,} items in the DDP logs.")

        ddp_expanded = pd.merge(left=ddp_activities,
                                right=all_static_metadata[["item_id","desc", "author_nickname","author_signature"]],
                                on="item_id",
                                how="left")
        check_ddp_cols = ["UserName","SearchTerm","Comment","desc","author_nickname","author_signature"]
        drop_words = input("   Enter words to drop from data donation packages separated by commas: ")
        drop_words = drop_words.replace("\n"," ")
        drop_words_list = list(map(lambda x:x.strip(), drop_words.split(",")))
        drop_words_list = [w for w in drop_words_list if w != ""]

        ok_for_donation = []
        for _,t in ddp_expanded[["item_id"]+check_ddp_cols].iterrows():
            w = " ".join([s.replace("\n"," ") for s in t[check_ddp_cols] if isinstance(s,str)]).lower().strip()
            ok_for_donation += [not any([q.lower() in w for q in drop_words_list])]

        print(f"   {sum(ok_for_donation):,} items are ok for donation.")
        ddp_activities = ddp_activities[ok_for_donation]

        print(f"   {len(ddp_activities):,} items in the DDP logs.")
        print(f"   Saving.")
        ddp_activities.to_csv(join(cf["result_paths"]["main_data_dir"], cf["result_paths"]["ddp_results_fn"]),index=False)
    


    





    print(f"(4/4) Generating a pretty CSV to be used for the video viewer on the FYP website.")

    website_metadata = copy(all_static_metadata)

    """
    target_cols = ['item_id', 'data.desc','g_video_story',
        'g_list_of_objects_in_video', 'g_text_visible_in_video',
        'audio_transcript', 'data.createTime',
        'data.author.nickname', 'data.author.signature',
        'data.authorStats.diggCount', 'data.authorStats.followerCount',
        'data.authorStats.followingCount', 'data.authorStats.heart',
        'data.authorStats.heartCount', 'data.authorStats.videoCount',
        'data.music.authorName', 'data.music.original', 'data.music.title',
        'data.stats.commentCount',
        'data.stats.diggCount', 'data.stats.playCount', 'data.stats.shareCount',
        'source_platform_url',
        'source_url.region', 'source_url.tz_name', 'source_url.categoryType',
        'video_duration', 'video_locationcreated', 'video_is_ad',
        'g_music_present', 'g_humans_talking',
        'g_main_activity', 'g_probable_location',
        'g_human_count', 'g_guessed_genders',
        'g_estimated_age_of_humans_observed', 'g_emotions',
        'g_seems_political', 'g_sensitive_topic',
        'g_symbols_or_brands_observed', 'g_flags_observed',
        'g_guessed_ethnicities_of_humans', g_communicative_function,
        'g_audio_language']
    
    target_cols = [c for c in target_cols if c in all_static_metadata.columns]

    website_metadata = copy(all_static_metadata[target_cols])
    """

    # fix the item_id column type
    website_metadata.item_id = website_metadata.item_id.astype(str)


    # extract hashtags from the description (remove the hashtags from the description as well)
    desc_wo_hashtags = []
    all_hashtags = []
    for u in website_metadata['desc']:
        if isinstance(u,str) and "#" in u:
            hashtags = []
            for hh in u.split(" "):
                if len(hh)>0 and hh[0]=="#":
                    xx = hh.split("#")
                    hashtags += [xxx for xxx in xx if len(xxx)>0]
            hashtags = sorted(hashtags, key=lambda x: len(x), reverse=True)
            flurpy = u
            for h in hashtags:
                flurpy = flurpy.replace("#"+h, "")
            desc_wo_hashtags += [flurpy.strip()]
            all_hashtags += ["#"+" #".join(hashtags)]
        else:
            desc_wo_hashtags += [u]
            all_hashtags+= [""]

    website_metadata['desc'] = desc_wo_hashtags
    website_metadata['hashtags'] = all_hashtags


    # fix various columns
    website_metadata.g_human_count = website_metadata.g_human_count.map(lambda x: str(x).replace("nan", "").replace("None", "").replace(" ", "").split(".")[0])
    website_metadata.g_estimated_age_of_humans_observed = website_metadata.g_estimated_age_of_humans_observed.map(lambda x: str(x).replace("nan", "").replace("None", "").replace(" ", "").split(".")[0])
    website_metadata["this_video"] = [f"{u:03}/{len(website_metadata):,}" for u in list(range(1,1+len(website_metadata)))]

    # format numbers (int)
    int_cols = website_metadata.select_dtypes(include="int").columns
    website_metadata[int_cols] = website_metadata[int_cols].map(lambda x:f"{x:,.0f}")

    # format numbers (float)
    float_cols = website_metadata.select_dtypes(include="float").columns
    website_metadata[float_cols] = website_metadata[float_cols].map(lambda x:f"{x:,.0f}")

    if "source_url.categoryType" in website_metadata.columns:
    # convert the cateogry type to a human readable format
        category_type = {119: "Singing & Dancing", 
        104: "Comedy", 
        112: "Sports", 
        100: "Anime & Comics", 
        107: "Relationship", 
        101: "Shows", 
        110: "Lipsync", 
        105: "Daily Life", 
        102: "Beauty Care", 
        103: "Games", 
        114: "Society", 
        109: "Outfit", 
        115: "Cars", 
        111: "Food", 
        113: "Animals", 
        106: "Family", 
        108: "Drama", 
        117: "Fitness & Health", 
        116: "Education", 
        118: "Technology",
        120: "?????"}

        website_metadata["source_url.categoryType"] = website_metadata["source_url.categoryType"].map(lambda x: f"{category_type.get(int(str(x).replace('nan','0')), 'Not on the Explore page')}")
    else:
        website_metadata["source_url.categoryType"] = cf["misc"]["label"]


    # rename columns so that they look a bit nicer
    new_cols = []
    for c in website_metadata.columns:
        new_cols += [c.replace("data.", "").replace("_v2", "").replace(".", "_")]
    website_metadata.columns = new_cols

    print(f"   Saving website metadata for {len(website_metadata):,} videos as {website_metadata_path}\n")
    website_metadata.to_csv(website_metadata_path,index=False)

    print("Done\n"+"*"*80+"\n")


if __name__ == "__main__":
    organize_results()