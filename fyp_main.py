# -*- coding: utf-8 -*-
"""
Script Name: 
Description: 
Author: Patrik
Date: 
"""

CONFIG_PATH = "config.toml"





############################################################################################################
###                     Utilities
############################################################################################################

def check_repetitive_patterns(text, min_pattern_length=5, min_repetitions=5, max_text_length=1000):
    from collections import defaultdict

    if not isinstance(text,str):
        return "Not a string"

    if len(text) > max_text_length:
        return "String too long"

    words = text.split()
    n = len(words)
    
    pattern_counts = defaultdict(int)
    
    # Check for all possible pattern lengths from min_pattern_length to half of the total number of words
    for length in range(min_pattern_length, n // 2 + 1):
        for i in range(n - length + 1):
            pattern = tuple(words[i:i + length])
            pattern_counts[pattern] += 1
    
    repetitive_patterns = []
    
    for pattern, count in pattern_counts.items():
        if count >= min_repetitions:
            repetitive_patterns.append((pattern, count))

    if repetitive_patterns:
        return "Found repetitive patterns"
    else:
        return "Good string"




def get_recent_files(directory, suffix=None, how_recent=10):
    from os import listdir
    from os.path import isfile, join, getmtime, getctime
    from datetime import datetime, timedelta

    current_time = datetime.now()
    recent_files = []

    for filename in listdir(directory):
        file_path = join(directory, filename)
        if isfile(file_path) and (suffix is None or file_path.endswith(suffix)):
            modified_time = datetime.fromtimestamp(getmtime(file_path))
            created_time = datetime.fromtimestamp(getctime(file_path))
            time_difference = current_time - max(modified_time, created_time)
            if time_difference < timedelta(minutes=how_recent):
                recent_files.append({"filename":file_path, "mtime":modified_time, "ctime":created_time})

    return sorted(recent_files,key=lambda x: x["mtime"], reverse=True)



def pretty_str_seconds(proc_time_seconds: float) -> str:
    minutes, seconds = divmod(proc_time_seconds, 60)
    out = ""
    if minutes > 0:
        out += f"{minutes:.0f}m"
    if seconds > 0:
        if minutes > 0:
            out += " and "
        out += f"{seconds:.0f}s"
    return out

def get_item_id_from_video_uri(video_uri):
    if video_uri[-1] == "/":
        video_uri = video_uri[:-1]
    return video_uri.split("/")[-1]





############################################################################################################
###                     Process Zeeschuimer metadata
############################################################################################################

# read a file with one json object per line and return a list of dictionaries
def read_ndjson_file(file_path, label="", zeeschuimer_path="") -> list:
    from json import loads
    from os.path import basename
    fine_fn = file_path.replace(zeeschuimer_path+"/","").replace("/","").replace(".ndjson","").split('-')[0]
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            line = '{"label":"' + label + '",' + line[1:]
            #line = '{"log_script":"' + basename(file_path).split('.')[0].split('-')[0] + '",' + line[1:]
            line = '{"log_script":"' + fine_fn + '",' + line[1:]
            #print(line)
            data.append(loads(line))
    return data



def clean_up_baseline_log(item_list_or_ndjson_path, label="", zeeschuimer_path=""):
    import pandas as pd
    from datetime import datetime

    if isinstance(item_list_or_ndjson_path, str):
        item_list = read_ndjson_file(item_list_or_ndjson_path, label=label, zeeschuimer_path=zeeschuimer_path)
    elif isinstance(item_list_or_ndjson_path, list):
        item_list = item_list_or_ndjson_path
    else:
        print("Input must be a list of dictionaries or a path to an ndjson file.")
        return pd.DataFrame()
        

    fix1 = {'data.contents': ['desc'],
    'data.video.bitrateInfo': [],
    'data.video.shareCover': [],
    'data.video.subtitleInfos': [],
    'data.challenges': ['desc'],
    'data.stickersOnItem': ['stickerText'],
    'data.textExtra': 'hashtagName',
    'data.ad_info.about_this_ad_info.about_this_ad_items': ["orientation_info"],
    'data.effectStickers': ['name'],
    'data.videoSuggestWordsList.video_suggest_words_struct': ['words'],
    'data.anchors': ['description','keyword'],
    'data.warnInfo': ['key'],
    'data.imagePost.cover.imageURL.urlList': [],
    'data.imagePost.images': [],
    'data.imagePost.shareCover.imageURL.urlList': []
    }

    def fixx(x, sub_keys):
        from numpy import nan as np_nan
        hh = []
        if isinstance(x, list) and len(sub_keys)>0:
            for y in x:
                if isinstance(y, dict):
                    gg = []
                    for sk in sub_keys:
                        if sk in y:
                            gg += [str(y[sk])]
                    gg = "__".join(gg)
                    hh += [gg]
            return " | ".join(hh)
        else:
            return np_nan


    def clean_url(the_url):
        from urllib.parse import unquote
        outout = {}
        for u in the_url.split("?")[1].split("&"):
            v = u.split("=")
            v[1] = unquote(v[1]).replace(",","|")
            try:
                v1 = int(v1)
            except:
                pass
            outout.update({"source_url."+v[0]:v[1]})
        return outout


    if len(item_list) == 0:
        return pd.DataFrame()

    u_raw_posts_df = pd.json_normalize(item_list)
    u_raw_posts_df.item_id = u_raw_posts_df.item_id.astype(int)

    for ff in fix1:
        if ff in u_raw_posts_df.columns:
            u_raw_posts_df[ff] = u_raw_posts_df[ff].apply(lambda x:fixx(x, fix1[ff]))
        
    for ff in fix1:
        if fix1[ff] == [] and ff in u_raw_posts_df.columns:
            del u_raw_posts_df[ff]
            
    some_cols = u_raw_posts_df.columns
    for c in some_cols:
        for drop_these in ["avatar", "secUid","data.contents","music.cover","music.playUrl","data.video"]:
            if drop_these in c:
                del u_raw_posts_df[c]
                
    source_details = []
    for ii in u_raw_posts_df.index:
        source_details += [clean_url(u_raw_posts_df['source_url'][ii])]
        
    source_details = pd.DataFrame(source_details)

    u_raw_posts_df = pd.merge(left=u_raw_posts_df, right=source_details, left_index=True, right_index=True)

    del u_raw_posts_df["source_url"]

    u_raw_posts_df["data.createTime"] = u_raw_posts_df["data.createTime"].apply(lambda x:datetime.fromtimestamp(x))
    u_raw_posts_df["timestamp_collected"] = u_raw_posts_df["timestamp_collected"].apply(lambda x: datetime.fromtimestamp(int(x/1000)))

    object_cols = [c for c in u_raw_posts_df.columns if u_raw_posts_df[c].dtype == 'object']

    u_raw_posts_df[object_cols] = u_raw_posts_df[object_cols].map(lambda x: x.replace(","," ") if type(x)==str else x)

    u_raw_posts_df[object_cols] = u_raw_posts_df[object_cols].map(lambda x: x.replace("\n"," ") if type(x) == str else x)

    return u_raw_posts_df



def get_baseline_info_as_string(the_raw_posts_df):
    n_items = len(the_raw_posts_df)
    the_string = ""
    the_string += "-"*40+ "\n"
    the_string += f"Baseline log info ({n_items:,} items):"+ "\n"
    the_string += "-"*40+ "\n"
    the_raw_posts_df = the_raw_posts_df.fillna("-")
    for i,c in enumerate([
        "timestamp_collected", "data.createTime", "source_platform_url", "source_url.browser_language", 
        "source_url.app_language", "source_url.cookie_enabled", 
        "source_url.language", "source_url.os", "source_url.region",
        "source_url.showAds", "source_url.tz_name", "source_url.user_is_login", "source_url.categoryType"
        ]):
        if c in the_raw_posts_df.columns:
            the_string += c.replace("source_url.","")
            if i<2:
                the_string += f": first: {min(the_raw_posts_df[c])}  |  last: {max(the_raw_posts_df[c])}"+ "\n"
            else:
                counted_values = the_raw_posts_df.value_counts(c)
                if len(counted_values) > 1:
                    the_string += f" ({len(counted_values)} unique values)\n   "
                    for j,cc in enumerate(counted_values.index):
                        if counted_values[cc]/n_items >= 0.01 and j<5:
                            if j>0:
                                the_string += "  |  "
                            the_string += f"{cc}: {counted_values[cc]/n_items:.0%}"
                    if len(counted_values) > 5:
                        the_string += "  |  ..."
                else:
                    the_string += f": {counted_values.index[0]}: 100%"
                the_string += "\n"
    the_string += "-"*40 + "\n"

    return the_string







############################################################################################################
###                     Process DDP logs and PykTok metadata
############################################################################################################

def get_ddp_activities(the_filename):
    from json import load
    from pandas import DataFrame
    from os.path import basename

    the_basename = basename(the_filename).split(".")[0]

    def prune_activity_dict(x,the_new):
        if "EffectLink" in x:
            x["Link"] = x["EffectLink"]
            del x["EffectLink"]
        things_we_dont_need = ["IP","DeviceModel","DeviceSystem","NetworkType","Carrier","Photo"]
        for k in things_we_dont_need:
            if k in x.keys():
                del x[k]
        x.update(the_new)
        return x

    with open(the_filename) as f:
        data_donation_package = load(f)

    long_list = []
    for k in data_donation_package["Activity"].keys():
        k2 = data_donation_package["Activity"][k]
        for kk in k2:
            if isinstance(data_donation_package["Activity"][k][kk],list):
                long_list += [prune_activity_dict(g,{"ActivityType":k}) for g in data_donation_package["Activity"][k][kk] if not k in ["Status"]]
    if isinstance(data_donation_package["Comment"]["Comments"]["CommentsList"],list):
        long_list += [prune_activity_dict(g,{"ActivityType":"Comment"}) for g in data_donation_package["Comment"]["Comments"]["CommentsList"]]
    
    ddp_activities = DataFrame(long_list).sort_values("Date")
    ddp_activities["label"] = the_basename

    return ddp_activities




def get_tiktok_data(tiktok_url):
    from mypyktok import specify_browser, get_tiktok_raw_data, generate_data_row
    from json import loads
    from copy import copy
    from time import sleep

    specify_browser('chrome')

    try:
        my_soup = get_tiktok_raw_data(tiktok_url,"chrome")
    except:
        print("Failed to get soup. Sleeping 4 seconds.")
        sleep(4)
        return [None, "no_soup", None]

    try:
        stuff = loads(my_soup.find('script', id="__UNIVERSAL_DATA_FOR_REHYDRATION__").text)
    except:
        print("Failed to get JSON from soup. Sleeping 4 seconds.")
        sleep(4)
        return [None, "no_json_in_soup", None]

    the_returned_url = None
    if isinstance(stuff,dict) and '__DEFAULT_SCOPE__' in stuff.keys():
        try:
            the_returned_url = stuff['__DEFAULT_SCOPE__']['seo.abtest']['canonical']
            page_type = 'unknown'
        except:
            page_type = 'seo_data_missing'
        if page_type == 'unknown':
            if 'webapp.video-detail' in stuff['__DEFAULT_SCOPE__']:
                if "itemInfo" in stuff['__DEFAULT_SCOPE__']['webapp.video-detail'] and \
                    "itemStruct" in stuff['__DEFAULT_SCOPE__']['webapp.video-detail']["itemInfo"]:
                    page_type = 'video'
                else:
                    page_type = 'video_error'
            else:
                page_type = 'other'
    else:
        page_type = 'no_default_scope_key'

    heya = None
    if page_type == 'video':
        try:
            heya = copy(generate_data_row(stuff["__DEFAULT_SCOPE__"]['webapp.video-detail']['itemInfo']['itemStruct']))
            #quick_download_video_cover(heya.iloc[0]["video_cover"])
        except:
            page_type = "failed_generate_data_row"
    return {"url":the_returned_url, "page_type":page_type, "video_metadata":heya}



def download_video_cover(url, the_path, the_item_id, target_height=500):
    from requests import get
    import toml
    from os.path import join, exists
    from PIL import Image

    cf = toml.load(CONFIG_PATH)
    save_path = join(the_path, f"{the_item_id}.jpg")
    if not exists(save_path):
        response = get(url)
        if response.status_code == 200:
            with open(save_path, 'wb') as file:
                file.write(response.content)

            # Resize the image
            image = Image.open(save_path)
            aspect_ratio = image.width / image.height
            resized_image = image.resize((int(target_height*aspect_ratio), target_height))
            #resized_image.save(save_path.replace(".jpg","-smaller.jpg"), optimize=False, quality=75)
            resized_image.save(save_path, optimize=False, quality=75) # overwrite the original file
        


def prune_pyktok_json(an_item):
    from copy import copy
    from pandas import DataFrame

    item_struct = copy(an_item['__DEFAULT_SCOPE__']['webapp.video-detail']['itemInfo']['itemStruct'])

    item_struct["item_id"] = item_struct["id"]

    for k in ["duration","cover"]:
        if k in item_struct["video"].keys():
            item_struct["video_"+k] = item_struct["video"][k]

    for k in ["id","uniqueId","nickname","signature","verified"]:
        if k in item_struct["author"].keys():
            item_struct["author_"+k] = item_struct["author"][k]

    if "music" in item_struct.keys():
        for k in ["id","title","authorName","original","duration","album"]:
            if k in item_struct["music"].keys():
                item_struct["music_"+k] = item_struct["music"][k]

    if "challenges" in item_struct.keys():
        item_struct["challenges"] = "|".join([f"[{g['id']}]{g['title']}" for g in item_struct["challenges"]])
    
    if "anchors" in item_struct.keys():
        item_struct["anchors"] = "|".join([f"[{g['id']}]{g['description']}" for g in item_struct["anchors"]])

    if "poi" in item_struct.keys():
        for k in ["name","address","city","province","country"]:
            if k in item_struct["poi"].keys():
                item_struct["poi_"+k] = item_struct["poi"][k]


    for k in ["diggCount","shareCount","commentCount","playCount","collectCount"]:
        if k in item_struct["stats"].keys():
            item_struct["stats_"+k] = item_struct["stats"][k]

    for k in ['id','scheduleTime', 'video', 'author', 'music', 'stats', 'statsV2', 
            'warnInfo', 'originalItem', 'officalItem', 'textExtra', 'secret', 'forFriend', 'digged', 'itemCommentStatus', 
            'takeDown', 'effectStickers', 'privateItem', 'duetEnabled', 'stitchEnabled', 'stickersOnItem', 'shareEnabled', 
            'comments', 'duetDisplay', 'stitchDisplay', 'indexEnabled', 'diversificationLabels', 'locationCreated', 'suggestedWords', 
            'contents', 'diversificationId', 'collected', 'videoSuggestWordsList', 'channelTags', 
            'item_control', 'keywordTags','poi','contentLocation']:
        if k in item_struct.keys():
            del item_struct[k]

    return DataFrame.from_dict(item_struct,orient="index").T




############################################################################################################
###                     GCP buckets
############################################################################################################

def get_gcp_bucket(bucket_name, verbose = False):
    from google.api_core.exceptions import Forbidden
    from google.cloud import storage

    try:
        # Initialize a client
        client = storage.Client()

        # Get the bucket
        bucket = client.get_bucket(bucket_name)

        # Try to access the bucket's metadata
        bucket.reload()
        if verbose:
            print(f"Access to the bucket '{bucket_name}' is authorized.")
        return bucket
    except Forbidden:
        if verbose:
            print(f"You don't have access to the bucket '{bucket_name}'.")
        return None
    except Exception as e:
        if verbose:
            print(f"An error occurred: {e}")
        return None



def list_files_in_bucket(bucket, prefix="", include_sub_prefixes=True, suffix=""):
    if suffix != "" and not suffix.startswith("."):
        suffix = "."+suffix
    blobs = bucket.list_blobs(prefix=prefix)
    files_in_bucket = [blob.name for blob in blobs]
    files_in_bucket = [fn.replace(prefix,"") for fn in files_in_bucket if fn.endswith(suffix)]
    files_in_bucket = [fn[1:] if fn[0]=="/" else fn for fn in files_in_bucket]
    if not include_sub_prefixes:
        files_in_bucket = [fn for fn in files_in_bucket if "/" not in fn]
    return files_in_bucket



def upload_blob(bucket, filename, source_dir="", prefix=""):
    from os.path import join
    blob = bucket.blob(join(prefix,filename))
    blob.upload_from_filename(join(source_dir,filename))



def download_blob(bucket, filename, prefix="", dest_dir=""):
    from os.path import join
    blob = bucket.blob(join(prefix,filename))
    blob.download_to_filename(join(dest_dir,filename))







############################################################################################################
###                     File, directory and config mgmt
############################################################################################################


def temp_path(filename="") -> str:
    import toml
    from os.path import join

    cf = toml.load(CONFIG_PATH)
    temp_dir = join(cf["result_paths"]["main_data_dir"], "temp")
    return join(temp_dir, filename)




def create_dirs():
    from os import makedirs
    from os.path import exists, join
    import toml
    import sys

    cf = toml.load(CONFIG_PATH)
    data_dir = cf["result_paths"]["main_data_dir"]
    backup_dir = join(cf["result_paths"]["main_data_dir"], "backup")
    #video_cover_dir = join(cf["result_paths"]["main_data_dir"], cf["result_paths"]["video_cover_dir"])
    temp_dir = join(cf["result_paths"]["main_data_dir"], "temp")

    if not (exists(cf["input_paths"]["zeeschuimer_path"])):# or exists(cf["input_paths"]["ddp_path"])):
        print(f"Directory with baseline logs not found. Exiting.")
        sys.exit(1)

    if not exists(data_dir):
        print(f"Creating directory {data_dir}")
        makedirs(data_dir)

    if not exists(temp_dir):
        print(f"Creating directory {temp_dir}")
        makedirs(temp_dir)

    if not exists(backup_dir):
        print(f"Creating directory {backup_dir}")
        makedirs(backup_dir)




def back_this_up(the_file, move_the_file=False) -> None:
    from os.path import join, exists, basename
    from datetime import datetime
    from shutil import copy, move
    import toml
    cf = toml.load(CONFIG_PATH)
    backup_dir = join(cf["result_paths"]["main_data_dir"], "backup")

    if exists(the_file) == False:
        #print(f"File '{the_file}' not found")
        return

    nice_now = datetime.now().strftime("%Y%m%d_%H%M%S")

    backup_file = join(backup_dir,"backup_"+nice_now+"_"+basename(the_file))

    if move_the_file:
        print(f"Backing up (moving) {basename(the_file)}")
        move(the_file, backup_file)
    else:
        print(f"Backing up (copying) {basename(the_file)}")
        copy(the_file, backup_file)



"""
The input to this function is a list of tuples, where each tuple contains a section and a key.
Example: update_config([("section1", "key1"), ("section2", "key2")])
"""
def update_config(data_list: list) -> None:
    import toml

    config_filename = CONFIG_PATH


    cf = toml.load(config_filename)

    print("\n"+"*"*80)
    print(f"Checking the config file and allowing you to update some values.")
    print(f"Leave the input blank to keep the current value. Just press ENTER/RETURN.")
    print(f"Enter '---' (three hyphens) to change a value to an empty string.")
    print(f"\nIf you want to change other settings in the config file, please edit 'config.toml' directly.")
    print(f"You can change the Gemini prompts in 'prompts.toml'.")
    print("*"*80+"\n")

    for section in cf:
        for key in cf[section]:
            if cf[section][key] == '' and (section,key) not in data_list:
                data_list.append((section, key))
  
    changed = False
    for (section, key) in data_list:
        new_value = input(f"'{section}':'{key}' is '{cf[section][key]}'. Change to: ")
        if new_value and new_value != cf[section][key]:
            if new_value == "---":
                new_value = ""
            try:
                # Attempt to convert to an appropriate type (int, float, etc.)
                if not isinstance(cf[section][key],str):
                    new_value = eval(new_value)
            except:
                pass  # Keep it as a string if conversion fails
            cf[section][key] = new_value
            print(f"   Value changed to '{new_value}'")
            changed = True
        else:
            print("   Value unchanged.")

    if changed:
        print("\nUpdating config file with new values.")
        with open(config_filename, "w") as f:
            toml.dump(cf, f)
    else:
        print("\nNo changes made to the config file.")
    print("*"*80+"\n")








############################################################################################################
###                    Gemini
############################################################################################################



def rescue_temp_gemini_results():
    from os import remove, listdir
    from os.path import join, basename
    import toml
    from json import load
    from datetime import datetime
    from pandas import read_pickle, DataFrame, concat
    cf = toml.load(CONFIG_PATH)

    gemini_video_analysis_path = join(cf["result_paths"]["main_data_dir"],cf["result_paths"]["gemini_video_analysis_fn"])

    json_saves = [g for g in listdir(temp_path()) if g.startswith("temp_gemini_results") and g.endswith(".json")]
    print(f"Found {len(json_saves)} json files in the temp directory.")
    if len(json_saves) == 0:
        return

    json_files = []
    # Iterate over each file in the directory
    for filename in json_saves:
        file_path = temp_path(filename)
        with open(file_path, 'r') as file:
            json_data = load(file)
            json_data["analysis_ts"] = filename
            json_files.append(json_data)


    rescued_gemini_results = DataFrame(json_files)
    rescued_gemini_results = rescued_gemini_results.dropna()
    rescued_gemini_results["item_id"] = rescued_gemini_results["filename"].apply(lambda x: int(basename(x).split(".")[0]))
    rescued_gemini_results.drop("filename", axis=1, inplace=True)
    rescued_gemini_results["analysis_time"] = 0.0
    rescued_gemini_results["processing_time"] = 0.0
    rescued_gemini_results["analysis_ts"] = rescued_gemini_results["analysis_ts"].map(lambda x:datetime.strptime(x.split(".")[0].split("_")[-1], "%Y%m%d%H%M%S%f"))

    current_gemini_results = read_pickle(gemini_video_analysis_path)

    rescued_gemini_results = rescued_gemini_results[~rescued_gemini_results.item_id.isin(current_gemini_results.item_id)]

    updated_gemini_results = concat([current_gemini_results, rescued_gemini_results])
    n_na_values = (updated_gemini_results.isna()*1).sum().sum()

    #print(updated_gemini_results.info())
    print(f"Number of missing values: {n_na_values}")
    print(f"shapes: Current:{current_gemini_results.shape} and Rescued:{rescued_gemini_results.shape} and Updated:{updated_gemini_results.shape}")

    hey_there = input("Y to continue saving the rescued data, or N to exit")

    if hey_there.lower() != "y":
        print("Exiting without saving the rescued data.")
        return

    # backup the existing analysis data and move it to the backup folder
    back_this_up(gemini_video_analysis_path, move_the_file=True)

    print(f"Saving the old & rescued analysis data to {gemini_video_analysis_path}")
    updated_gemini_results.to_pickle(gemini_video_analysis_path)

    hey_there = input("Y to continue deleting all the rescued json files in the temp directory, or N to exit")

    if hey_there.lower() != "y":
        print("Exiting without saving the rescued data.")
        return

    for filename in json_saves:
        file_path = temp_path(filename)
        remove(file_path)
        print(".", end="", flush=True)
    print(" done")





def upload_to_gemini(path, mime_type=None, verbose=False):
    from google.generativeai import upload_file, configure
    from os.path import exists

    import toml
    cf = toml.load(CONFIG_PATH)
    import sys
    try:
        configure(api_key=cf["gemini"]["key"])
    except:
        print("Error Gemini API key. Exiting.")
        sys.exit(0)


    if not exists(path):
        raise FileNotFoundError(f"File '{path}' not found")

    file = upload_file(path, mime_type=mime_type)
    if verbose:
        print(f"File uploaded to Gemini.")
    return file


def wait_for_files_active(files, verbose = False):
    from time import sleep
    from google.generativeai import get_file, configure

    import toml
    cf = toml.load(CONFIG_PATH)
    import sys
    try:
        configure(api_key=cf["gemini"]["key"])
    except:
        print("Error Gemini API key. Exiting.")
        sys.exit(0)

    if verbose:
        print("Waiting for file to be prepared...")
    for name in (file.name for file in files):
        file = get_file(name)
        while file.state.name == "PROCESSING":
            if verbose:
                print(".", end="", flush=True)
            sleep(2)
            file = get_file(name)
        if file.state.name != "ACTIVE":
            return False
    if verbose:
        print("...all files ready")
    return True




def analyze_single_video(this_file, timeout=200, verbose=False):
    from json import loads
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
    from google.generativeai import GenerativeModel, configure

    import toml
    cf = toml.load(CONFIG_PATH)
    import sys
    try:
        configure(api_key=cf["gemini"]["key"])
    except:
        print("Error Gemini API key. Exiting.")
        sys.exit(0)


    with open('gemini_prompt.txt', 'r') as file:
        gemini_prompt = file.read()
    #prompts = toml.load("prompts.toml")

    # Create the model
    generation_config = {
        "temperature": cf["gemini"]["temperature"],
        "top_p": cf["gemini"]["top_p"],
        "top_k": cf["gemini"]["top_k"],
        "max_output_tokens": cf["gemini"]["max_output_tokens"],
        "response_mime_type": "application/json",
    }

    model = GenerativeModel(
        model_name=cf["gemini"]["model"],
        generation_config=generation_config,
        safety_settings={
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE}
    )

    had_enough = False
    little_counter = 0
    while not had_enough:
        little_counter += 1
        try:
            response = model.generate_content([this_file, gemini_prompt],#prompts["gemini"]["prompt_step_1"]],
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

        if not good_strings and little_counter < 2:
            had_enough = False
            if verbose:
                print("1st analysis did not go well, trying again.")
        elif not good_strings:
            had_enough = True
            if verbose:
                print("2nd analysis failed, not trying again.")
        else:
            had_enough = True
            if verbose:
                print("Gemini output looks ok.")

    return raw_json



def generate_content(my_prompt, verbose=False):
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
    from google.generativeai import GenerativeModel, configure
    import toml
    cf = toml.load(CONFIG_PATH)

    import sys
    try:
        configure(api_key=cf["gemini"]["key"])
    except:
        print("Error Gemini API key. Exiting.")
        sys.exit(0)

    # Create the model
    generation_config = {
        "temperature": cf["gemini"]["temperature"],
        "top_p": cf["gemini"]["top_p"],
        "top_k": cf["gemini"]["top_k"],
        "max_output_tokens": cf["gemini"]["max_output_tokens"],
        "response_mime_type": "text/plain",
    }

    model = GenerativeModel(
        model_name=cf["gemini"]["model"],
        generation_config=generation_config,
        safety_settings={
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE}
    )

    response = model.generate_content(my_prompt)
    
    try:
        fine_text = response.candidates[0].content.parts[0].text
    except:
        fine_text = ""

    return fine_text

    
"""def improve_single_gemini_analysis_p(one_analysis_results_w_prompt):
    improvement_prompt = one_analysis_results_w_prompt[0]
    one_analysis_results = one_analysis_results_w_prompt[1]
    if isinstance(one_analysis_results,str):
        one_analysis_results = one_analysis_results.replace("\n"," ")
        a_prompt = f"{improvement_prompt} {one_analysis_results}"
        improved_analysis = generate_content(a_prompt).strip().replace("\n"," ")
        print(".",end="",flush=True)
    else:
        print("#",end="",flush=True)
        improved_analysis = one_analysis_results
    return improved_analysis"""







def gemini_video_process(the_video_file_w_number, verbose=False):
    from datetime import datetime
    from google.generativeai import delete_file
    from os import remove
    from os.path import join
    from json import dump

    import toml
    cf = toml.load(CONFIG_PATH)

    tutti_start_time = datetime.now()
    the_video_filename = the_video_file_w_number[1]
    #print(f"{the_video_file_w_number[0]:04} {the_video_filename.split('/')[-1]}")
    timeout = cf["gemini"]["timeout"]

    if verbose:
        print("Connecting to GCP bucket...")
    main_bucket = get_gcp_bucket(cf["video_storage"]["GCP_bucket"])
    if main_bucket is None:
        print("Could not connect to GCP bucket. Exiting.")
        return

    if verbose:
        print("Downloading video object...")
    download_blob(main_bucket, 
                  the_video_filename,
                  prefix=cf["video_storage"]["prefix"], 
                  dest_dir=temp_path())
        
    if verbose:
        print("Uploading video to Gemini...")
    files_for_gemini = [
        upload_to_gemini(temp_path(the_video_filename), mime_type="video/mp4", verbose=verbose),
    ]
    file_is_ready_for_analysis = wait_for_files_active(files_for_gemini, verbose=verbose)

    analysis_start_time = datetime.now()

    if file_is_ready_for_analysis:        
        if verbose:
            print(f"Analyzing video...")
        video_analysis_results = analyze_single_video(files_for_gemini[0], timeout=timeout, verbose=verbose)
    else:
        if verbose:
            print(f"File prep for Gemini analysis failed: {the_video_filename}")
        video_analysis_results = {}

    analysis_time = (datetime.now() - analysis_start_time).total_seconds()

    if verbose:
        print("Deleting files...")
    #delete_file(files_for_gemini[0].name)
    remove(temp_path(the_video_filename))
    
    tutti_time = (datetime.now() - tutti_start_time).total_seconds()
    print(f"{the_video_file_w_number[0]:04} {the_video_filename.split('.')[0]} done. Gemini analysis: {analysis_time:.1f}s. Total time: {tutti_time:.1f}s")

    video_analysis_results["filename"] = the_video_filename
    
    # Save the result at this stage for this single video as a precaution if it all blows up
    temp_fn = join(temp_path(f"temp_gemini_results_{the_video_file_w_number[0]:04}_{"".join([ch for ch in str(datetime.now()) if ch in '1234567890'])}.json"))
    with open(temp_fn, 'w') as file:
        dump(video_analysis_results,file)

    video_analysis_results["analysis_time"] = analysis_time
    video_analysis_results["processing_time"] = tutti_time
    video_analysis_results["analysis_ts"] = datetime.now()
    return video_analysis_results








if __name__ == "__main__":
    print("Module is being run directly.")

