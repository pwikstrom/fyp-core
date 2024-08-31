# -*- coding: utf-8 -*-
"""
Script Name: 
Description: 
Author: Patrik
Date: 
"""

CONFIG_PATH = "config.toml"


############################################################################################################
###                     Initialize project + File, directory and config mgmt
############################################################################################################


def temp_path(filename: str = "") -> str:
    import toml
    from os.path import join

    cf = toml.load(CONFIG_PATH)
    temp_dir = join(cf["paths"]["main"], "temp")
    return join(temp_dir, filename)




def create_dirs(cf: dict, clear_temp_dir: bool = False) -> None:
    from os import makedirs
    from os.path import join
    from os import listdir, remove


    for k in ["main", "zeeschuimer_raw", "zeeschuimer_refined", "temp", "backup"]:
        makedirs(cf["paths"][k], exist_ok=True)

    if cf["media_storage"]["storage_type"]!="GCP":
        makedirs(join(cf["media_storage"]["local_storage_dir"],cf["media_storage"]["video_prefix"]), exist_ok=True)
        makedirs(join(cf["media_storage"]["local_storage_dir"],cf["media_storage"]["audio_prefix"]), exist_ok=True)
        makedirs(join(cf["media_storage"]["local_storage_dir"],cf["media_storage"]["video_cover_prefix"]), exist_ok=True)
    
    if clear_temp_dir:
        for fn in listdir(temp_path()):
            remove(join(temp_path(),fn))






def back_this_up(the_file: str, move_the_file: bool = False) -> None:
    from os.path import join, exists, basename
    from datetime import datetime
    from shutil import copy, move

    cf = init_config()

    if exists(the_file) == False:
        return

    nice_now = datetime.now().strftime("%Y%m%d_%H%M%S")

    backup_file = join(cf["paths"]["backup"],"backup_"+nice_now+"_"+basename(the_file))

    if move_the_file:
        print(f"Backing up (moving) {basename(the_file)}")
        move(the_file, backup_file)
    else:
        print(f"Backing up (copying) {basename(the_file)}")
        copy(the_file, backup_file)



def update_config(data_list: list) -> None:
    """
    The input to this function is a list of tuples, where each tuple contains a section and a key.
    Example: update_config([("section1", "key1"), ("section2", "key2")])
    """
    import toml

    config_filename = CONFIG_PATH


    cf = toml.load(config_filename)

    print("\n"+"*"*80)
    print(f"Checking the config file and allowing you to update some values.")
    print(f"Leave the input blank to keep the current value. Just press ENTER/RETURN.")
    print(f"Enter '---' (three hyphens) to change a value to an empty string.")
    print(f"\nIf you want to change other settings in the config file, please edit 'config.toml' directly.")
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



def init_config() -> dict:
    from os.path import join
    import toml
    cf = toml.load(CONFIG_PATH)

    cf["paths"]["baseline"] = join(cf["paths"]["main"], cf["fn"]["baseline_results_fn"])
    cf["paths"]["pyk_metadata"] = join(cf["paths"]["main"], cf["fn"]["pyk_metadata_fn"])
    cf["paths"]["data_donations"] = join(cf["paths"]["main"], cf["fn"]["ddp_results_fn"])
    cf["paths"]["all_static_metadata"] = join(cf["paths"]["main"], cf["fn"]["all_static_metadata_fn"])
    cf["paths"]["gemini_video_analysis"] = join(cf["paths"]["main"],cf["fn"]["gemini_video_analysis_fn"])
    cf["paths"]["failed_downloads"] = join(cf["paths"]["main"], cf["fn"]["pyk_failed_items_fn"])
    cf["paths"]["audio_transcription"] = join(cf["paths"]["main"], cf["fn"]["audio_transcriptions_fn"])
    cf["paths"]["website_metadata"] = join(cf["paths"]["main"], cf["fn"]["website_metadata_fn"])
    cf["paths"]["temp"] = join(cf["paths"]["main"], "temp")
    cf["paths"]["backup"] = join(cf["paths"]["main"], "backup")

    return cf


def init_project(clear_temp_dir=False) -> dict:

    from os import getcwd
    from os.path import join, exists
    from sys import path as sys_path


    here = getcwd().split("/")
    while not exists(join("/".join(here),"__proj__.py")):
        here.pop()

    # this is the root folder for the project structure
    abs_project_root_path = join("/".join(here))
    #print("Project root:",abs_project_root_path)

    # add project root path to PATH since the modules are located in the project structure
    sys_path.append(abs_project_root_path)

    cf = init_config()
    cf["paths"]["project_root"] = abs_project_root_path

    create_dirs(cf, clear_temp_dir)

    return cf
        




############################################################################################################
###                     Utilities
############################################################################################################

def check_repetitive_patterns(text: str, min_pattern_length: int = 5, min_repetitions: int = 5, max_text_length: int = 1000) -> str:
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





def extract_and_join_subkeys(data, sub_keys: list):
    """
    Process a list of dictionaries or a single value, extracting and joining specified sub-keys.

    Args:
    data (list or any): The input data to process. If it's a list, each item is expected to be a dictionary.
    sub_keys (list): A list of keys to extract from each dictionary in the list.

    Returns:
    str or numpy.nan: A string of concatenated values from the specified sub-keys, 
                      or numpy.nan if the input is not a list or is empty.

    Description:
    This function extracts and concatenates values from specific keys in a list of dictionaries.
    If the input is not a list or is empty, it returns numpy.nan.
    For each dictionary in the list, it extracts the values of the specified sub-keys,
    joins them with "__", and then joins all these combined values with " | ".

    Example:
    >>> data = [
    ...     {"id": 1, "name": "John", "age": 30},
    ...     {"id": 2, "name": "Jane", "age": 25},
    ...     {"id": 3, "name": "Bob", "age": 35}
    ... ]
    >>> sub_keys = ["name", "age"]
    >>> result = extract_and_join_subkeys(data, sub_keys)
    >>> print(result)
    'John__30 | Jane__25 | Bob__35'
    """
    from numpy import nan as np_nan
    joined_values = []
    if isinstance(data, list) and len(sub_keys) > 0:
        for item in data:
            if isinstance(item, dict):
                subkey_values = []
                for sk in sub_keys:
                    if sk in item:
                        subkey_values.append(str(item[sk]))
                joined_values.append("__".join(subkey_values))
        return " | ".join(joined_values)
    else:
        return np_nan






def clean_url(the_url: str) -> dict:
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




############################################################################################################
###                     Process Zeeschuimer metadata
############################################################################################################

# read a file with one json object per line and return a list of dictionaries
def read_ndjson_file(file_path, cf=None) -> list:
    from json import loads

    # if the config is not provided, create a bare minimum default config
    if cf is None:
        cf = {"paths":{"zeeschuimer_raw":""}, "misc":{"label":""}}

    fine_fn = file_path.replace(cf["paths"]["zeeschuimer_raw"]+"/","").replace("/","").replace(".ndjson","").split('-')[0]
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            line = '{"label":"' + cf["misc"]["label"] + '",' + line[1:]
            line = '{"log_script":"' + fine_fn + '",' + line[1:]
            data.append(loads(line))
    return data



def refine_zeeschuimer_log(item_list_or_ndjson_path: str | list[dict], cf=None):
    import pandas as pd
    from datetime import datetime

    if isinstance(item_list_or_ndjson_path, str):
        item_list = read_ndjson_file(item_list_or_ndjson_path, cf)
    elif isinstance(item_list_or_ndjson_path, list):
        item_list = item_list_or_ndjson_path
    else:
        print("Input must be a list of dictionaries or a path to an ndjson file.")
        return pd.DataFrame()
        
    # if the list is empty, return an empty dataframe
    if len(item_list) == 0:
        return pd.DataFrame()

    # normalize the list of dictionaries into a dataframe and convert the item_id to an integer
    zeeschuimer_logs_df = pd.json_normalize(item_list)
    zeeschuimer_logs_df.item_id = zeeschuimer_logs_df.item_id.astype(int)

    # drop these columns
    zeeschuimer_logs_df.drop(columns=["avatar", "secUid", "data.contents", "music.cover", "music.playUrl", "data.video"], errors="ignore", inplace=True)


    # the dataframe based zeeschuimer data has a lot of variables that we don't
    # need or need to simplify. This dict is used to iterate over the columns in the DF
    # and indicate that the data should be simplified into a string (or dropped). 
    # The utility function extract_and_join_subkeys
    # is used to extract the subkeys and join them into a single string.
    columns_to_fix = {'data.contents': ['desc'],
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

    # iterate over the columns_in columns_to_fix
    for a_column_to_fix in columns_to_fix:
        # if the column is in the DF, apply the extract_and_join_subkeys function
        if a_column_to_fix in zeeschuimer_logs_df.columns:
            zeeschuimer_logs_df[a_column_to_fix] = zeeschuimer_logs_df[a_column_to_fix].apply(lambda x:extract_and_join_subkeys(x, columns_to_fix[a_column_to_fix]))
        
    # iterate over the columns_in columns_to_fix (again)
    for ff in columns_to_fix:
        # if the column is in the DF and the list of subkeys in 'columns_to_fix' is empty, drop the column
        if columns_to_fix[ff] == [] and ff in zeeschuimer_logs_df.columns:
            del zeeschuimer_logs_df[ff]
            
    # the column 'source_url' is a string that contains the url and lots of useful metadata
    # that we can extract into separate columns. This is done with the function clean_url
    # and the result is a dataframe with the source_url metadata as separate columns.
    source_details = []
    for ii in zeeschuimer_logs_df.index:
        source_details += [clean_url(zeeschuimer_logs_df['source_url'][ii])]        
    source_details = pd.DataFrame(source_details)

    # merge the source_details dataframe with the zeeschuimer_logs_df dataframe and drop the source_url column
    zeeschuimer_logs_df = pd.merge(left=zeeschuimer_logs_df, right=source_details, left_index=True, right_index=True)
    del zeeschuimer_logs_df["source_url"]

    # convert the 'data.createTime' and 'timestamp_collected' columns to datetime
    zeeschuimer_logs_df["data.createTime"] = zeeschuimer_logs_df["data.createTime"].apply(lambda x:datetime.fromtimestamp(x))
    zeeschuimer_logs_df["timestamp_collected"] = zeeschuimer_logs_df["timestamp_collected"].apply(lambda x: datetime.fromtimestamp(int(x/1000)))

    # replace commas and newlines in object columns with spaces
    object_cols = [c for c in zeeschuimer_logs_df.columns if zeeschuimer_logs_df[c].dtype == 'object']
    zeeschuimer_logs_df[object_cols] = zeeschuimer_logs_df[object_cols].map(lambda x: x.replace(","," ") if type(x)==str else x)
    zeeschuimer_logs_df[object_cols] = zeeschuimer_logs_df[object_cols].map(lambda x: x.replace("\n"," ") if type(x) == str else x)

    return zeeschuimer_logs_df



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
    from fyp.mypyktok import specify_browser, get_tiktok_raw_data, generate_data_row
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
    from os.path import join, exists
    from PIL import Image

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
###                     manage media storage / GCP bucket
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



def init_media_storage(verbose=False):
    from os.path import join

    cf = init_config()

    if cf["media_storage"]["storage_type"]=="GCP":
        if verbose:
            print("Connecting to GCP bucket...")
        main_media_storage = get_gcp_bucket(cf["media_storage"]["GCP_bucket"])
        if main_media_storage is None:
            print("Could not connect to GCP bucket. Exiting.")
            return None
    else:
        if verbose:
            print("Using local storage.")
        main_media_storage = cf["media_storage"]["local_storage_dir"]
    return main_media_storage





def list_files_in_storage(storage_location, prefix="", include_sub_prefixes=True, suffix=""):
    from os.path import join
    from os import listdir

    if isinstance(storage_location,str): # if it's a string, it's a local directory
        files_in_storage = [fn for fn in listdir(join(storage_location,prefix)) if fn.endswith(suffix)]
    else:
        if suffix != "" and not suffix.startswith("."):
            suffix = "."+suffix
        blobs = storage_location.list_blobs(prefix=prefix)
        files_in_storage = [blob.name for blob in blobs]
        files_in_storage = [fn.replace(prefix,"") for fn in files_in_storage if fn.endswith(suffix)]
        files_in_storage = [fn[1:] if fn[0]=="/" else fn for fn in files_in_storage]
        if not include_sub_prefixes:
            files_in_storage = [fn for fn in files_in_storage if "/" not in fn]
    
    return files_in_storage



def save_blob_to_storage(storage_location, filename, source_dir="", prefix=""):
    from os.path import join, exists
    from shutil import copyfile
    if isinstance(storage_location,str): # if it's a string, it's a local directory
        if exists(join(source_dir,filename)):
            copyfile(join(source_dir,filename), join(storage_location,prefix,filename))
        else:
            print(f"File '{filename}' not found in '{source_dir}'")
    else:
        blob = storage_location.blob(join(prefix,filename))
        blob.upload_from_filename(join(source_dir,filename))


def load_blob_from_storage(storage_location, filename, prefix="", dest_dir=""):
    from os.path import join, exists
    from shutil import copyfile
    if isinstance(storage_location,str): # if it's a string, it's a local directory
        if exists(join(storage_location,prefix,filename)):
            copyfile(join(storage_location,prefix,filename), join(dest_dir,filename))
        else:
            print(f"File '{filename}' not found in '{join(storage_location,prefix)}'")
    else:
        blob = storage_location.blob(join(prefix,filename))
        blob.download_to_filename(join(dest_dir,filename))






############################################################################################################
###                    Gemini
############################################################################################################


def rescue_temp_gemini_results(verbose=False):
    from os import remove, listdir
    from os.path import join, basename
    from json import load
    from datetime import datetime
    from pandas import read_pickle, DataFrame, concat

    cf = init_config()

    json_saves = [g for g in listdir(temp_path()) if g.startswith("temp_gemini_results") and g.endswith(".json")]

    if verbose:
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

    rescued_gemini_results["analysis_time"] = 0.0
    rescued_gemini_results["processing_time"] = 0.0
    rescued_gemini_results["analysis_ts"] = rescued_gemini_results.analysis_ts.map(lambda x:datetime.fromtimestamp(int(x.split("_")[-1].split(".")[0])))
    rescued_gemini_results = rescued_gemini_results.dropna()

    current_gemini_results = read_pickle(cf["paths"]["gemini_video_analysis"])

    rescued_gemini_results = rescued_gemini_results[~rescued_gemini_results.item_id.isin(current_gemini_results.item_id)]

    updated_gemini_results = concat([current_gemini_results, rescued_gemini_results])

    print(f"Gemini DF shapes: Old: {current_gemini_results.shape}, New: {rescued_gemini_results.shape} and Combined: {updated_gemini_results.shape}")

    if verbose:
        hey_there = input("Y to continue saving the rescued data, or N to exit")

        if hey_there.lower() != "y":
            print("Exiting without saving the rescued data.")
            return

    # backup the existing analysis data and move it to the backup folder
    back_this_up(cf["paths"]["gemini_video_analysis"], move_the_file=True)

    if verbose:
        print(f"Saving the old & rescued analysis data to {cf["paths"]["gemini_video_analysis"]}")
    updated_gemini_results.to_pickle(cf["paths"]["gemini_video_analysis"])

    if verbose:
        hey_there = input("Y to continue deleting all the rescued json files in the temp directory, or N to exit")

        if hey_there.lower() != "y":
            print("Exiting without saving the rescued data.")
            return

    for filename in json_saves:
        file_path = temp_path(filename)
        remove(file_path)
        if verbose:
            print(".", end="", flush=True)
    if verbose:
        print(" done")



def upload_to_gemini(path, mime_type=None, verbose=False):
    from google.generativeai import upload_file, configure
    from os.path import exists
    import sys

    cf = init_config()

    try:
        configure(api_key=cf["gemini"]["key"])
    except:
        print("Error Gemini API key (upload_to_gemini). Exiting.")
        sys.exit(0)

    if not exists(path):
        raise FileNotFoundError(f"File '{path}' not found")

    try:
        file = upload_file(path, mime_type=mime_type)
        if verbose:
            print(f"File uploaded to Gemini.")
    except Exception as e:
        print(f"Error uploading file to Gemini: {e}")
        return None
    
    return file



def wait_for_files_active(files, verbose = False):
    from time import sleep
    from google.generativeai import get_file, configure
    import sys

    cf = init_config()

    try:
        configure(api_key=cf["gemini"]["key"])
    except:
        print("Error Gemini API key (wait_for_files_active). Exiting.")
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
            print(f"File {name} is not {file.state.name}")
            return False
    if verbose:
        print("...all files ready")
    return True



def analyze_single_video(this_file, timeout=200, verbose=False):
    from json import loads
    from google.generativeai.types import HarmCategory, HarmBlockThreshold
    from google.generativeai import GenerativeModel, configure
    import sys

    cf = init_config()

    try:
        configure(api_key=cf["gemini"]["key"])
    except:
        print("Error Gemini API key (analyze_single_video). Exiting.")
        sys.exit(0)

    with open(cf["gemini"]["prompt"], 'r') as file:
        gemini_prompt = file.read()

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
            if verbose:
                print(f"Response received: {raw_string}")

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



def gemini_analysis_from_video_filename(a_video_filename,
                                        timeout=30,
                                        verbose=False):
    from datetime import datetime
    from os.path import join, basename
    from json import dump

    the_item_id = int(basename(a_video_filename).split(".")[0])

    if verbose:
        print(f"Uploading video {the_item_id} to Gemini...")
    files_for_gemini = [
        upload_to_gemini(a_video_filename, 
                         mime_type="video/mp4", 
                         verbose=verbose),
    ]
    files_for_gemini = [gf for gf in files_for_gemini if not gf is None] # remove any None values, i.e. failed uploads

    if len(files_for_gemini) > 0:

        file_is_ready_for_analysis = wait_for_files_active(files_for_gemini, verbose=verbose)

        analysis_start_time = datetime.now()

        if file_is_ready_for_analysis:        
            if verbose:
                print(f"Analyzing video {the_item_id}...")
            fine_video_analysis_results = analyze_single_video(files_for_gemini[0], 
                                                          timeout=timeout, 
                                                          verbose=verbose)
        else:
            if verbose:
                print(f"File prep for Gemini analysis failed: {the_item_id}")
            fine_video_analysis_results = {}
    else:
        if verbose:
            print(f"File was not uploaded to Gemini, no analysis was made:  {the_item_id}")
        fine_video_analysis_results = {}

    if len(fine_video_analysis_results) > 0:
        print(f"{the_item_id} Gemini analysis successful")
    else:
        print(f"{the_item_id} Gemini analysis failed")


    analysis_time = (datetime.now() - analysis_start_time).total_seconds()

    
    fine_video_analysis_results["item_id"] = the_item_id
    fine_video_analysis_results["analysis_time"] = analysis_time
    fine_video_analysis_results["analysis_ts"] = int(datetime.now().timestamp())

    # Save the result at this stage for this single video as a precaution if it all blows up
    temp_fn = join(temp_path(f"temp_gemini_results_{fine_video_analysis_results["item_id"]}_{fine_video_analysis_results["analysis_ts"]}.json"))
    if verbose:
        print(f"Saving temp Gemini results to {temp_fn}")
    with open(temp_fn, 'w') as file:
        dump(fine_video_analysis_results,file)
    
    return fine_video_analysis_results






def gemini_video_process(the_video_file_w_number, verbose=False):
    from datetime import datetime
    from os import remove

    the_number = the_video_file_w_number[0]
    the_video_filename = the_video_file_w_number[1]
    cf = the_video_file_w_number[2]

    tutti_start_time = datetime.now()
    timeout = cf["gemini"]["timeout"]

    main_media_storage = init_media_storage(verbose=verbose)

    if verbose:
        print("Loading video object...")
    load_blob_from_storage(main_media_storage, 
                  the_video_filename,
                  prefix=cf["media_storage"]["video_prefix"], 
                  dest_dir=temp_path())

    video_analysis_results = gemini_analysis_from_video_filename(temp_path(the_video_filename),
                                                                 timeout=timeout,
                                                                 verbose=verbose)

    tutti_time = (datetime.now() - tutti_start_time).total_seconds()
    video_analysis_results["processing_time"] = tutti_time

    print(f"{the_number:04} {the_video_filename.split('.')[0]} done. Gemini analysis: {video_analysis_results["analysis_time"]:.1f}s. Total time: {tutti_time:.1f}s")

    if verbose:
        print("Deleting files...")
    remove(temp_path(the_video_filename))

    return video_analysis_results






if __name__ == "__main__":
    print("Module is being run directly.")

