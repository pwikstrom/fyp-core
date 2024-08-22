# -*- coding: utf-8 -*-
"""
Script Name: 
Description: 
Author: freelon
Date: Thu Jul 14 14:06:01 2022
"""

import browser_cookie3
from bs4 import BeautifulSoup
from datetime import datetime
import json
import numpy as np
import os
import pandas as pd
import random
import re
import requests
import time

headers = {'Accept-Encoding': 'gzip, deflate, sdch',
           'Accept-Language': 'en-US,en;q=0.8',
           'Upgrade-Insecure-Requests': '1',
           'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
           'Cache-Control': 'max-age=0',
           'Connection': 'keep-alive'}
url_regex = r'(?<=\.com/)(.+?)(?=\?|$)'
runsb_rec = 'We strongly recommend you run \'specify_browser\' first, which will allow you to run pyktok\'s functions without using the browser_name parameter every time. \'specify_browser\' takes as its sole argument a string representing a browser installed on your system, e.g. "chrome," "firefox," "edge," etc.'
runsb_err = 'No browser defined for cookie extraction. We strongly recommend you run \'specify_browser\', which takes as its sole argument a string representing a browser installed on your system, e.g. "chrome," "firefox," "edge," etc.'

#print(runsb_rec)


class BrowserNotSpecifiedError(Exception):
    def __init__(self):
        super().__init__(runsb_err)


def specify_browser(browser):
    global cookies
    cookies = getattr(browser_cookie3, browser)(domain_name='www.tiktok.com')


def deduplicate_metadata(metadata_fn, video_df, dedup_field='video_id'):
    if os.path.exists(metadata_fn):
        metadata = pd.read_csv(metadata_fn, keep_default_na=False)
        combined_data = pd.concat([metadata, video_df])
        combined_data[dedup_field] = combined_data[dedup_field].astype(str)
    else:
        combined_data = video_df
    return combined_data.drop_duplicates(dedup_field)


"""pyk_data_structure = {
    'desc': str,
    'createTime': datetime,
    'challenges': str,
    'item_id': int,
    'video_duration': int,
    'author_id': int,
    'author_uniqueId': str,
    'author_nickname': str,
    'author_signature': str,
    'author_verified': bool,
    'music_id': int,
    'music_title': str,
    'music_authorName': str,
    'music_original': bool,
    'music_duration': int,
    'stats_diggCount': int,
    'stats_commentCount': int,
    'stats_playCount': int,
    'stats_collectCount': int,
    'anchors': str,
    'poi_name': str,
    'poi_address': str,
    'poi_city': str,
    'IsAigc': bool,
    'AIGCDescription': str,
    'video_cover': str,
    'poi_province': str,
    'poi_country': str,
    'stats_shareCount': int,
    'playlistId': int,
    'isAd': bool,
    'music_album': str,
    'aigcLabelType': int,
    'video_downloaded': bool,
    'audio_extracted': bool,
    'cover_downloaded': bool,
    'last_modified': datetime
}"""

pyk_data_defaults = {
    'desc': "",
    'createTime': "no default", # the type is datetime, but that doesn't work for me but aok, since I'm not using it anyway
    'item_id': 0,
    'video_duration': 0,
    'author_id': 0,
    'author_uniqueId': "",
    'author_nickname': "",
    'author_signature': "",
    'author_verified': False,
    'music_id': 0,
    'music_title': "",
    'music_authorName': "",
    'music_album': "",
    'music_original': False,
    'music_duration': 0,
    'playlistId': 0,
    'stats_diggCount': -1,
    'stats_commentCount': -1,
    'stats_playCount': -1,
    'stats_collectCount': -1,
    'stats_shareCount': -1,
    'anchors': "",
    'challenges': "",
    'poi_name': "",
    'poi_address': "",
    'poi_city': "",
    'poi_province': "",
    'poi_country': "",
    'IsAigc': False,
    'AIGCDescription': "",
    'aigcLabelType': "",
    'isAd': False,
    'video_cover': "",
    'video_downloaded': False,
    'audio_extracted': False,
    'cover_downloaded': False,
    'do_not_modify': True,
    'last_modified': "no default" # the type is datetime, but that doesn't work for me but aok, since I'm not using it anyway
}



"""
This is Patrik's version of this function. The original is on the original Github repo
"""
def generate_data_row(item_struct):
    from copy import copy
    from pandas import DataFrame



    item_struct["item_id"] = int(item_struct["id"])

    try:
        item_struct["createTime"] = datetime.fromtimestamp(int(item_struct["createTime"]))
    except:
        item_struct["createTime"] = datetime(2000,1,1)

    item_struct["last_modified"] = datetime.now()

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
    
    if "anchors" in item_struct.keys() and isinstance(item_struct["anchors"],list) and \
        len(item_struct["anchors"]) > 0 and "description" in item_struct["anchors"][0].keys():
            item_struct["anchors"] = "|".join([f"[{g['id']}]{g['description']}" for g in item_struct["anchors"]])

    if "poi" in item_struct.keys():
        for k in ["name","address","city","province","country"]:
            if k in item_struct["poi"].keys():
                item_struct["poi_"+k] = item_struct["poi"][k]


    for k in ["diggCount","shareCount","commentCount","playCount","collectCount"]:
        if k in item_struct["stats"].keys():
            item_struct["stats_"+k] = int(item_struct["stats"][k])

    pyk_data_types = {}
    for dh in pyk_data_defaults.keys():
        if not dh in ["createTime","last_modified"]:
            pyk_data_types[dh] = type(pyk_data_defaults[dh])
            if dh not in item_struct.keys():
                item_struct[dh] = pyk_data_defaults[dh]
    print()


    pyk_data = DataFrame.from_dict(item_struct,orient="index").T
    pyk_data["createTime"] = pd.to_datetime(pyk_data["createTime"])
    pyk_data["last_modified"] = pd.to_datetime(pyk_data["last_modified"])
    pyk_data = pyk_data[list(pyk_data_defaults.keys())]
    pyk_data = pyk_data.astype(pyk_data_types)

    return pyk_data





def get_tiktok_raw_data(a_url, browser_name=None):
    if 'cookies' not in globals() and browser_name is None:
        raise BrowserNotSpecifiedError
    global cookies
    if browser_name is not None:
        cookies = getattr(browser_cookie3, browser_name)(
            domain_name='www.tiktok.com')
    tt = requests.get(a_url,
                      headers=headers,
                      cookies=cookies,
                      timeout=20)
    # retain any new cookies that got set in this request
    cookies = tt.cookies
    soup = BeautifulSoup(tt.text, "html.parser")
    return soup


def get_tiktok_json(video_url, browser_name=None):
    if 'cookies' not in globals() and browser_name is None:
        raise BrowserNotSpecifiedError
    global cookies
    if browser_name is not None:
        cookies = getattr(browser_cookie3, browser_name)(
            domain_name='www.tiktok.com')
    tt = requests.get(video_url,
                      headers=headers,
                      cookies=cookies,
                      timeout=20)
    # retain any new cookies that got set in this request
    cookies = tt.cookies
    soup = BeautifulSoup(tt.text, "html.parser")
    tt_script = soup.find('script', attrs={'id': "SIGI_STATE"})
    try:
        tt_json = json.loads(tt_script.string)
    except AttributeError:
        return
    return tt_json



def alt_get_tiktok_json(video_url, browser_name=None):
    if 'cookies' not in globals() and browser_name is None:
        raise BrowserNotSpecifiedError
    global cookies
    if browser_name is not None:
        cookies = getattr(browser_cookie3, browser_name)(
            domain_name='www.tiktok.com')
    tt = requests.get(video_url,
                      headers=headers,
                      cookies=cookies,
                      timeout=20)
    # retain any new cookies that got set in this request
    cookies = tt.cookies
    soup = BeautifulSoup(tt.text, "html.parser")
    tt_script = soup.find(
        'script', attrs={'id': "__UNIVERSAL_DATA_FOR_REHYDRATION__"})
    try:
        tt_json = json.loads(tt_script.string)
    except AttributeError:
        #print("The function encountered a downstream error and did not deliver any data, which happens periodically for various reasons. Please try again later.")
        return
    return tt_json



def save_tiktok(video_url,
                save_video=True,
#                metadata_fn='',
                browser_name=None,
                save_path=""):
    
    from os.path import join
    fgh = join("data", save_path)


    if 'cookies' not in globals() and browser_name is None:
        raise BrowserNotSpecifiedError
    #if save_video == False and metadata_fn == '':
    #    print('Since save_video and metadata_fn are both False/blank, the program did nothing.')
    #    return

    tt_json = get_tiktok_json(video_url, browser_name)

    if False and tt_json is not None:
        video_id = list(tt_json['ItemModule'].keys())[0]

        if save_video == True:
            regex_url = re.findall(url_regex, video_url)[0]
            if 'imagePost' in tt_json['ItemModule'][video_id]:
                slidecount = 1
                for slide in tt_json['ItemModule'][video_id]['imagePost']['images']:
                    video_fn = regex_url.replace(
                        '/', '_') + '_slide_' + str(slidecount) + '.jpeg'
                    tt_video_url = slide['imageURL']['urlList'][0]
                    headers['referer'] = 'https://www.tiktok.com/'
                    # include cookies with the video request
                    #print(tt_video_url)
                    tt_video = requests.get(
                        tt_video_url, allow_redirects=True, headers=headers, cookies=cookies)
                    with open(join(save_path,video_fn), 'wb') as fn:
                        fn.write(tt_video.content)
                    slidecount += 1
            else:
                regex_url = re.findall(url_regex, video_url)[0]
                video_fn = regex_url.replace('/', '_') + '.mp4'
                tt_video_url = tt_json['ItemModule'][video_id]['video']['downloadAddr']
                headers['referer'] = 'https://www.tiktok.com/'
                # include cookies with the video request
                #print(tt_video_url)
                tt_video = requests.get(
                    tt_video_url, allow_redirects=True, headers=headers, cookies=cookies)
            with open(join(save_path,video_fn), 'wb') as fn:
                fn.write(tt_video.content)

        data_slot = tt_json['ItemModule'][video_id]
        data_row = generate_data_row(data_slot)
        try:
            user_id = list(tt_json['UserModule']['users'].keys())[0]
            user_verified = tt_json['UserModule']['users'][user_id]['verified']
            if user_verified == None:
                user_verified = False
            data_row.loc[0, "author_verified"] = user_verified
        except Exception:
            pass

    else:
        tt_json = alt_get_tiktok_json(video_url, browser_name)
        if tt_json:
            if save_video:
                #regex_url = re.findall(url_regex, video_url)[0]
                #video_fn = regex_url.replace('/', '_') + '.mp4'
                if video_url[-1] == '/':
                    video_fn = video_url.split("/")[-2] + ".mp4"
                else:
                    video_fn = video_url.split("/")[-1] + ".mp4"

                if len(tt_json["__DEFAULT_SCOPE__"]['webapp.video-detail']['itemInfo']['itemStruct']['video']) == 0:
                    print("The video metadata came back empty. Returning None.")
                    return
                
                tt_video_url = tt_json["__DEFAULT_SCOPE__"]['webapp.video-detail']['itemInfo']['itemStruct']['video']['downloadAddr']
                headers['referer'] = 'https://www.tiktok.com/'
                # include cookies with the video request
                #print("hejsan")
                tt_video = requests.get(
                    tt_video_url, allow_redirects=True, headers=headers, cookies=cookies)
                with open(join(save_path,video_fn), 'wb') as fn:
                    fn.write(tt_video.content)

            data_slot = tt_json["__DEFAULT_SCOPE__"]['webapp.video-detail']['itemInfo']['itemStruct']
            data_row = generate_data_row(data_slot)
            try:
                #user_id = list(tt_json['UserModule']['users'].keys())[0]
                user_verified = tt_json["__DEFAULT_SCOPE__"]['webapp.video-detail']['itemInfo']['itemStruct']['author']
                if user_verified == None:
                    user_verified = False
                data_row.loc[0, "author_verified"] = user_verified
            except Exception:
                pass
        else:
            data_row = pd.DataFrame()
    return data_row
