#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 16:10:19 2023

@author: evelyncan & isadarvin
"""

import csv
import json
import pandas as pd
import numpy as np
import pandas_gbq
from ast import literal_eval 
import re
from flask import Flask, request, render_template
import os
import requests
import mysql.connector
import google.cloud.logging
import logging
from flask_cors import CORS, cross_origin

#prints logs to console
def log(message, client):
    logging.warning(message)
    print(message)


app = Flask(__name__)

@app.route("/instagram-follower", methods=["POST"])
def cleaninstagramfollower():
    #gets the json payload
    payload = request.json
    #connects to google
    #sets up loggin
    client = google.cloud.logging.Client()
    client.setup_logging()

    df = pd.DataFrame(payload)

    df = df.rename(columns={'public_email':'email', 'contact_phone_number':'Phone'})
    df = df.dropna(subset=['email', 'Phone'], how = 'all')
    try:
        df['email'] = df['email'].str.strip()
        df['email'] = df['email'].replace(' ', '', regex=True)
        df = df.drop_duplicates(subset=['email'])
        df = df.dropna(subset=['email'])
        discard = ["gov", ".mil", "info", "dev", "test", "fuck", "shit", "bitch", "stupid", "dumb", "nytimes", "bushwick"]
        df = df[~df.email.str.contains('|'.join(discard))]
    except Exception as e:
        log(e, client)
    df = df[['username', 'email', 'Phone', 'follower_count', 'following_count', 'biography', 'query']]
    
    #return to json to send back 
    json_data = df.to_json(orient='columns')
    return json_data


@app.route("/instagram-profile", methods=["POST"])
def cleaninstagramprofile():
    #gets the json payload
    payload = request.json
    #connects to google
    #sets up loggin
    client = google.cloud.logging.Client()
    client.setup_logging()

    df = pd.DataFrame(payload)
    
    df = df.rename(columns={'public_email':'email', 'contact_phone_number':'Phone'})
    df = df.dropna(subset=['email', 'Phone'], how = 'all')
    try:
        df['email'] = df['email'].str.strip()
        df['email'] = df['email'].replace(' ', '', regex=True)
        df = df.drop_duplicates(subset=['email'])
        df = df.dropna(subset=['email'])
        discard = ["gov", ".mil", "info", "dev", "test", "fuck", "shit", "bitch", "stupid", "dumb", "nytimes", "bushwick"]
        df = df[~df.email.str.contains('|'.join(discard))]
    except Exception as e:
        log(e, client)

    df = df[['username', 'email', 'Phone', 'follower_count', 'following_count', 'biography', 'query']]
    #return to json to send back 
    json_data = df.to_json(orient='columns')
    return json_data


@app.route("/instagram-hashtag", methods=["POST"])
def cleaninstagramhashtag():
    #gets the json payload
    payload = request.json
    #connects to google
    #sets up loggin
    client = google.cloud.logging.Client()
    client.setup_logging()

    df = pd.DataFrame(payload)
    df = df.rename(columns={'phone':'Phone'})
    df = df.dropna(subset=['email', 'Phone'], how = 'all')
    try:
        df['email'] = df['email'].str.strip()
        df['email'] = df['email'].replace(' ', '', regex=True)
        df = df.drop_duplicates(subset=['email'])
        df = df.dropna(subset=['email'])
        discard = ["gov", ".mil", "info", "dev", "test", "fuck", "shit", "bitch", "stupid", "dumb", "nytimes", "bushwick"]
        df = df[~df.email.str.contains('|'.join(discard))]
    except Exception as e:
        log(e, client)
    df = df[['username', 'email', 'Phone', 'follower_count', 'following_count', 'biography', 'query']]
    #return to json to send back 
    json_data = df.to_json(orient='columns')
    return json_data


@app.route("/tiktok-hashtag", methods=["POST"])
def cleantiktokhashtag():
    #gets the json payload
    payload = request.json
    #connects to google
    #sets up loggin
    client = google.cloud.logging.Client()
    client.setup_logging()

    #turning all files into a single df
    dfs_csv = [pd.read_json(file) for file in files]
    common_col = list(set.intersection(*(set(df.columns) for df in dfs_csv)))
    df_input = pd.DataFrame(payload)
    df_input = pd.concat([df[common_col] for df in dfs_csv], ignore_index=False)
    l = df_input.values.tolist()
    df = pd.DataFrame()
    ls = []
    following = []
    followers = []
    username = []
    #extracting all emails and relevant data 
    for a in l:
        for b in a:
            try:
                b = b.replace("'", '"')
                b = literal_eval(b)
            except:
                ls.append(None)
                following.append(None)
                followers.append(None)
                username.append(None)
                continue
            if isinstance(b, dict) and "authorMeta" in b:
                author = b["authorMeta"]
                bio = author["signature"]
                ls.append(bio)
                
                f = author["following"]
                following.append(f)
                fs = author["fans"]
                followers.append(fs)
                u = author["name"]
                username.append(u)
    
    df["bio"] = ls
    df["following_count"] = following
    df["follower_count"] = followers
    df["username"] = username
    df["following_count"] = df["following_count"].astype(float)
    df["follower_count"] = df["follower_count"].astype(float)
    
    email = []
    for c in ls:
        e = re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", str(c))
        email.append(e)
        
    df['email'] = email
    
    def extract_email(lst):
        if len(lst) > 0:
            return lst[0]
        else:
            return None
        
    df['email'] = df['email'].apply(extract_email)
    df_input = df.copy()
    #process of cleaning the email col that has now been created
    df_input = df_input.dropna(subset=['email'])

    try:
        df_input['email'] = df_input['email'].str.strip()
        df_input['email'] = df_input['email'].replace(' ', '', regex=True)
        df_input = df_input.drop_duplicates(subset=['email'])
        df_input = df_input.dropna(subset=['email'])
        discard = ["gov", ".mil", "info", "dev", "test", "fuck", "shit", "bitch", "stupid", "dumb", "nytimes", "bushwick"]
        df_input = df_input[~df_input.email.str.contains('|'.join(discard))]
    except Exception as e:
        log(e, client)
    df_input = df_input[['username', 'email', 'follower_count', 'following_count', 'bio']]
    sending = df_input.to_dict('records')
    return sending

"""
@app.route("/other-types", methods=["POST"])
def ingestform():
    
    #gets the json payload
    payload = request.json
    #connects to google
    #sets up loggin
    client = google.cloud.logging.Client()
    client.setup_logging()
    #sends the payload to the below mentioned cloud-run
    #saves payload in a sql database
    dfs_csv = [pd.read_csv(file, lineterminator='\n') for file in files]

    return {csv back to user after being cleaned}

def sending_to_bq(full_csv):
    full_csv.to_gbq(f"people-first-337420.Staging.{scrape_name}_{date_cleaned}", project_id="people-first-337420", if_exists = 'replace')
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)