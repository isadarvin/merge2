import pandas as pd
import requests
import json

endpoint = 'https://merge-2-6eq4iaayqa-uc.a.run.app'

with open('follower_scrape_test.json') as f:
    data = json.load(f)

test = requests.post(endpoint+"/instagram-follower", json=data)

print(test.json)