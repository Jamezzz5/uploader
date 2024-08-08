import sys
import pytest
import logging
import argparse
import pandas as pd
import upload.utils as utl
import upload.creator as cre
import upload.fbapi as fbapi
import upload.awapi as awapi
import upload.dcapi as dcapi
import upload.szkapi as szkapi


def test_adset():
    api = fbapi.FbApi(config_file='fbconfig.json')
    # asu = fbapi.AdSetUpload()
    # asu.upload_all_adsets(api=api)
    params = {'name': 'Pre-Order_Meta_BR_MMORPG Fans_Cross Device',
              'campaign_id': '120212735965520215',
              'billing_event': 'IMPRESSIONS',
              'status': 'PAUSED',
              'targeting': '{"age_max":44,"age_min":18,"custom_audiences":[],"device_platforms":["mobile","desktop"],"facebook_positions":["feed"],"genders":[1],"geo_locations":{"countries":["BR"]},"publisher_platforms":["facebook","instagram"]}',
              'start_time': '08/1/2024', 'end_time': '08/20/2024',
              'bid_amount': 200,
              'promoted_object': '{"custom_event_type":"PURCHASE","page_id":"114036714208","pixel_id":"2153927484709809"}',
              'lifetime_budget': 200000}
    api.account.create_ad_set(params=params)
