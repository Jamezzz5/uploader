import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import uploader.utils as utl
from requests_oauthlib import OAuth2Session

dcm_path = 'dcm'
config_path = os.path.join(utl.config_file_path, dcm_path)

base_url = 'https://www.googleapis.com/dfareporting'


class DcApi(object):
    version = '3.2'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.usr_id = None
        self.report_id = None
        self.config_list = None
        self.client = None
        self.lp_dict = {}
        self.cam_dict = {}
        self.ag_dict = {}
        self.ad_dict = {}
        self.df = pd.DataFrame()
        self.r = None
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix.  '
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading DC config file: {}'.format(config))
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.config_file))
            sys.exit(0)
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.access_token = self.config['access_token']
        self.refresh_token = self.config['refresh_token']
        self.refresh_url = self.config['refresh_url']
        self.usr_id = self.config['usr_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.refresh_token, self.refresh_url, self.usr_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in DC config file.'
                                'Aborting.'.format(item))
                sys.exit(0)

    def get_client(self):
        token = {'access_token': self.access_token,
                 'refresh_token': self.refresh_token,
                 'token_type': 'Bearer',
                 'expires_in': 3600,
                 'expires_at': 1504135205.73}
        extra = {'client_id': self.client_id,
                 'client_secret': self.client_secret}
        self.client = OAuth2Session(self.client_id, token=token)
        token = self.client.refresh_token(self.refresh_url, **extra)
        self.client = OAuth2Session(self.client_id, token=token)

    def create_url(self, entity=None):
        vers_url = '/v{}'.format(self.version)
        usr_url = '/userprofiles/{}/'.format(self.usr_id)
        full_url = (base_url + vers_url + usr_url)
        if entity:
            full_url += entity
        return full_url

    @staticmethod
    def get_id(dict_o, match, dict_two=None, match_two=None, parent_id=None,
               match_name='name', parent_name='parent'):
        if parent_id:
            id_list = [k for k, v in dict_o.items() if v[match_name] == match
                       and v[parent_name] == parent_id]
        else:
            id_list = [k for k, v in dict_o.items() if v[match_name] == match]
        if dict_two is not None:
            id_list = [k for k, v in dict_two.items() if v[match_name] == match_two
                       and v[parent_name] == id_list[0]]
        return id_list

    def get_id_dict(self, entity=None, parent=None, fields=None, nest=None,
                    resp_entity=None):
        url = self.create_url(entity)
        id_dict = {}
        params = {}
        next_page = True
        next_page_token = None
        while next_page:
            if next_page_token:
                params['pageToken'] = next_page_token
            r = self.make_request(url, method='get', params=params)
            id_dict = self.get_dict_from_page(id_dict, r.json(),
                                              list(parent.values())[0],
                                              list(fields.values()), nest,
                                              resp_entity)
            if 'nextPageToken' in r.json():
                next_page_token = r.json()['nextPageToken']
            else:
                next_page = False
        return id_dict

    @staticmethod
    def get_dict_from_page(id_dict, page, parent, fields=None, nest=None,
                           entity=None):
        resp_fields = [parent]
        if fields:
            resp_fields += fields
        id_dict.update({x[nest]['id'] if nest else x['id']:
                       {'parent' if y == parent else y.replace('.', ''):
                        x[nest][y] if nest and y in x[nest] else x[y]
                        for y in resp_fields
                        if y in x or (nest and y in x[nest])}
                        for x in page[entity] if entity})
        return id_dict

    def get_lp_id_dict(self):
        parent = {'advertiserId': 'advertiserId'}
        fields = {'id': 'id', 'url': 'url'}
        lp_dict = self.get_id_dict(entity='advertiserLandingPages',
                                   parent=parent, fields=fields,
                                   resp_entity='landingPages')
        return lp_dict

    def get_cam_id_dict(self):
        parent = {'advertiserId': 'advertiserId'}
        fields = {'id': 'id', 'name': 'name'}
        cam_dict = self.get_id_dict(entity='campaigns', parent=parent,
                                    fields=fields, resp_entity='campaigns')
        return cam_dict

    def set_id_dict(self, dcm_object=None):
        if dcm_object == 'landing_page':
            self.lp_dict = self.get_lp_id_dict()
        if dcm_object == 'campaign':
            self.cam_dict = self.get_cam_id_dict()

    def create_campaign(self, campaign, entity='campaigns'):
        url = self.create_url(entity)
        r = self.make_request(url, method='post', body=campaign.cam_dict)
        if 'error' in r.json():
            logging.warning('Campaign not uploaded.  '
                            'Response: \n {}'.format(r.json()))
        return r

    def create_landing_page(self, lp, entity='advertiserLandingPages'):
        url = self.create_url(entity)
        r = self.make_request(url, method='post', body=lp.lp_dict)
        if 'error' in r.json():
            logging.warning('Landing page not uploaded.  '
                            'Response: \n {}'.format(r.json()))
        return r

    def make_request(self, url, method, params=None, body=None):
        self.get_client()
        try:
            self.r = self.raw_request(url, method, params, body)
        except requests.exceptions.SSLError as e:
            logging.warning('Warning SSLError as follows {}'.format(e))
            time.sleep(30)
            self.r = self.make_request(url, method, params, body)
        return self.r

    def raw_request(self, url, method, params=None, body=None):
        if not params:
            params = {}
        if body:
            if method == 'get':
                self.r = self.client.get(url, params=params, json=body)
            elif method == 'post':
                self.r = self.client.post(url, params=params, json=body)
        else:
            if method == 'get':
                self.r = self.client.get(url, params=params)
            elif method == 'post':
                self.r = self.client.post(url, params=params)
        return self.r

    def request_error(self):
        logging.warning('Unknown error: {}'.format(self.r.text))
        sys.exit(0)


class CampaignUpload(object):
    name = 'name'
    advertiserId = 'advertiserId'
    status = 'defaultLandingPage'
    sd = 'startDate'
    ed = 'endDate'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='campaign_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        for col in [self.sd, self.ed]:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        self.config = df.to_dict(orient='index')

    def set_campaign(self, campaign_id, api=None):
        cam = Campaign(self.config[campaign_id], api=api)
        return cam

    def upload_all_campaigns(self, api):
        total_camp = str(len(self.config))
        for idx, c_id in enumerate(self.config):
            logging.info('Uploading campaign {} of {}.  '
                         'Campaign Name: {}'.format(idx + 1, total_camp, c_id))
            self.upload_campaign(api, c_id)
        logging.info('Pausing for 30s while campaigns finish uploading.')
        time.sleep(30)

    def upload_campaign(self, api, campaign_id):
        campaign = self.set_campaign(campaign_id, api)
        if not campaign.check_exists(api):
            api.create_campaign(campaign)


class Campaign(object):
    __slots__ = ['name', 'advertiserId', 'archived', 'defaultLandingPageId',
                 'startDate', 'endDate', 'cam_dict', 'api']

    def __init__(self, cam_dict, api=None):
        self.defaultLandingPageId = None
        for k in cam_dict:
            setattr(self, k, cam_dict[k])
        self.api = api
        if self.api:
            self.get_landing_page_id(self.api)
        self.cam_dict = self.create_cam_dict()

    def create_cam_dict(self):
        cam_dict = {
            'name': '{}'.format(self.name),
            'archived': '{}'.format('false'),
            'startDate': '{}'.format(self.startDate),
            'endDate': '{}'.format(self.endDate),
            'advertiserId': int(self.advertiserId),
            'defaultLandingPageId': int(self.defaultLandingPageId)
        }
        return cam_dict

    def get_landing_page_id(self, api):
        lp = LandingPage({'name': self.defaultLandingPageId,
                          'advertiserId': self.advertiserId,
                          'url': self.defaultLandingPageId}, api=api)
        self.defaultLandingPageId = lp.id

    def check_exists(self, api):
        if not api.cam_dict:
            api.set_id_dict('campaign')
        cid = api.get_id(api.cam_dict, self.name)
        if cid:
            logging.warning('{} already in account.  '
                            'This was not uploaded.'.format(self.name))
            return True


class LandingPage(object):
    __slots__ = ['name', 'id', 'url', 'advertiserId', 'lp_dict', 'api']

    def __init__(self, lp_dict, api=None):
        self.id = None
        for k in lp_dict:
            setattr(self, k, lp_dict[k])
        self.api = api
        self.lp_dict = self.create_lp_dict()
        if self.api:
            self.get_landing_page_id(self.api)

    def create_lp_dict(self):
        lp_dict = {
            'name': '{}'.format(self.name),
            'url': '{}'.format(self.url),
            'advertiserId': '{}'.format(self.advertiserId)
        }
        return lp_dict

    def get_landing_page_id(self, api):
        if not api.lp_dict:
            api.set_id_dict('landing_page')
        lp_ids = api.get_id(api.lp_dict, self.name,
                            parent_id=self.advertiserId, match_name='url')
        if lp_ids:
            self.id = lp_ids[0]
        else:
            logging.info('Landing page does not exist. Uploading')
            self.upload(api)

    def upload(self, api):
        logging.info('Uploading landing page with {}'.format(self.lp_dict))
        r = api.create_landing_page(self, entity='advertiserLandingPages')
        self.id = r.json()['id']
