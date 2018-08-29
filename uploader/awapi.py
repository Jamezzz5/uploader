import os
import sys
import yaml
import time
import logging
import numpy as np
import pandas as pd
import datetime as dt
import uploader.utils as utl
from io import BytesIO
from googleads import adwords

config_path = utl.config_file_path


class AwApi(object):
    def __init__(self):
        self.df = pd.DataFrame()
        self.config = None
        self.configfile = None
        self.client_id = None
        self.client_secret = None
        self.developer_token = None
        self.refresh_token = None
        self.client_customer_id = None
        self.config_list = []
        self.adwords_client = None
        self.report_type = 'AD_PERFORMANCE_REPORT'

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning('Config file name not in vendor matrix. ' +
                            'Aborting.')
            sys.exit(0)
        logging.info('Loading Adwords config file: {}'.format(config))
        self.configfile = os.path.join(config_path, config)
        self.load_config()
        self.check_config()
        self.adwords_client = (adwords.AdWordsClient.
                               LoadFromStorage(self.configfile))

    def load_config(self):
        try:
            with open(self.configfile, 'r') as f:
                self.config = yaml.safe_load(f)
        except IOError:
            logging.error('{} not found.  Aborting.'.format(self.configfile))
            sys.exit(0)
        self.config = self.config['adwords']
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.developer_token = self.config['developer_token']
        self.refresh_token = self.config['refresh_token']
        self.client_customer_id = self.config['client_customer_id']
        self.config_list = [self.config, self.client_id, self.client_secret,
                            self.developer_token, self.refresh_token,
                            self.client_customer_id]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning('{} not in AW config file.'.format(item))
                sys.exit(0)

class CampaignUpload(object):
    def __init__(self, config_file=None):
        self.config_file = config_file
        self.name = 'campaign_name'
        self.objective = 'campaign_objective'
        self.spend_cap = 'campaign_spend_cap'
        self.status = 'campaign_status'
        self.config = None
        self.cam_objective = None
        self.cam_status = None
        self.cam_spend_cap = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_campaign_upload.xlsx'):
        df = pd.read_excel(config_path + config_file)
        df = df.dropna(subset=[self.name])
        for col in [self.spend_cap]:
            df[col] = df[col] * 100
        self.config = df.set_index(self.name).to_dict(orient='index')

    def check_config(self, campaign):
        pass
        # self.check_param(campaign, self.objective, Campaign.Objective)
        # self.check_param(campaign, self.status, Campaign.EffectiveStatus)

    def check_param(self, campaign, param, param_class):
        input_param = self.config[campaign][param]
        valid_params = [v for k, v in vars(param_class).items()
                        if not k[-2:] == '__']
        if input_param not in valid_params:
            logging.warning('{} not valid.  Use one of the'
                            'following names: {}'.format(param, valid_params))

    def set_campaign(self, campaign):
        self.cam_objective = self.config[campaign][self.objective]
        self.cam_spend_cap = self.config[campaign][self.spend_cap]
        self.cam_status = self.config[campaign][self.status]

    def upload_all_campaigns(self, api):
        total_campaigns = str(len(self.config))
        for idx, campaign in enumerate(self.config):
            logging.info('Uploading campaign ' + str(idx + 1) + ' of ' +
                         total_campaigns + '.  Campaign Name: ' + campaign)
            self.upload_campaign(api, campaign)
        logging.info('Pausing for 30s while campaigns finish uploading.')
        time.sleep(30)

    def upload_campaign(self, api, campaign):
        self.check_config(campaign)
        self.set_campaign(campaign)
        api.create_campaign(campaign, self.cam_objective, self.cam_status,
                            self.cam_spend_cap)
