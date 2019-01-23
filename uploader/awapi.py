import os
import sys
import yaml
import time
import uuid
import logging
import pandas as pd
import uploader.utils as utl
from googleads import adwords

config_path = utl.config_file_path


class AwApi(object):
    def __init__(self, config_file=None):
        self.config_file = config_file
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
        self.cam_dict = {}
        self.v = 'v201806'
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config):
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

    @staticmethod
    def get_operation(operand, operator='ADD'):
        operation = [{
            'operator': operator,
            'operand': operand
        }]
        return operation

    def get_campaigns(self):
        cs = self.adwords_client.GetService('CampaignService', version=self.v)
        start_index = 0
        page_size = 100
        selector = {
            'fields': ['Id', 'Name', 'Status'],
            'paging': {
                'startIndex': '{}'.format(start_index),
                'numberResults': '{}'.format(page_size)
            }
        }
        more_pages = True
        while more_pages:
            page = cs.get(selector)
            self.cam_dict.update({x['name']: x['id'] for x in page['entries']
                                  if 'entries' in page})
            start_index += page_size
            selector['paging']['startIndex'] = str(start_index)
            more_pages = start_index < int(page['totalNumEntries'])

    def set_budget(self, name, budget, method):
        bs = self.adwords_client.GetService('BudgetService', version=self.v)
        budget = {
            'name': '{}-{}'.format(name, uuid.uuid4()),
            'amount': {
                'microAmount': '{}'.format(budget * 1000000)
            },
            'deliveryMethod': '{}'.format(method)
        }
        budget_operations = self.get_operation(budget)
        budget_id = bs.mutate(budget_operations)['value'][0]['budgetId']
        return budget_id

    def create_campaign(self, name, status, sd, ed, budget, method, freq,
                        channel, channel_sub, network, strategy, settings):
        cs = self.adwords_client.GetService('CampaignService', version=self.v)
        budget_id = self.set_budget(name, budget, method)
        operand = {
            'name': '{}'.format(name),
            'status': '{}'.format(status),
            'advertisingChannelType': '{}'.format(channel),
            'biddingStrategyConfiguration': strategy,
            'endDate': '{}'.format(ed),
            'budget': {
                'budgetId': budget_id
            },
            'networkSetting': network,
        }
        if sd:
            operand['startDate'] = '{}'.format(sd)
        if freq:
            operand['frequencyCap'] = freq
        if settings:
            operand['settings'] = settings
        if channel_sub:
            operand['advertisingChannelSubType'] = '{}'.format(channel_sub)
        operations = self.get_operation(operand)
        campaigns = cs.mutate(operations)
        return campaigns

    def create_adgroup(self, name, campaign_name, status, bid_type, bid):
        ags = self.adwords_client.GetService('AdGroupService', version=self.v)
        if not self.cam_dict:
            self.get_campaigns()
        bids = [{'xsi_type': bid_type,
                 'bid': {'microAmount': '{}'.format(bid * 1000000)}, }]
        operand = {
          'campaignId': '{}'.format(self.cam_dict[campaign_name]),
          'name': '{}'.format(name),
          'status': '{}'.format(status),
          'biddingStrategyConfiguration': {
              'bids': bids
          }
        }
        operations = self.get_operation(operand)
        ad_groups = ags.mutate(operations)
        return ad_groups


class CampaignUpload(object):
    def __init__(self, config_file=None):
        self.config_file = config_file
        self.name = 'name'
        self.status = 'status'
        self.sd = 'startDate'
        self.ed = 'endDate'
        self.budget = 'budget'
        self.method = 'deliveryMethod'
        self.freq = 'frequencyCap'
        self.channel = 'advertisingChannelType'
        self.channel_sub = 'advertisingChannelSubType'
        self.network = 'networkSetting'
        self.strategy = 'biddingStrategy'
        self.settings = 'settings'
        self.config = None
        self.cam_status = None
        self.cam_sd = None
        self.cam_ed = None
        self.cam_budget = None
        self.cam_method = None
        self.cam_freq = None
        self.cam_channel = None
        self.cam_channel_sub = None
        self.cam_network = None
        self.cam_strategy = None
        self.cam_settings = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_campaign_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        for col in [self.sd, self.ed]:
            df[col] = df[col].dt.strftime('%Y%m%d')
        self.config = df.set_index(self.name).to_dict(orient='index')
        for k in self.config:
            for item in [self.freq, self.network, self.strategy]:
                self.config[k][item] = self.config[k][item].split('|')
            self.set_dictionaries(k)

    def set_dictionaries(self, k):
        self.config[k][self.freq] = self.set_freq(self.config[k][self.freq])
        self.config[k][self.network] = self.set_net(self.config[k]
                                                    [self.network])
        self.config[k][self.strategy] = self.set_strat(self.config[k]
                                                       [self.strategy])

    @staticmethod
    def set_freq(freq):
        if freq:
            freq = {
                'impressions': freq[0],
                'timeUnit': freq[1],
                'level': freq[2]
            }
        return freq

    @staticmethod
    def set_net(network):
        net_dict = {
            'targetGoogleSearch': 'false',
            'targetSearchNetwork': 'false',
            'targetContentNetwork': 'false',
            'targetPartnerSearchNetwork': 'false'
        }
        if network:
            for net in network:
                net_dict[net] = 'true'
        return net_dict

    @staticmethod
    def set_strat(strategy):
        strat_dict = {
            'biddingStrategyType': strategy[0]
        }
        if len(strategy) == 1:
            strat_dict['bid'] = {'microAmount': strategy[1]}
        return strat_dict

    def set_campaign(self, campaign):
        self.cam_status = self.config[campaign][self.status]
        self.cam_sd = self.config[campaign][self.sd]
        self.cam_ed = self.config[campaign][self.ed]
        self.cam_budget = self.config[campaign][self.budget]
        self.cam_method = self.config[campaign][self.method]
        self.cam_freq = self.config[campaign][self.freq]
        self.cam_channel = self.config[campaign][self.channel]
        self.cam_channel_sub = self.config[campaign][self.channel_sub]
        self.cam_network = self.config[campaign][self.network]
        self.cam_strategy = self.config[campaign][self.strategy]
        self.cam_settings = self.config[campaign][self.settings]

    def upload_all_campaigns(self, api):
        total_camp = str(len(self.config))
        for idx, c in enumerate(self.config):
            logging.info('Uploading campaign {} of {}.'
                         'Campaign Name: {}'.format(idx + 1, total_camp, c))
            self.upload_campaign(api, c)
        logging.info('Pausing for 30s while campaigns finish uploading.')
        time.sleep(30)

    def upload_campaign(self, api, campaign):
        self.set_campaign(campaign)
        api.create_campaign(campaign, self.cam_status, self.cam_sd,
                            self.cam_ed, self.cam_budget, self.cam_method,
                            self.cam_freq, self.cam_channel,
                            self.cam_channel_sub, self.cam_network,
                            self.cam_strategy, self.cam_settings)


class AdGroupUpload(object):
    def __init__(self, config_file=None):
        self.config_file = config_file
        self.name = 'name'
        self.cam_name = 'campaignName'
        self.status = 'status'
        self.bid_type = 'bidtype'
        self.bid_val = 'bid'
        self.config = None
        self.ag_name = None
        self.ag_cam_name = None
        self.ag_status = None
        self.ag_bid_type = None
        self.ag_bid_val = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_campaign_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        self.config = df.set_index(self.name).to_dict(orient='index')

    def set_adgroup(self, adgroup):
        self.ag_name = adgroup
        self.ag_cam_name = self.config[adgroup][self.cam_name]
        self.ag_bid_val = self.config[adgroup][self.status]
        self.ag_bid_type = self.config[adgroup][self.bid_type]
        self.ag_bid_val = self.config[adgroup][self.bid_val]

    def upload_all_adgroups(self, api):
        total_ag = str(len(self.config))
        for idx, ag in enumerate(self.config):
            logging.info('Uploading adgroup {} of {}.  '
                         'Adgroup Name: {}'.format(idx + 1, total_ag, ag))
            self.upload_adgroup(api, ag)
        logging.info('Pausing for 30s while campaigns finish uploading.')
        time.sleep(30)

    def upload_adgroup(self, api, adgroup):
        self.set_adgroup(adgroup)
        api.create_adgroup(adgroup, self.ag_cam_name, self.ag_status,
                           self.ag_bid_type, self.ag_bid_val)
