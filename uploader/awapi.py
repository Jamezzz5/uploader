import os
import sys
import yaml
import time
import uuid
import logging
import numpy as np
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
        self.ag_dict = {}
        self.v = 'v201809'
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
            'operand': x
        } for x in operand]
        return operation

    def get_id_dict(self, service='CampaignService', parent=None,
                    parent_resp=None, page_len=100):
        cs = self.adwords_client.GetService(service, version=self.v)
        id_dict = {}
        start_index = 0
        selector_fields = ['Id', 'Name', 'Status']
        if parent:
            selector_fields.append(parent)
        selector = {'fields': selector_fields,
                    'paging': {'startIndex': '{}'.format(start_index),
                               'numberResults': '{}'.format(page_len)}}
        more_pages = True
        while more_pages:
            page = cs.get(selector)
            if parent:
                id_dict.update({x['id']: {'name': x['name'],
                                          'parent': x[parent_resp]}
                                for x in page['entries'] if 'entries' in page})
            else:
                id_dict.update({x['id']: {'name': x['name']}
                                for x in page['entries'] if 'entries' in page})
            start_index += page_len
            selector['paging']['startIndex'] = str(start_index)
            more_pages = start_index < int(page['totalNumEntries'])
        return id_dict

    def set_budget(self, name, budget, method):
        bs = self.adwords_client.GetService('BudgetService', version=self.v)
        budget = {
            'name': '{}-{}'.format(name, uuid.uuid4()),
            'amount': {
                'microAmount': '{}'.format(budget * 1000000)
            },
            'deliveryMethod': '{}'.format(method)
        }
        budget_operations = self.get_operation([budget])
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
        operations = self.get_operation([operand])
        campaigns = cs.mutate(operations)
        return campaigns

    def create_adgroup(self, name, campaign_name, status, bid_type, bid,
                       keywords):
        ags = self.adwords_client.GetService('AdGroupService', version=self.v)
        if not self.cam_dict:
            self.cam_dict = self.get_id_dict(service='CampaignService')
        bids = [{'xsi_type': bid_type,
                 'bid': {'microAmount': '{}'.format(bid * 1000000)}, }]
        cid = [k for k, v in self.cam_dict.items()
               if v['name'] == campaign_name]
        operand = {
          'campaignId': '{}'.format(cid[0]),
          'name': '{}'.format(name),
          'status': '{}'.format(status),
          'biddingStrategyConfiguration': {
              'bids': bids
          }
        }
        operations = self.get_operation([operand])
        ad_groups = ags.mutate(operations)
        ag_id = ad_groups['value'][0]['id']
        self.add_keywords_to_addgroups(ag_id, keywords)
        return ad_groups

    def add_keywords_to_addgroups(self, ag_id, keywords):
        agcs = self.adwords_client.GetService('AdGroupCriterionService',
                                              version=self.v)
        keywords = [{'xsi_type': 'BiddableAdGroupCriterion',
                     'adGroupId': ag_id, 'criterion': x} for x in keywords]
        operations = self.get_operation(keywords)
        ad_group_criteria = agcs.mutate(operations)
        return ad_group_criteria

    def create_ad(self, adgroup_name, campaign_name, ad_type, headline1,
                  headline2, headline3, description, description2, final_url,
                  track_url):
        ags = self.adwords_client.GetService('AdGroupAdService',
                                             version=self.v)
        if not self.cam_dict:
            self.cam_dict = self.get_id_dict(service='CampaignService')
        if not self.ag_dict:
            self.ag_dict = self.get_id_dict(service='AdGroupService',
                                            parent='CampaignId',
                                            parent_resp='campaignId')
        cid = [k for k, v in self.cam_dict.items()
               if v['name'] == campaign_name]
        agid = [k for k, v in self.ag_dict.items()
                if v['name'] == adgroup_name and v['parent'] == cid[0]][0]
        operand = {
            'xsi_type': 'AdGroupAd',
            'adGroupId': int(agid),
            'ad': {
                'xsi_type': '{}'.format(ad_type),
                'headlinePart1': '{}'.format(headline1),
                'headlinePart2': '{}'.format(headline2),
                'headlinePart3': '{}'.format(headline3),
                'description': '{}'.format(description),
                'description2': '{}'.format(description2),
                'finalUrls': ['{}'.format(final_url)],
                'trackingUrlTemplate': '{}'.format(track_url)
            },
        }
        operations = self.get_operation([operand])
        ads = ags.mutate(operations)
        return ads


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
            logging.info('Uploading campaign {} of {}.  '
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
        self.target = 'target'
        self.config = None
        self.ag_name = None
        self.ag_cam_name = None
        self.ag_status = None
        self.ag_bid_type = None
        self.ag_bid_val = None
        self.ag_target = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_adgroup_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        target_config = self.load_targets()
        df[self.target] = df[self.target].map(target_config)
        self.config = df.set_index(self.name).to_dict(orient='index')
        for adgroup in self.config:
            self.config[adgroup][self.target] =\
                [{'xsi_type': 'Keyword', 'matchType': x[0], 'text': x[1]}
                 for x in self.config[adgroup]['target'] if x != ['']]

    @staticmethod
    def load_targets(target_file='aw_adgroup_target_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, target_file))
        df = df.fillna('')
        for col in df:
            df[col] = 'BROAD|' + df[col]
            df[col] = np.where(df[col].str.contains("[", regex=False),
                               df[col].str.replace('BROAD|', 'EXACT|',
                                                   regex=False), df[col])
            df[col] = np.where(df[col].str.contains('"', regex=False),
                               df[col].str.replace('BROAD|', 'PHRASE|',
                                                   regex=False), df[col])
            df[col] = df[col].str.replace('[', '')
            df[col] = df[col].str.replace(']', '')
            df[col] = df[col].str.replace('"', '')
            df[col] = df[col].replace('BROAD|', '', regex=False)
            df[col] = df[col].str.split('|')
        target_config = df.to_dict(orient='list')
        return target_config

    def set_adgroup(self, adgroup):
        self.ag_name = adgroup
        self.ag_cam_name = self.config[adgroup][self.cam_name]
        self.ag_bid_val = self.config[adgroup][self.status]
        self.ag_bid_type = self.config[adgroup][self.bid_type]
        self.ag_bid_val = self.config[adgroup][self.bid_val]
        self.ag_target = self.config[adgroup][self.target]

    def upload_all_adgroups(self, api):
        total_ag = str(len(self.config))
        for idx, ag in enumerate(self.config):
            logging.info('Uploading adgroup {} of {}.  '
                         'Adgroup Name: {}'.format(idx + 1, total_ag, ag))
            self.upload_adgroup(api, ag)
        logging.info('Pausing for 30s while ad groups finish uploading.')
        time.sleep(30)

    def upload_adgroup(self, api, adgroup):
        self.set_adgroup(adgroup)
        api.create_adgroup(adgroup, self.ag_cam_name, self.ag_status,
                           self.ag_bid_type, self.ag_bid_val, self.ag_target)


class AdUpload(object):
    def __init__(self, config_file=None):
        self.config_file = config_file
        self.ag_name = 'adGroupName'
        self.cam_name = 'campaignName'
        self.type = 'adType'
        self.headline1 = 'headlinePart1'
        self.headline2 = 'headlinePart2'
        self.headline3 = 'headlinePart3'
        self.description = 'description'
        self.description2 = 'description2'
        self.final_url = 'finalUrls'
        self.track_url = 'trackingUrlTemplate'
        self.config = None
        self.ad_ag_name = None
        self.ad_cam_name = None
        self.ad_type = None
        self.ad_headline1 = None
        self.ad_headline2 = None
        self.ad_headline3 = None
        self.ad_description = None
        self.ad_description2 = None
        self.ad_final_url = None
        self.ad_track_url = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_ad_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.ag_name])
        df = df.fillna('')
        df = self.check_urls(df)
        self.config = df.to_dict(orient='index')

    def set_ad(self, adgroup):
        self.ad_ag_name = self.config[adgroup][self.ag_name]
        self.ad_cam_name = self.config[adgroup][self.cam_name]
        self.ad_type = self.config[adgroup][self.type]
        self.ad_headline1 = self.config[adgroup][self.headline1]
        self.ad_headline2 = self.config[adgroup][self.headline2]
        self.ad_headline3 = self.config[adgroup][self.headline3]
        self.ad_description = self.config[adgroup][self.description]
        self.ad_description2 = self.config[adgroup][self.description2]
        self.ad_final_url = self.config[adgroup][self.final_url]
        self.ad_track_url = self.config[adgroup][self.track_url]

    def check_urls(self, df):
        for col in [self.final_url, self.track_url]:
            df[col] = np.where(df[col][:4] != 'http',
                               'http://' + df[col], df[col])
        return df

    def upload_all_ads(self, api):
        total_ad = str(len(self.config))
        for idx, ad in enumerate(self.config):
            logging.info('Uploading ad {} of {}.  '
                         'Ad Row: {}'.format(idx + 1, total_ad, ad + 2))
            self.upload_ad(api, ad)
        logging.info('Pausing for 30s while ad groups finish uploading.')
        time.sleep(30)

    def upload_ad(self, api, ad):
        self.set_ad(ad)
        api.create_ad(self.ad_ag_name, self.ad_cam_name, self.ad_type,
                      self.ad_headline1, self.ad_headline2, self.ad_headline3,
                      self.ad_description, self.ad_description2,
                      self.ad_final_url, self.ad_track_url)
