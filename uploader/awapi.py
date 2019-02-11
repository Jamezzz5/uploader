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

    def mutate_service(self, service, operator):
        svc = self.get_service(service)
        operation = self.get_operation(operator)
        resp = svc.mutate(operation)
        return resp

    def get_service(self, service):
        svc = self.adwords_client.GetService(service, version=self.v)
        return svc

    def get_id_dict(self, service='CampaignService', parent=None,
                    parent_resp=None, page_len=100):
        svc = self.get_service(service)
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
            page = svc.get(selector)
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
        budget = {
            'name': '{}-{}'.format(name, uuid.uuid4()),
            'amount': {
                'microAmount': '{}'.format(budget * 1000000)
            },
            'deliveryMethod': '{}'.format(method)
        }
        resp = self.mutate_service('BudgetService', [budget])
        budget_id = resp['value'][0]['budgetId']
        return budget_id

    def set_id_dicts(self):
        self.cam_dict = self.get_id_dict(service='CampaignService')
        self.ag_dict = self.get_id_dict(service='AdGroupService',
                                        parent='CampaignId',
                                        parent_resp='campaignId')

    def get_id(self, dict_o, match, dict_two=None, match_two=None, ):
        self.set_id_dicts()
        id_list = [k for k, v in dict_o.items() if v['name'] == match]
        if dict_two:
            id_list = [k for k, v in dict_two.items() if v['name'] == match_two
                       and v['parent'] == id_list[0]]
        return id_list

    def create_campaign(self, name, status, sd, ed, budget, method, freq,
                        channel, channel_sub, network, strategy, settings):
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
        campaigns = self.mutate_service('CampaignService', [operand])
        return campaigns

    def create_adgroup(self, name, campaign_name, status, bid_type, bid,
                       keywords):
        bids = [{'xsi_type': bid_type,
                 'bid': {'microAmount': '{}'.format(bid * 1000000)}, }]
        cid = self.get_id(self.cam_dict, campaign_name)
        operand = {
          'campaignId': '{}'.format(cid[0]),
          'name': '{}'.format(name),
          'status': '{}'.format(status),
          'biddingStrategyConfiguration': {
              'bids': bids
          }
        }
        ad_groups = self.mutate_service('AdGroupService', [operand])
        ag_id = ad_groups['value'][0]['id']
        self.add_keywords_to_addgroups(ag_id, keywords)
        return ad_groups

    def add_keywords_to_addgroups(self, ag_id, keywords):
        keywords = [{'xsi_type': 'BiddableAdGroupCriterion',
                     'adGroupId': ag_id, 'criterion': x} for x in keywords]
        ad_group_criteria = self.mutate_service('AdGroupCriterionService',
                                                keywords)
        return ad_group_criteria

    def create_ad(self, ad):
        agid = self.get_id(self.cam_dict, ad.campaignName,
                           self.ag_dict, ad.AdGroupName)
        operand = {
            'xsi_type': 'AdGroupAd',
            'adGroupId': int(agid[0]),
            'ad': ad.ad_dict,
        }
        ads = self.mutate_service('AdGroupAdService', [operand])
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
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_ad_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.ag_name])
        df = df.fillna('')
        df = self.check_urls(df)
        self.config = df.to_dict(orient='index')

    def set_ad(self, ad_id):
        ad = Ad(self.config[ad_id])
        return ad

    def check_urls(self, df):
        for col in [self.final_url, self.track_url]:
            df[col] = np.where(df[col][:4] != 'http',
                               'http://' + df[col], df[col])
        return df

    def upload_all_ads(self, api):
        total_ad = str(len(self.config))
        for idx, ad_id in enumerate(self.config):
            logging.info('Uploading ad {} of {}.  '
                         'Ad Row: {}'.format(idx + 1, total_ad, ad_id + 2))
            self.upload_ad(api, ad_id)
        logging.info('Pausing for 30s while ad groups finish uploading.')
        time.sleep(30)

    def upload_ad(self, api, ad_id):
        ad = self.set_ad(ad_id)
        api.create_ad(ad)


class Ad(object):
    __slots__ = ['adGroupName', 'campaignName', 'adType', 'headlinePart1',
                 'headlinePart2', 'headlinePart3', 'description',
                 'description2', 'finalUrls', 'trackingUrlTemplate', 'ad_dict']

    def __init__(self, ad_dict):
        for k in ad_dict:
            setattr(self, k, ad_dict[k])
        self.ad_dict = self.create_ad_dict()

    def create_ad_dict(self):
        ad_dict = {
                'xsi_type': '{}'.format(self.adType),
                'finalUrls': ['{}'.format(self.finalUrls)],
                'trackingUrlTemplate': '{}'.format(self.trackingUrlTemplate)
        }
        if self.adType == 'ExpandedTextAd':
            ad_dict['headlinePart1'] = '{}'.format(self.headlinePart1)
            ad_dict['headlinePart2'] = '{}'.format(self.headlinePart2)
            ad_dict['description'] = '{}'.format(self.description)
            if self.headlinePart3:
                ad_dict['headlinePart3'] = '{}'.format(self.headlinePart3)
            if self.description2:
                ad_dict['description2'] = '{}'.format(self.description2)
        return ad_dict
