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
        self.ad_dict = {}
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

    def get_id_dict(self, service='CampaignService', parent=None, page_len=100,
                    fields=None, nest=None):
        svc = self.get_service(service)
        id_dict = {}
        start_index = 0
        selector_fields = ['Id', 'Status']
        [selector_fields.extend(list(x.keys())) for x in [fields, parent] if x]
        selector = {'fields': selector_fields,
                    'paging': {'startIndex': '{}'.format(start_index),
                               'numberResults': '{}'.format(page_len)}}
        more_pages = True
        while more_pages:
            page = svc.get(selector)
            id_dict = self.get_dict_from_page(id_dict, page,
                                              list(parent.values())[0],
                                              list(fields.values()), nest)
            start_index += page_len
            selector['paging']['startIndex'] = str(start_index)
            more_pages = start_index < int(page['totalNumEntries'])
        return id_dict

    @staticmethod
    def get_dict_from_page(id_dict, page, parent, fields=None, nest=None):
        resp_fields = [parent]
        if fields:
            resp_fields += fields
        if nest:
            id_dict.update({x[nest]['id']: {'parent' if y == parent else y:
                                            x[nest][y] if y in x[nest] else
                                            x[y] for y in resp_fields}
                            for x in page['entries'] if 'entries' in page})
        else:
            id_dict.update({x['id']: {'parent' if y == parent else y:
                                      x[y] for y in resp_fields}
                            for x in page['entries'] if 'entries' in page})
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

    def get_campaign_id_dict(self):
        parent = {'BaseCampaignId': 'baseCampaignId'}
        fields = {'Name': 'name'}
        cam_dict = self.get_id_dict(service='CampaignService', parent=parent,
                                    fields=fields)
        return cam_dict

    def get_adgroup_id_dict(self):
        parent = {'CampaignId': 'campaignId'}
        fields = {'Name': 'name'}
        ag_dict = self.get_id_dict(service='AdGroupService', parent=parent,
                                   fields=fields)
        return ag_dict

    def get_ad_dict(self):
        parent = {'AdGroupId': 'adGroupId'}
        fields = {'HeadlinePart1': 'headlinePart1', 'UrlData': 'urlData',
                  'HeadlinePart2': 'headlinePart2',
                  'Description': 'description',
                  'ExpandedTextAdHeadlinePart3': 'headlinePart3',
                  'ExpandedTextAdDescription2': 'description2',
                  'CreativeTrackingUrlTemplate': 'trackingUrlTemplate',
                  'CreativeFinalUrls': 'finalUrls', 'DisplayUrl': 'displayUrl'}
        ad_dict = self.get_id_dict(service='AdGroupAdService',
                                   parent=parent, fields=fields, nest='ad')
        return ad_dict

    def set_id_dict(self, aw_object='all'):
        if aw_object in ['campaign', 'adgroup', 'ad', 'all']:
            self.cam_dict = self.get_campaign_id_dict()
        if aw_object in ['adgroup', 'ad', 'all']:
            self.ag_dict = self.get_adgroup_id_dict()
        if aw_object in ['ad', 'all']:
            self.ad_dict = self.get_ad_dict()

    @staticmethod
    def get_id(dict_o, match, dict_two=None, match_two=None, parent_id=None):
        if parent_id:
            id_list = [k for k, v in dict_o.items() if v['name'] == match
                       and v['parent'] == parent_id]
        else:
            id_list = [k for k, v in dict_o.items() if v['name'] == match]
        if dict_two is not None:
            id_list = [k for k, v in dict_two.items() if v['name'] == match_two
                       and v['parent'] == id_list[0]]
        return id_list

    def check_exists(self, name, aw_object, object_dict, parent_id=None):
        if not object_dict:
            self.set_id_dict(aw_object)
        if self.get_id(object_dict, name, parent_id):
            logging.warning('{} already in account.  '
                            'This {} was not uploaded.'.format(name, aw_object))
            return True

    def create_campaign(self, campaign):
        budget_id = self.set_budget(campaign.name, campaign.budget,
                                    campaign.deliveryMethod)
        campaign.cam_dict['budget'] = {
                'budgetId': budget_id
            }
        campaigns = self.mutate_service('CampaignService', [campaign.cam_dict])
        return campaigns

    def create_adgroup(self, ag):
        ag.cid = self.get_id(self.cam_dict, ag.campaignName)[0]
        operand = ag.ag_dict
        operand['campaignId'] = '{}'.format(ag.cid)
        ad_groups = self.mutate_service('AdGroupService', [operand])
        ag.id = ad_groups['value'][0]['id']
        self.add_keywords_to_addgroups(ag)
        return ad_groups

    def add_keywords_to_addgroups(self, ag):
        keywords = [{'xsi_type': 'BiddableAdGroupCriterion', 'adGroupId': ag.id,
                     'criterion': x} for x in ag.target_dict]
        ad_group_criteria = self.mutate_service('AdGroupCriterionService',
                                                keywords)
        return ad_group_criteria

    def create_ad(self, ad):
        agid = self.get_id(self.cam_dict, ad.campaignName,
                           self.ag_dict, ad.adGroupName)
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
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_campaign_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        for col in [self.sd, self.ed]:
            df[col] = df[col].dt.strftime('%Y%m%d')
        self.config = df.to_dict(orient='index')
        for k in self.config:
            for item in [self.freq, self.network, self.strategy]:
                self.config[k][item] = self.config[k][item].split('|')

    def set_campaign(self, campaign):
        cam = Campaign(self.config[campaign])
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
        campaign = self.set_campaign(campaign_id)
        if not campaign.check_exists(api):
            api.create_campaign(campaign)


class Campaign(object):
    __slots__ = ['name', 'status', 'startDate', 'endDate', 'budget',
                 'deliveryMethod', 'frequencyCap', 'advertisingChannelType',
                 'advertisingChannelSubType', 'networkSetting',
                 'biddingStrategy', 'settings', 'cam_dict']

    def __init__(self, cam_dict):
        for k in cam_dict:
            setattr(self, k, cam_dict[k])
        self.frequencyCap = self.set_freq(self.frequencyCap)
        self.networkSetting = self.set_net(self.networkSetting)
        self.biddingStrategy = self.set_strat(self.biddingStrategy)
        self.cam_dict = self.create_cam_dict()

    def create_cam_dict(self):
        cam_dict = {
            'name': '{}'.format(self.name),
            'status': '{}'.format(self.status),
            'advertisingChannelType': '{}'.format(self.advertisingChannelType),
            'biddingStrategyConfiguration': self.biddingStrategy,
            'endDate': '{}'.format(self.endDate),
            'networkSetting': self.networkSetting,
        }
        if self.startDate:
            cam_dict['startDate'] = '{}'.format(self.startDate)
        if self.frequencyCap:
            cam_dict['frequencyCap'] = self.frequencyCap
        if self.settings:
            cam_dict['settings'] = self.settings
        if self.advertisingChannelSubType:
            cam_dict['advertisingChannelSubType'] =\
                '{}'.format(self.advertisingChannelSubType)
        return cam_dict

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

    def check_exists(self, api):
        if not api.cam_dict:
            api.set_id_dict('campaign')
        cid = api.get_id(api.cam_dict, self.name)
        if cid:
            logging.warning('{} already in account.  '
                            'This was not uploaded.'.format(self.name))
            return True


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
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file='aw_adgroup_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, config_file))
        df = df.dropna(subset=[self.name])
        df = df.fillna('')
        target_config = self.load_targets()
        df[self.target] = df[self.target].map(target_config)
        self.config = df.to_dict(orient='index')

    @staticmethod
    def load_targets(target_file='aw_adgroup_target_upload.xlsx'):
        df = pd.read_excel(os.path.join(config_path, target_file))
        df = df.fillna('')
        for col in df:
            df[col] = 'BROAD|' + df[col]
            for kw_t in [('[', 'EXACT|'), ('"', 'PHRASE|')]:
                df[col] = np.where(df[col].str.contains(kw_t[0], regex=False),
                                   df[col].str.replace('BROAD|', kw_t[1],
                                                       regex=False), df[col])
            for r in ['[', ']', '"']:
                df[col] = df[col].str.replace(r, '', regex=False)
            df[col] = df[col].replace('BROAD|', '', regex=False)
            df[col] = df[col].str.split('|')
        target_config = df.to_dict(orient='list')
        return target_config

    def set_adgroup(self, adgroup_id):
        ag = AdGroup(self.config[adgroup_id])
        return ag

    def upload_all_adgroups(self, api):
        total_ag = str(len(self.config))
        for idx, ag_id in enumerate(self.config):
            logging.info('Uploading adgroup {} of {}.  '
                         'Adgroup Name: {}'.format(idx + 1, total_ag, ag_id))
            self.upload_adgroup(api, ag_id)
        logging.info('Pausing for 30s while ad groups finish uploading.')
        time.sleep(30)

    def upload_adgroup(self, api, ag_id):
        ag = self.set_adgroup(ag_id)
        if not ag.check_exists(api):
            api.create_adgroup(ag)


class AdGroup(object):
    __slots__ = ['name', 'campaignName', 'status', 'bidtype', 'bid', 'target',
                 'ag_dict', 'target_dict', 'id', 'cid']

    def __init__(self, ag_dict):
        for k in ag_dict:
            setattr(self, k, ag_dict[k])
        self.ag_dict = self.create_adgroup_dict()
        self.target_dict = self.create_target_dict()

    def create_adgroup_dict(self):
        bids = [{'xsi_type': self.bidtype,
                 'bid': {'microAmount': '{}'.format(self.bid * 1000000)}, }]
        ag_dict = {
          'name': '{}'.format(self.name),
          'status': '{}'.format(self.status),
          'biddingStrategyConfiguration': {
              'bids': bids
          }
        }
        return ag_dict

    def create_target_dict(self):
        target = [{'xsi_type': 'Keyword', 'matchType': x[0], 'text': x[1]}
                  for x in self.target if x != ['']]
        return target

    def check_exists(self, api):
        if not api.ag_dict:
            api.set_id_dict('adgroup')
        ag_id = api.get_id(api.cam_dict, self.campaignName,
                           api.ag_dict, self.name)
        if ag_id:
            logging.warning('{} already in account.  '
                            'This was not uploaded.'.format(self.name))
            return True


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
        logging.info('Pausing for 30s while ads finish uploading.')
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

    def check_exists(self, api):
        if not api.ad_dict:
            api.set_id_dict('all')
        ag_id = api.get_id(api.cam_dict, self.campaignName,
                           api.ag_dict, self.adGroupName)
        if ag_id:
            logging.warning('{} already in account.  '
                            'This was not uploaded.'.format('ad'))
            return True
