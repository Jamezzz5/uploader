import sys
import logging
import argparse
import uploader.fbapi as fbapi
import uploader.awapi as awapi
import uploader.creator as cre

formatter = logging.Formatter('%(asctime)s [%(module)14s]' +
                              '[%(levelname)8s] %(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

console = logging.StreamHandler(sys.stdout)
console.setFormatter(formatter)
log.addHandler(console)

file = logging.FileHandler('logfile.log', mode='w')
file.setFormatter(formatter)
log.addHandler(file)

parser = argparse.ArgumentParser()
parser.add_argument('--create', action='store_true')
parser.add_argument('--api', choices=['all', 'fb', 'aw'])
parser.add_argument('--upload', choices=['all', 'c', 'as', 'ad'])
args = parser.parse_args()


def main():
    if args.create:
        crc = cre.CreatorConfig('creator_config.xlsx')
        crc.do_all()
    if args.api =='all' or args.api == 'fb':
        api = fbapi.FbApi(config_file='fbconfig.json')
        if args.upload == 'all' or args.upload == 'c':
            cu = fbapi.CampaignUpload(config_file='campaign_upload.xlsx')
            cu.upload_all_campaigns(api=api)
        if args.upload == 'all' or args.upload == 'as':
            asu = fbapi.AdSetUpload(config_file='adset_upload.xlsx')
            asu.upload_all_adsets(api=api)
        if args.upload == 'all' or args.upload == 'ad':
            ctv = fbapi.Creative(creative_file='creative_hashes.csv')
            adu = fbapi.AdUpload(config_file='ad_upload.xlsx')
            adu.upload_all_ads(api, ctv)
    if args.api =='all' or args.api == 'aw':
        api = awapi.AwApi(config_file='awconfig.yaml')
        if args.upload == 'all' or args.upload == 'c':
            cu = awapi.CampaignUpload(config_file='aw_campaign_upload.xlsx')
            cu.upload_all_campaigns(api)

if __name__ == '__main__':
    main()
