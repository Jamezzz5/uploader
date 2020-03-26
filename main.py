import sys
import logging
import argparse
import upload.creator as cre
import upload.fbapi as fbapi
import upload.awapi as awapi
import upload.dcapi as dcapi
import upload.szkapi as szkapi


def set_log():
    formatter = logging.Formatter('%(asctime)s [%(module)14s]'
                                  '[%(levelname)8s] %(message)s')
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    log.addHandler(console)

    try:
        log_file = logging.FileHandler('logfile.log', mode='w')
        log_file.setFormatter(formatter)
        log.addHandler(log_file)
    except PermissionError as e:
        logging.warning('Could not open logfile with error: \n {}'.format(e))


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.critical("Uncaught exception: ",
                     exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = handle_exception


def get_args(arguments=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--create', action='store_true')
    parser.add_argument('--api', choices=['all', 'fb', 'aw', 'szk', 'dcm'])
    parser.add_argument('--upload', choices=['all', 'c', 'as', 'ad'])
    if arguments:
        args = parser.parse_args(arguments.split())
    else:
        args = parser.parse_args()
    return args


def main(arguments=None):
    set_log()
    args = get_args(arguments)
    if args.create:
        crc = cre.CreatorConfig('create/creator_config.xlsx')
        crc.do_all()
    if args.api == 'all' or args.api == 'fb':
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
    if args.api == 'all' or args.api == 'aw':
        api = awapi.AwApi(config_file='awconfig.yaml')
        if args.upload == 'all' or args.upload == 'c':
            cu = awapi.CampaignUpload(config_file='aw_campaign_upload.xlsx')
            cu.upload_all_campaigns(api)
        if args.upload == 'all' or args.upload == 'as':
            agu = awapi.AdGroupUpload(config_file='aw_adgroup_upload.xlsx')
            agu.upload_all_adgroups(api)
        if args.upload == 'all' or args.upload == 'ad':
            adu = awapi.AdUpload(config_file='aw_ad_upload.xlsx')
            adu.upload_all_ads(api)
    if args.api == 'all' or args.api == 'szk':
        api = szkapi.SzkApi(config_file='szkconfig.json')
        if args.upload == 'all' or args.upload == 'c':
            cu = szkapi.CampaignUpload(config_file='szk_campaign_upload.xlsx')
            cu.upload_all_campaigns(api)
    if args.api == 'all' or args.api == 'dcm':
        api = dcapi.DcApi(config_file='dcapi.json')
        if args.upload == 'all' or args.upload == 'c':
            cu = dcapi.CampaignUpload(config_file='campaign_upload.xlsx')
            cu.upload_all_campaigns(api)
        if args.upload == 'all' or args.upload == 'as':
            pu = dcapi.PlacementUpload(config_file='placement_upload.xlsx')
            pu.upload_all_placements(api)


if __name__ == '__main__':
    main()
