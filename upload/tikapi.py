"""TikTok Business API uploader. Mirrors the redditapi class shape so
the relation system, name-create flow, and run telemetry pick it up
without special-casing. Campaign, AdGroup (Adset) and Ad levels.

Auth is a static Access-Token header (no OAuth refresh dance) shared
with the processor's read client — the config keys match the
processor's tikapi.json so token sharing is a straight copy. Every
response arrives HTTP 200 wrapped in ``{code, message, data}``; only
``code == 0`` is success, so the envelope is checked on every call.

Field names and the enum defaults below come from TikTok's v1.3 spec
(the generated ``tiktok-business-api-sdk`` model docs), confirmed
against a live ad account — see the ``OBSERVED`` comments. Values the
plan supplies pass through verbatim rather than being mapped onto a
guessed vocabulary; only the defaults are spelled here, each one a
value the live account actually uses."""
import hashlib
import io
import itertools
import json
import logging
import os
import sys
import time
import unicodedata
from urllib.parse import urlparse

import requests
from urllib3 import filepost
from urllib3.fields import RequestField

import uploader.upload.utils as utl


tik_path = 'tik'
config_path = os.path.join(utl.config_file_path, tik_path)
base_url = 'https://business-api.tiktok.com/open_api'
api_version = 'v1.3'
CHANNEL = 'TikTok'

REQUEST_TIMEOUT = (10, 30)
UPLOAD_TIMEOUT = (10, 300)
MAX_LIST_PAGES = 100

REGION_LEVEL_RANGES = ('TO_COUNTRY', 'TO_PROVINCE', 'TO_CITY',
                       'TO_DISTRICT')
LIST_PAGE_SIZE = 100

OBJECTIVE_DEFAULT = 'TRAFFIC'
STATUS_DEFAULT = 'DISABLE'
BUDGET_MODE_DEFAULT = 'BUDGET_MODE_TOTAL'

BILLING_EVENT_DEFAULT = 'CPC'
OPTIMIZATION_GOAL_DEFAULT = 'CLICK'
OPTIMIZATION_GOAL_REACH = 'REACH'
FREQUENCY_DEFAULT = 3
FREQUENCY_SCHEDULE_DEFAULT = 7
PACING_DEFAULT = 'PACING_MODE_SMOOTH'
ADGROUP_BUDGET_MODE_DEFAULT = 'BUDGET_MODE_DAY'
PLACEMENT_TYPE_DEFAULT = 'PLACEMENT_TYPE_NORMAL'
PLACEMENTS_DEFAULT = ['PLACEMENT_TIKTOK']
SCHEDULE_START_END = 'SCHEDULE_START_END'
SCHEDULE_FROM_NOW = 'SCHEDULE_FROM_NOW'

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

AD_FORMAT_VIDEO = 'SINGLE_VIDEO'
AD_FORMAT_IMAGE = 'SINGLE_IMAGE'
AD_FORMAT_CAROUSEL = 'CAROUSEL_ADS'
AD_FORMATS = (AD_FORMAT_VIDEO, AD_FORMAT_IMAGE, AD_FORMAT_CAROUSEL)
AD_ROW_ERRORS = (ValueError, OSError, requests.RequestException)
UPLOAD_TYPE_FILE = 'UPLOAD_BY_FILE'
IDENTITY_LINKED = 'TT_USER'
IDENTITY_CUSTOM = 'CUSTOMIZED_USER'
IDENTITY_AUTHORIZED = 'AUTH_CODE'
IDENTITY_BUSINESS = 'BC_AUTH_TT'
IDENTITY_LABELS = {
    IDENTITY_LINKED: 'Linked TikTok Account',
    IDENTITY_CUSTOM: 'Custom User (Pangle/App Bundle)',
    IDENTITY_AUTHORIZED: 'Authorized Account',
    IDENTITY_BUSINESS: 'Business Center Account',
}
HASHTAG_SCENE = 'HASHTAG_RELATED'
HASHTAG_ACTION = 'VIEW_HASHTAG'
HASHTAG_BATCH = 50
VIDEO_EXTENSIONS = frozenset(('.mp4', '.mov', '.mpeg', '.avi'))
IMAGE_EXTENSIONS = frozenset(('.jpg', '.jpeg', '.png'))
VIDEO_READY_ATTEMPTS = 11
VIDEO_READY_INTERVAL = 30
VIDEO_MAX_BYTES = 500 * 1024 * 1024
IMAGE_MAX_BYTES = 50 * 1024 * 1024
IMAGE_MAX_PIXELS = 3686400
IMAGE_PLACEMENTS = frozenset((
    'PLACEMENT_PANGLE', 'PLACEMENT_GLOBAL_APP_BUNDLE'))
EMOJI_START = 0x2190


def hashtag_key(value):
    """A hashtag as TikTok's suggestions are compared: no ``#``,
    casefolded."""
    return str(value or '').strip().lstrip('#').casefold()


def hashtag_actions(ids):
    """The ``actions`` list reaching people who viewed hashtags ``ids``,
    in the shape TikTok's own ad groups carry; ``[]`` for none."""
    if not ids:
        return []
    return [{'action_scene': HASHTAG_SCENE, 'action_period': 0,
             'video_user_actions': [HASHTAG_ACTION],
             'action_category_ids': [str(x) for x in ids]}]


def live_hashtag_ids(actions):
    """The hashtag ids a live ad group's ``actions`` target; creator and
    video behaviours are not hashtags and are left out."""
    return [str(tag) for action in actions or []
            if action.get('action_scene') == HASHTAG_SCENE
            for tag in action.get('action_category_ids') or []]


def unknown_hashtags_message(names):
    """Why a row naming hashtags TikTok does not offer was refused."""
    return (f'TikTok has no hashtag to target named {", ".join(names)}. '
            'Copy an ad group that uses it from TikTok, or enter its '
            'hashtag id.')


def identity_values(value, identity_type='', business_center=''):
    """Decode a picker token; plain IDs retain explicit row settings."""
    parts = str(value or '').split('::')
    if len(parts) == 3:
        return parts[1], parts[0], parts[2]
    return str(value or ''), identity_type, business_center


def _cell(value):
    text = str(value or '').strip()
    return '' if text.lower() in ('nan', 'none', '_') else text


def _text_errors(label, value, maximum):
    """What TikTok would refuse in one text field: wide characters count
    double, and emoji start above marks such as (R) and TM."""
    text = str(value or '')
    length = sum(2 if unicodedata.east_asian_width(char) in 'WF' else 1
                 for char in text)
    errors = []
    if length > maximum:
        errors.append(f'Shorten {label} to {maximum} characters.')
    if any(unicodedata.category(char) == 'So' and ord(char) >= EMOJI_START
           for char in text):
        errors.append(f'Remove emoji from {label}.')
    return errors


def _media_rows(response):
    body = utl.response_body(response)
    error = _extract_error(body)
    if error:
        raise ValueError(error.get('message') or 'TikTok refused the media.')
    data = body.get('data')
    if isinstance(data, dict):
        data = data.get('list', [data])
    if not isinstance(data, list):
        raise ValueError('TikTok returned unreadable media information.')
    return data


def _dimensions(row):
    try:
        return int(row.get('width') or 0), int(row.get('height') or 0)
    except (TypeError, ValueError):
        return 0, 0


def _media_error(row, is_video=False):
    """Why TikTok cannot use this media, or ''. Only a video's
    ``displayable`` False refuses: images read False even when ads run."""
    width, height = _dimensions(row)
    if width <= 0 or height <= 0:
        return 'TikTok has not supplied media dimensions. Try again later.'
    if is_video:
        if row.get('displayable') is False:
            return 'TikTok cannot display this creative. Choose another file.'
        kind = row.get('format')
        if kind and f'.{kind.lower()}' not in VIDEO_EXTENSIONS:
            return 'Use an MP4, MOV, MPEG or AVI video.'
        if int(row.get('size') or 0) > VIDEO_MAX_BYTES:
            return 'Choose a video smaller than 500 MB.'
    elif width * height > IMAGE_MAX_PIXELS:
        return 'Choose a smaller image, up to about 1920 x 1920.'
    return ''


def _video_ready(row, require_cover):
    return (all(_dimensions(row)) and row.get('displayable') is not False
            and (not require_cover or bool(row.get('video_cover_url'))))


# "no id dict built yet" — distinct from the '' scope an unfiltered
# (campaign-level) read legitimately records.
_UNSCOPED = object()


def _api_url(endpoint):
    """Full v1.3 url for an endpoint path like ``/campaign/create/``."""
    return '{}/{}{}'.format(base_url, api_version, endpoint)


class _MultipartFile:
    """Stream multipart files in bounded writes without buffering them."""

    def __init__(self, fields, field, file_name, path):
        boundary = filepost.choose_boundary()
        part = RequestField(field, b'', filename=file_name)
        part.make_multipart()
        empty, self.content_type = filepost.encode_multipart_formdata(
            [*fields.items(), part], boundary=boundary)
        tail = f'\r\n--{boundary}--\r\n'.encode()
        self.length = len(empty) + os.path.getsize(path)
        self.parts = [io.BytesIO(empty[:-len(tail)]), open(path, 'rb'),
                      io.BytesIO(tail)]

    def __len__(self):
        """The Content-Length requests sends ahead of the body."""
        return self.length

    def read(self, size=-1):
        """Up to ``size`` bytes, all of them for a negative size; ``b''``
        once the body has been read."""
        blocks = []
        while self.parts and size:
            block = self.parts[0].read(size)
            if not block:
                self.parts.pop(0).close()
                continue
            blocks.append(block)
            if size > 0:
                size -= len(block)
        return b''.join(blocks)

    def close(self):
        """Release the file when a send stopped before its end."""
        while self.parts:
            self.parts.pop().close()


def _to_budget(value):
    """TikTok budgets are plain currency floats — no micro/cent
    scaling (unlike Reddit/FB). None for blank/non-numeric values so
    the field is omitted rather than sent malformed."""
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _to_datetime(value, end_of_day=False, timezone=utl.FLIGHT_TIMEZONE):
    """Plan-derived flight dates (MM/DD/YYYY strings or excel
    datetimes) -> TikTok's ``'%Y-%m-%d %H:%M:%S'`` in UTC, which is how
    TikTok reads a schedule timestamp. The cell is read in ``timezone``,
    the advertiser's, so a bare date starts at the account's midnight;
    an end date is day bounded the way fbapi's update path bounds it,
    so it means the end *of* that day rather than midnight at its start.

    :param value: raw spreadsheet cell
    :param end_of_day: bound a midnight timestamp to 23:59:59
    :param timezone: the advertiser's timezone name
    :returns: formatted UTC timestamp, or None when unparseable
    """
    if value is None or value == '':
        return None
    try:
        stamp = utl.flight_utc(value, timezone, end_of_day)
    except utl.FLIGHT_TIME_ERRORS:
        return None
    return stamp.strftime(TIME_FORMAT)


def _account_timezone(api):
    """The advertiser timezone ``api`` can read; the Pacific default
    for an api without the read, such as a test stub."""
    read = getattr(api, 'advertiser_timezone', None)
    return read() if read else utl.FLIGHT_TIMEZONE


def _platform_id(data, id_key):
    """Platform id out of a create response's ``data`` block.

    The single-object creates answer with a scalar (``campaign_id``,
    ``adgroup_id``), but ``/ad/create/`` takes a ``creatives`` list and
    so answers with the plural ``ad_ids``. Accept either spelling —
    one ad per config row then reads exactly like the levels above it.

    :param data: the response's ``data`` block
    :param id_key: singular id field, e.g. ``'adgroup_id'``
    :returns: the id as a string, or None
    """
    if not isinstance(data, dict):
        return None
    value = data.get(id_key)
    if not value:
        value = data.get(f'{id_key}s')
        if isinstance(value, (list, tuple)):
            value = value[0] if value else None
    return str(value) if value else None


def _to_bool(value):
    """Truthiness of a spreadsheet cell for TikTok's boolean fields —
    excel hands these over as 'TRUE'/'true'/1/True inconsistently, and
    a blank must read as unset rather than False."""
    if value is None or value == '':
        return False
    if isinstance(value, str):
        return value.strip().lower() in ('true', 'yes', '1', 'y')
    return bool(value)


def _reach_cap(cell):
    """``(optimization_goal, frequency fields)`` for a ``REACH|days|count``
    cell; TikTok refuses an uncapped Reach ad group, so a missing number
    takes the live account's own 3-per-7-days cap."""
    goal, *cap = str(cell).split('|')
    if goal != OPTIMIZATION_GOAL_REACH:
        return str(cell), {}
    days, count, *_ = [*cap, '', '']
    return goal, {
        'frequency_schedule': (int(days) if days.isdigit()
                               else FREQUENCY_SCHEDULE_DEFAULT),
        'frequency': int(count) if count.isdigit() else FREQUENCY_DEFAULT}


def _live_flag(value):
    """A live boolean as the ``TRUE``/``FALSE`` cell :func:`_to_bool`
    reads, or '' when absent so it is not copied as off."""
    if value is None or value == '':
        return ''
    return 'TRUE' if _to_bool(value) else 'FALSE'


def _extract_error(body):
    """TikTok envelopes every response as ``{code, message, data}``
    and answers HTTP 200 even on failure — the envelope code is the
    only real signal. {} on success (``code == 0``), else
    ``{'code', 'message'}``; a body without a ``code`` key is treated
    as an error so an unparseable response can't read as success."""
    if not isinstance(body, dict) or 'code' not in body:
        return {'code': None, 'message': 'Unparseable TikTok response'}
    if body.get('code') == 0:
        return {}
    return {'code': body.get('code'),
            'message': str(body.get('message') or '').strip()}


def _populate_tik_result(result, response, id_key='campaign_id'):
    """Fill ``result`` from a TikTok create response. Success:
    ``code == 0`` with ``data.campaign_id``. Failure: non-zero code +
    message; empty messages get the raw body appended — the actionable
    reason is otherwise lost."""
    body = utl.response_body(response)
    err = _extract_error(body)
    if not err:
        platform_id = _platform_id(body.get('data') or {}, id_key)
        if platform_id:
            result['platform_id'] = platform_id
            result['status'] = 'created'
            return
        err = {'code': None,
               'message': f'TikTok response missing {id_key}'}
    message = err.get('message') or ''
    http_status = getattr(response, 'status_code', '') or ''
    if not message:
        try:
            raw = json.dumps(body)[:500]
        except (TypeError, ValueError):
            raw = ''
        if raw and raw != '{}':
            message = f'TikTok Ads error (HTTP {http_status}): {raw}'
    utl.fail_result(result, message or 'Unknown error from TikTok Ads',
                    err.get('code'))
    logging.warning('TikTok create failed (HTTP %s): %s',
                    http_status, result['error_message'])


class TikApi(object):
    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        self.access_token = None
        self.advertiser_id = None
        self.interest_categories = None
        self.hashtag_ids = {}
        self.config_list = None
        self.headers = None
        self.cam_dict = {}
        self.adgroup_dict = {}
        self.ad_dict = {}
        self.id_dict_scope = {}
        self.adgroup_settings = {}
        self.media = {}
        self.media_errors = {}
        self.video_covers = {}
        self.progress_line = 'Preparing a TikTok ad'
        self.timezone = None
        self.r = None
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning(
                'TikTok config file not in vendor matrix. Aborting.')
            sys.exit(0)
        logging.info(f'Loading TikTok config file: {config}')
        self.config_file = os.path.join(config_path, config)
        self.load_config()
        self.check_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
        except IOError:
            logging.error(f'{self.config_file} not found. Aborting.')
            sys.exit(0)
        self.access_token = self.config.get('access_token', '')
        self.advertiser_id = str(self.config.get('advertiser_id', '') or '')
        self.config_list = [self.access_token, self.advertiser_id]

    def check_config(self):
        """Both keys are required for writes — unlike the processor's
        read client, where advertiser_id is optional."""
        for key in ('access_token', 'advertiser_id'):
            if not str(self.config.get(key, '') or '').strip():
                logging.warning(f'{key} not in TikTok config file. Aborting.')
                sys.exit(0)

    def set_headers(self):
        self.headers = {'Access-Token': self.access_token,
                        'Content-Type': 'application/json'}
        return self.headers

    def _post(self, url, body=None):
        self.set_headers()
        try:
            self.r = requests.post(url, headers=self.headers,
                                   json=body or {}, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.SSLError as e:
            logging.warning(f'TikTok SSLError: {e}')
            time.sleep(30)
            self.r = self._post(url, body=body)
        return self.r

    def _get(self, url, params=None):
        self.set_headers()
        return requests.get(url, headers=self.headers,
                            params=params or {}, timeout=REQUEST_TIMEOUT)

    def _post_file(self, url, data, field, file_name, path):
        """Stream an asset with its multipart boundary in Content-Type."""
        self.set_headers()
        body = _MultipartFile(data, field, file_name, path)
        headers = {'Access-Token': self.access_token,
                   'Content-Type': body.content_type}
        try:
            return requests.post(url, headers=headers, data=body,
                                 timeout=UPLOAD_TIMEOUT)
        finally:
            body.close()

    @staticmethod
    def get_id(dict_o, match, match_name='name'):
        return [k for k, v in dict_o.items() if v.get(match_name) == match]

    def get_interest_categories(self, language='en'):
        """The interest-category taxonomy as ``[{'id', 'name',
        'level'}]``, read once per client; ids are strings."""
        if self.interest_categories is not None:
            return self.interest_categories
        body = utl.response_body(self._get(
            _api_url('/tool/interest_category/'),
            params={'advertiser_id': self.advertiser_id,
                    'language': language}))
        err = _extract_error(body)
        if err:
            logging.warning('TikTok interest categories failed: %s',
                            err.get('message') or err.get('code'))
            return []
        rows = (body.get('data') or {}).get('interest_categories') or []
        self.interest_categories = [
            {'id': str(x.get('interest_category_id') or ''),
             'name': str(x.get('interest_category_name') or ''),
             'level': x.get('level')} for x in rows
            if x.get('interest_category_id')]
        return self.interest_categories

    def ad_preview(self, ad_id):
        """A shareable preview link for a launched ad, or ``''`` when
        TikTok refuses or the body is unreadable."""
        body = utl.response_body(self._post(
            _api_url('/creative/ads_preview/create/'),
            body={'advertiser_id': self.advertiser_id,
                  'preview_type': 'AD', 'ad_id': str(ad_id)}))
        if _extract_error(body):
            return ''
        return str((body.get('data') or {}).get('preview_link') or '')

    def resolve_interest_ids(self, names):
        """``{name: id}`` for interest names, ``''`` for a miss."""
        catalogue = self.get_interest_categories()
        return {name: (utl.match_name(
            catalogue, name, 'TikTok interest categories') or {}).get(
            'id', '') for name in names}

    def interest_keyword_candidates(self, keyword):
        """Return active interest keywords; raise ValueError on a refusal."""
        body = utl.response_body(self._get(
            _api_url('/tool/interest_keyword/recommend/'),
            params={'advertiser_id': self.advertiser_id,
                    'keyword': str(keyword or '').strip(),
                    'language': 'en'}))
        err = _extract_error(body)
        if err:
            raise ValueError('TikTok interest keywords: '
                             f"{err.get('message') or err.get('code')}")
        rows = (body.get('data') or {}).get('recommended_keywords') or []
        return [{'id': str(row['keyword_id']),
                 'name': str(row.get('keyword') or '')}
                for row in rows if row.get('keyword_id')
                and row.get('status', 'EFFECTIVE') == 'EFFECTIVE']

    def resolve_interest_keyword_ids(self, names):
        """``{name: id}`` for interest keywords typed by name, ``''``
        where TikTok offers none spelled exactly so or the read failed."""
        return {name: self._interest_keyword_id(
            str(name or '').strip().casefold()) for name in names}

    def _interest_keyword_id(self, wanted):
        """Return an exact name's ID, or '' for a blank, miss or refusal."""
        if not wanted:
            return ''
        try:
            rows = self.interest_keyword_candidates(wanted)
        except ValueError as e:
            logging.warning('%s', e)
            return ''
        return next((row['id'] for row in rows
                     if row['name'].casefold() == wanted), '')

    def resolve_hashtag_ids(self, names):
        """``{name: id}`` for hashtags typed by name, ``''`` where TikTok
        suggests none spelled exactly so."""
        return {name: self._hashtag_id(hashtag_key(name)) for name in names}

    def hashtag_candidates(self, keyword):
        """``[{'id', 'name'}]`` TikTok suggests for one keyword; several at
        once mix the suggestions and drop exact matches.

        :raises ValueError: TikTok refused the read
        """
        body = utl.response_body(self._get(
            _api_url('/tool/hashtag/recommend/'),
            params={'advertiser_id': self.advertiser_id,
                    'keywords': json.dumps([hashtag_key(keyword)])}))
        err = _extract_error(body)
        if err:
            raise ValueError(
                f"TikTok hashtags: {err.get('message') or err.get('code')}")
        rows = (body.get('data') or {}).get('recommend_keywords') or []
        return [{'id': str(row['keyword_id']),
                 'name': str(row.get('keyword') or '')}
                for row in rows if row.get('keyword_id')]

    def _hashtag_id(self, key):
        """One name's id; a failed read is not remembered, so a retry
        asks again."""
        if not key:
            return ''
        if key not in self.hashtag_ids:
            try:
                rows = self.hashtag_candidates(key)
            except ValueError as e:
                logging.warning('%s', e)
                return ''
            self.hashtag_ids[key] = next(
                (row['id'] for row in rows
                 if hashtag_key(row['name']) == key), '')
        return self.hashtag_ids[key]

    def hashtag_ids_for(self, value):
        """``(ids, unknown)`` for a Hashtags cell: ids pass through,
        typed names are looked up, and ``unknown`` keeps the names
        TikTok had no hashtag for."""
        tokens = utl.split_list(value)
        found = self.resolve_hashtag_ids(
            [token for token in tokens if not token.isdigit()])
        ids = [found.get(token, token) for token in tokens]
        return ([x for x in dict.fromkeys(ids) if x],
                [token for token in tokens if found.get(token) == ''])

    def get_hashtag_names(self, ids):
        """``{id: '#name'}`` for hashtag ids; an id TikTok does not
        answer for is left out, so a caller shows the bare id."""
        ids = list(dict.fromkeys(str(x) for x in ids or [] if x))
        names = {}
        for start in range(0, len(ids), HASHTAG_BATCH):
            body = utl.response_body(self._get(
                _api_url('/tool/hashtag/get/'),
                params={'advertiser_id': self.advertiser_id,
                        'keyword_ids': json.dumps(
                            ids[start:start + HASHTAG_BATCH])}))
            err = _extract_error(body)
            if err:
                logging.warning('TikTok hashtag names failed: %s',
                                err.get('message') or err.get('code'))
                continue
            for row in (body.get('data') or {}).get('keywords_status') or []:
                if row.get('keyword_id'):
                    names[str(row['keyword_id'])] = str(
                        row.get('keyword') or '')
        return names

    def _list_pages(self, endpoint, params=None, rows_key='list',
                    strict=False, until_short_page=False, verified=False):
        """Yield pages from ``rows_key``; strict inventory reads reject
        partial results so an incomplete picker cannot look complete.
        ``until_short_page`` is for endpoints that misreport ``total_page``.
        """
        page_params = dict(params or {})
        page_params.setdefault('advertiser_id', self.advertiser_id)
        page_params.setdefault('page_size', LIST_PAGE_SIZE)
        for page in range(1, MAX_LIST_PAGES + 1):
            page_params['page'] = page
            response = self._get(_api_url(endpoint), params=page_params)
            body = (self._verified_response(response) if verified
                    else utl.response_body(response))
            err = _extract_error(body)
            if err:
                if strict:
                    raise ValueError(
                        f'TikTok {endpoint}: {err.get("message")}')
                logging.warning('TikTok list %s failed: %s', endpoint,
                                err.get('message') or err.get('code'))
                return
            data = body.get('data') or {}
            if strict and not isinstance(data.get(rows_key), list):
                raise ValueError('TikTok inventory response has no list.')
            rows = data.get(rows_key) or []
            page_count = (data.get('page_info') or {}).get('total_page')
            if verified and (
                    not isinstance(page_count, int) or page_count < 0
                    or any(not isinstance(row, dict) for row in rows)):
                raise ValueError('TikTok returned an incomplete inventory. '
                                 'Refresh TikTok and review again.')
            yield from rows
            total = page_count or 1
            last_page = (len(rows) < LIST_PAGE_SIZE if until_short_page
                         else page >= total)
            if last_page:
                return
        if strict:
            raise ValueError('TikTok inventory exceeded the page limit.')
        logging.warning(
            'TikTok list hit the %s-page cap; results may be truncated.',
            MAX_LIST_PAGES)

    def _inventory_rows(self, endpoint, id_key, name_key, rows_key='list'):
        rows = self._list_pages(endpoint, rows_key=rows_key, strict=True)
        return [{'id': str(row[id_key]),
                 'name': str(row.get(name_key) or row[id_key])}
                for row in rows if row.get(id_key)]

    def get_pixels(self):
        """List advertiser pixels; raise on denied or incomplete reads."""
        return self._inventory_rows(
            '/pixel/list/', 'pixel_id', 'pixel_name', rows_key='pixels')

    def get_identities(self):
        """Return picked-account tokens, labelled for a person to read.
        The token keeps the type and Business Center the ad needs."""
        found = {}
        for row in self._list_pages('/identity/get/', strict=True,
                                    rows_key='identity_list',
                                    until_short_page=True):
            identity = str(row.get('identity_id') or '')
            kind = row.get('identity_type')
            if not identity or kind not in IDENTITY_LABELS:
                continue
            center = row.get('identity_authorized_bc_id') or ''
            token = f'{kind}::{identity}::{center}'
            name = row.get('display_name') or identity
            found[token] = {'id': token, 'name': name,
                            'label': f'{name} · {IDENTITY_LABELS[kind]}'}
        return list(found.values())

    def get_adgroup_settings(self, adgroup_id):
        """Read the real parent placements once per client, failing closed."""
        key = str(adgroup_id)
        if key not in self.adgroup_settings:
            rows = self._list_pages('/adgroup/get/', params={
                'filtering': json.dumps({'adgroup_ids': [key]})}, strict=True)
            found = [row for row in rows
                     if str(row.get('adgroup_id')) == key]
            if len(found) != 1:
                raise ValueError('The TikTok ad group could not be checked.')
            self.adgroup_settings[key] = found[0]
        return self.adgroup_settings[key]

    def wait_for_video(self, video_id, require_cover=False):
        """Wait for usable metadata and, if needed, the generated cover.
        Uploaded IDs survive failure so a later run can retry processing."""
        key = ('video', str(video_id))
        error_key = (str(video_id), require_cover)
        if error_key in self.media_errors:
            raise ValueError(self.media_errors[error_key])
        row = self.media.get(key, {})
        for attempt in range(VIDEO_READY_ATTEMPTS):
            if attempt:
                logging.info(f'{self.progress_line}: waiting for TikTok to '
                             'finish processing the video')
                time.sleep(VIDEO_READY_INTERVAL)
            if not _video_ready(row, require_cover):
                row = self.media[key] = self._read_media('video', video_id)
            if _video_ready(row, require_cover):
                error = _media_error(row, is_video=True)
                if error:
                    raise ValueError(error)
                return row
        message = ('TikTok is still processing the video or cannot use it. '
                   'Check it in the Asset Library, then retry this ad.')
        self.media_errors[error_key] = message
        raise ValueError(message)

    def _read_media(self, kind, media_id):
        """One ``video`` or ``image`` info row, ``{}`` until TikTok has it."""
        rows = _media_rows(self._get(
            _api_url(f'/file/{kind}/ad/info/'), params={
                'advertiser_id': self.advertiser_id,
                f'{kind}_ids': json.dumps([str(media_id)])}))
        return next((item for item in rows
                     if str(item.get(f'{kind}_id')) == str(media_id)), {})

    def get_image_info(self, image_id):
        """Return validated image metadata for local or supplied asset IDs."""
        key = ('image', str(image_id))
        row = self.media.get(key, {})
        if not all(_dimensions(row)):
            row = self._read_media('image', image_id)
        error = _media_error(row)
        if error:
            raise ValueError(error)
        self.media[key] = row
        return row

    def video_cover(self, video_id, video):
        """Import TikTok's generated cover into the advertiser's library."""
        if video_id in self.video_covers:
            return self.video_covers[video_id]
        url = video.get('video_cover_url')
        if not url:
            raise ValueError('Choose a cover image; TikTok has no cover yet.')
        rows = _media_rows(self._post(
            _api_url('/file/image/ad/upload/'), body={
                'advertiser_id': self.advertiser_id,
                'upload_type': 'UPLOAD_BY_URL', 'image_url': url,
                'file_name': f'cover_{video_id}.jpg'[:100]}))
        row = next((item for item in rows if item.get('image_id')), {})
        image_id = str(row.get('image_id') or '')
        if not image_id:
            raise ValueError('TikTok did not return a cover. Choose an image.')
        self.media[('image', image_id)] = row
        self.video_covers[video_id] = image_id
        return image_id

    def get_custom_audiences(self):
        """List custom and lookalike audiences for inclusion/exclusion."""
        return self._inventory_rows(
            '/dmp/custom_audience/list/', 'audience_id', 'name')

    def get_saved_audiences(self):
        """List reusable targeting presets for the ad group's saved ID."""
        return self._inventory_rows(
            '/dmp/saved_audience/list/', 'saved_audience_id',
            'saved_audience_name')

    def get_locations(self):
        """List locations available to this advertiser, with country labels."""
        body = utl.response_body(self._get(
            _api_url('/search/region/'),
            params={'advertiser_id': self.advertiser_id, 'language': 'en'}))
        error = _extract_error(body)
        if error:
            raise ValueError(f'TikTok locations: {error.get("message")}')
        rows = (body.get('data') or {}).get('region_list')
        if not isinstance(rows, list):
            raise ValueError('TikTok locations response has no list.')
        locations = []
        for row in rows:
            if not row.get('region_id'):
                continue
            labels = [str(row[key]) for key in (
                'region_name', 'country_code', 'region_level') if row.get(key)]
            value = str(row['region_id'])
            locations.append({
                'id': value, 'name': ' · '.join(labels) or value})
        return locations

    def find_locations(self, location_ids, placements=None,
                       objective_type=None):
        """``{id: name}`` for the location ids TikTok knows, read from
        the narrowest region level that places them all.

        :raises ValueError: a refused or malformed read.
        """
        wanted = {str(x).strip() for x in location_ids if str(x).strip()}
        found = {}
        for level_range in REGION_LEVEL_RANGES:
            if wanted <= set(found):
                break
            body = utl.response_body(self._get(
                _api_url('/tool/region/'),
                params={'advertiser_id': self.advertiser_id,
                        'placements': json.dumps(
                            list(placements or PLACEMENTS_DEFAULT)),
                        'objective_type': (objective_type
                                           or OBJECTIVE_DEFAULT),
                        'level_range': level_range}))
            error = _extract_error(body)
            if error:
                raise ValueError(
                    f'TikTok locations: {error.get("message")}')
            rows = (body.get('data') or {}).get('region_info')
            if not isinstance(rows, list):
                raise ValueError('TikTok locations response has no list.')
            for row in rows:
                rid = str(row.get('location_id') or '')
                if rid in wanted:
                    found[rid] = str(row.get('name') or rid)
        return found

    list_specs = {
        'campaign': ('/campaign/get/', 'campaign_id', 'campaign_name',
                     None, None),
        'adgroup': ('/adgroup/get/', 'adgroup_id', 'adgroup_name',
                    'campaign_id', 'campaign_ids'),
        'ad': ('/ad/get/', 'ad_id', 'ad_name', 'adgroup_id', 'adgroup_ids'),
    }
    id_dict_attrs = {'campaign': 'cam_dict', 'adgroup': 'adgroup_dict',
                     'ad': 'ad_dict'}

    def set_id_dict(self, kind=None, filter_id=None):
        """Cache live IDs and names, scoped to the requested parent.
        Recheck parents locally because TikTok may ignore unsupported filters."""
        spec = self.list_specs.get(kind)
        if not spec:
            return
        endpoint, id_field, name_field, parent_field, filter_key = spec
        filtering = {'primary_status': 'STATUS_NOT_DELETE'}
        if filter_id and filter_key:
            filtering[filter_key] = [str(filter_id)]
        params = {'filtering': json.dumps(filtering)}
        found = {}
        for row in self._list_pages(endpoint, params=params):
            oid = row.get(id_field)
            deleted = {'DELETE', 'STATUS_DELETE',
                       f'{kind.upper()}_STATUS_DELETE'}
            if not oid or any(row.get(key) in deleted for key in (
                    'operation_status', 'primary_status', 'secondary_status')):
                continue
            parent = str(row.get(parent_field) or '') if parent_field else ''
            if filter_id and parent_field and parent != str(filter_id):
                continue
            found[str(oid)] = {'id': str(oid),
                               'name': row.get(name_field, ''),
                               'parent_id': parent}
            if kind == 'campaign':
                found[str(oid)]['objective_type'] = row.get('objective_type')
            if kind == 'adgroup':
                self.adgroup_settings[str(oid)] = row
        setattr(self, self.id_dict_attrs[kind], found)
        self.id_dict_scope[kind] = str(filter_id or '')

    def ensure_id_dict(self, kind, filter_id=None):
        """The ``kind`` dict, re-listed when it is missing or was built
        for a different parent.

        Cached on the recorded scope, not on emptiness like Reddit's
        template: OBSERVED live, TikTok ad names repeat verbatim across
        ad groups in one campaign, so a dict still scoped to the
        previous parent name-matches a sibling and reports
        ``skipped_exists`` against another ad group's ad. A parent with
        no children yet is a legitimate empty result.

        :param kind: one of ``campaign`` / ``adgroup`` / ``ad``
        :param filter_id: parent platform id to scope to
        :returns: the id dict for ``kind``
        """
        scope = str(filter_id or '')
        if self.id_dict_scope.get(kind, _UNSCOPED) != scope:
            self.set_id_dict(kind, filter_id=filter_id)
        return getattr(self, self.id_dict_attrs[kind])

    live_kinds = {'Campaign': 'campaign', 'Adset': 'adgroup', 'Ad': 'ad'}

    def list_recent(self, level, limit=25, parent_ids=None):
        """The advertiser's ``level`` objects for the copy-from-account
        picker, most recently changed first, from the first
        ``utl.RECENT_SCAN_CAP`` rows. The parent filter is re-applied
        locally for the reason :meth:`set_id_dict` gives."""
        kind = self.live_kinds.get(level)
        if not kind:
            return []
        endpoint, id_field, name_field, parent_field, filter_key = (
            self.list_specs[kind])
        params = {}
        wanted = {str(x) for x in parent_ids or []}
        if wanted and filter_key:
            params['filtering'] = json.dumps({filter_key: sorted(wanted)})
        found = itertools.islice(self._list_pages(endpoint, params=params),
                                 max(limit, utl.RECENT_SCAN_CAP))
        if wanted and parent_field:
            found = (row for row in found
                     if str(row.get(parent_field)) in wanted)
        return utl.recent_rows(found, limit, id_key=id_field,
                               name_key=name_field, parent_key=parent_field,
                               created_key='create_time',
                               updated_key='modify_time')

    def create_entity(self, entity, entity_name='campaign'):
        """POST the entity's ``upload_dict`` to the level's create
        endpoint; the advertiser id rides every write."""
        body = dict(entity.upload_dict)
        body['advertiser_id'] = self.advertiser_id
        return self._post(_api_url(f'/{entity_name}/create/'), body=body)

    def delete_empty_adgroup(self, platform_id, parent_id):
        """Delete only a freshly verified paused ad group without any ads.
            Refuse unreadable inventory, a different campaign
            or a failed delete."""
        pid = str(platform_id)
        groups = self._replacement_rows('/adgroup/get/', {
            'adgroup_ids': [pid], 'primary_status': 'STATUS_ALL'})
        matches = [row for row in groups
                   if str(row.get('adgroup_id')) == pid]
        if (len(matches) != 1 or not parent_id
                or str(matches[0].get('campaign_id')) != str(parent_id)):
            raise ValueError('Could not verify the ad group and campaign. '
                             'Refresh TikTok and review again.')
        if matches[0].get('operation_status') != 'DISABLE':
            raise ValueError('Pause the TikTok ad group before replacing it.')
        ads = self._replacement_rows('/ad/get/', {
            'adgroup_ids': [pid], 'primary_status': 'STATUS_ALL'})
        if any(not row.get('adgroup_id')
               or str(row['adgroup_id']) == pid for row in ads):
            raise ValueError('This TikTok ad group contains ads or its ads '
                             'could not be verified. It cannot be replaced.')
        response = self._post(_api_url('/adgroup/status/update/'), body={
            'advertiser_id': self.advertiser_id, 'adgroup_ids': [pid],
            'operation_status': 'DELETE', 'allow_partial_success': False})
        self._verified_response(response)
        self.adgroup_dict.pop(pid, None)
        self.adgroup_settings.pop(pid, None)
        self.id_dict_scope.pop('adgroup', None)
        self.id_dict_scope.pop('ad', None)

    def _replacement_rows(self, endpoint, filtering):
        """Read every page with a verified success envelope before deleting."""
        return list(self._list_pages(
            endpoint, params={'filtering': json.dumps(filtering)},
            strict=True, verified=True))

    @staticmethod
    def _verified_response(response):
        """Require an explicit successful response for destructive writes."""
        body = utl.response_body(response)
        status = getattr(response, 'status_code', 200)
        if (body.get('code') not in (0, '0')
                or isinstance(status, int) and status >= 400):
            raise ValueError(body.get('message') or
                             'TikTok could not confirm the request. '
                             'Check the account before retrying.')
        return body

    def probe_account(self):
        """(ok, message) — verify the token reaches the configured
        advertiser, for the live pre-flight checks."""
        try:
            params = {'advertiser_ids': json.dumps([self.advertiser_id])}
            body = utl.response_body(
                self._get(_api_url('/advertiser/info/'), params=params))
            err = _extract_error(body)
            if err:
                return False, str(err.get('message') or err)
            rows = (body.get('data') or {}).get('list') or []
            if not rows:
                return False, (
                    'TikTok token reached no advertiser matching '
                    f'advertiser_id {self.advertiser_id!r} — confirm the id '
                    'in tikconfig.json.')
            return True, ''
        except Exception as e:
            return False, str(e)

    def advertiser_timezone(self):
        """The advertiser's timezone name, read once from
        ``/advertiser/info/``, else the Pacific default. Flight cells are
        typed in account time and TikTok reads a schedule timestamp as
        UTC, so every write converts through this zone."""
        if self.timezone:
            return self.timezone
        self.timezone = utl.FLIGHT_TIMEZONE
        if not (self.access_token and self.advertiser_id):
            return self.timezone
        params = {'advertiser_ids': json.dumps([self.advertiser_id]),
                  'fields': json.dumps(['timezone'])}
        try:
            body = utl.response_body(
                self._get(_api_url('/advertiser/info/'), params=params))
        except requests.exceptions.RequestException as e:
            logging.warning(f'TikTok advertiser timezone not read: {e}')
            return self.timezone
        rows = (body.get('data') or {}).get('list') or []
        zone = utl.known_timezone((rows[0] if rows else {}).get('timezone'))
        if zone:
            self.timezone = zone
        else:
            logging.warning(
                f'TikTok advertiser {self.advertiser_id} reported no known '
                f'timezone; flight times are read as {self.timezone}.')
        return self.timezone

    def upload_asset(self, file_path, is_video=True):
        """Stream a local creative with its MD5 signature to the Asset Library.
        Return ``(asset_id, error_message)``, with one value empty."""
        endpoint = ('/file/video/ad/upload/' if is_video
                    else '/file/image/ad/upload/')
        field = 'video_file' if is_video else 'image_file'
        signature_field = 'video_signature' if is_video else 'image_signature'
        id_key = 'video_id' if is_video else 'image_id'
        file_name = os.path.basename(file_path)
        extensions = VIDEO_EXTENSIONS if is_video else IMAGE_EXTENSIONS
        if os.path.splitext(file_name)[1].lower() not in extensions:
            return '', 'Choose an MP4/MOV/MPEG/AVI video or JPG/PNG image.'
        try:
            size = os.path.getsize(file_path)
            limit = VIDEO_MAX_BYTES if is_video else IMAGE_MAX_BYTES
            if not size or size > limit:
                return '', ('Choose a nonempty file under '
                            f'{limit // 1048576} MB.')
            with open(file_path, 'rb') as f:
                signature = hashlib.file_digest(f, 'md5').hexdigest()
        except IOError as e:
            return '', f'Could not read {file_path}: {e}'
        stem, extension = os.path.splitext(file_name)
        file_name = f'{stem[:60]}_{signature[:12]}{extension}'
        data = {'advertiser_id': self.advertiser_id,
                'upload_type': UPLOAD_TYPE_FILE,
                'file_name': file_name,
                signature_field: signature}
        try:
            response = self._post_file(
                _api_url(endpoint), data=data, field=field,
                file_name=file_name, path=file_path)
        except (OSError, requests.exceptions.RequestException) as e:
            return '', str(e)
        body = utl.response_body(response)
        err = _extract_error(body)
        if err:
            return '', (err.get('message')
                        or f'TikTok upload failed (code {err.get("code")})')
        # A list of assets even for a single file.
        rows = body.get('data') or []
        if isinstance(rows, dict):
            rows = [rows]
        for row in rows:
            if isinstance(row, dict) and row.get(id_key):
                asset_id = str(row[id_key])
                kind = 'video' if is_video else 'image'
                self.media[(kind, asset_id)] = row
                return asset_id, ''
        return '', f'TikTok upload response missing {id_key}'

    entity_status_endpoints = {'Campaign': '/campaign/status/update/',
                               'Adset': '/adgroup/status/update/',
                               'Ad': '/ad/status/update/'}
    entity_status_id_fields = {'Campaign': 'campaign_ids',
                               'Adset': 'adgroup_ids', 'Ad': 'ad_ids'}

    def update_object(self, object_level, platform_id, changes,
                      context=None):
        """Patch one existing object, retaining per-object API failures."""
        result = utl.new_update_result(platform_id)
        try:
            kind = self.live_kinds[object_level]
            if AdGroupUpload.hashtags in (changes or {}):
                ids, unknown = self.hashtag_ids_for(
                    changes[AdGroupUpload.hashtags])
                if unknown:
                    raise ValueError(unknown_hashtags_message(unknown))
                changes = {**changes, AdGroupUpload.hashtags: '|'.join(ids)}
            params = self.update_params(object_level, changes, context,
                                        self.advertiser_timezone())
            if object_level == 'Ad':
                parent = (context or {}).get('_parent_platform_id')
                if not parent:
                    raise ValueError('The saved ad group ID is missing.')
                body = {'advertiser_id': self.advertiser_id,
                        'adgroup_id': str(parent), 'patch_update': True,
                        'creatives': [{'ad_id': str(platform_id), **params}]}
            else:
                body = {'advertiser_id': self.advertiser_id,
                        f'{kind}_id': str(platform_id), **params}
            response = self._post(_api_url(f'/{kind}/update/'), body=body)
            error = _extract_error(utl.response_body(response))
            if error or getattr(response, 'status_code', 200) >= 400:
                utl.fail_result(result, error.get('message')
                                or 'TikTok refused the update.',
                                error.get('code'))
        except (KeyError, ValueError, TypeError,
                requests.RequestException) as e:
            utl.fail_result(result, e)
        return result

    @staticmethod
    def update_params(object_level, changes, context=None,
                      timezone=utl.FLIGHT_TIMEZONE):
        """Transform only editable fields; never send create defaults.
        Flight cells are read in the advertiser's ``timezone``."""
        fields = UPDATE_COLUMN_FIELDS.get(object_level, {})
        if not changes or set(changes) - set(fields):
            raise ValueError('These TikTok settings require a new object.')
        params = {}
        for col, value in changes.items():
            if object_level == 'Adset' and col == 'optimization_goal':
                previous = (context or {}).get('_previous_values') or {}
                utl.reach_cap(previous.get(col) or '', value,
                              cap_required=True)
                params.update(_reach_cap(value)[1])
            elif col in ('start_time', 'end_time'):
                stamp = _to_datetime(value, end_of_day=col == 'end_time',
                                     timezone=timezone)
                if not stamp:
                    raise ValueError('Enter a valid flight date and time.')
                params[fields[col]] = stamp
            elif col in AdGroupUpload.targeting_list_cols:
                params[fields[col]] = utl.split_list(value)
            elif col == AdGroupUpload.hashtags:
                params[fields[col]] = hashtag_actions(utl.split_list(value))
            elif col in ('budget', 'bid_price'):
                amount = _to_budget(value)
                if amount is None or amount < 0:
                    raise ValueError(f'Enter a valid {col.replace("_", " ")}.')
                params[fields[col]] = amount
            else:
                params[fields[col]] = str(value or '')
        return params

    def update_statuses(self, object_level, platform_ids, activate=True):
        """POST ``operation_status`` on existing objects by id — one
        call per id so each row carries its own verdict. Returns one
        dict per id: {'platform_id', 'status' ('updated'|'failed'),
        'error_code', 'error_message'}."""
        endpoint = self.entity_status_endpoints.get(object_level)
        id_field = self.entity_status_id_fields.get(object_level)
        status = 'ENABLE' if activate else 'DISABLE'
        results = []
        for pid in platform_ids:
            result = utl.new_update_result(pid)
            if not endpoint:
                results.append(utl.fail_result(
                    result, f'Unknown TikTok level: {object_level}'))
                continue
            try:
                body = {'advertiser_id': self.advertiser_id,
                        id_field: [str(pid)],
                        'operation_status': status}
                err = _extract_error(utl.response_body(
                    self._post(_api_url(endpoint), body=body)))
                if err:
                    utl.fail_result(
                        result,
                        err.get('message')
                        or 'Unknown error from TikTok Ads',
                        err.get('code'))
            except Exception as e:
                utl.fail_result(result, e)
            results.append(result)
        return results


class CampaignUpload(utl.BaseUploadConfig):
    config_dir = config_path
    config_label = 'TikTok campaign config'
    file_name = 'campaign_upload.xlsx'
    name = 'name'
    objective_type = 'objective_type'
    status = 'operation_status'
    budget_mode = 'budget_mode'
    budget = 'budget'
    budget_optimize_on = 'budget_optimize_on'
    snapshot_cols = [objective_type, status, budget_mode, budget,
                     budget_optimize_on]

    @staticmethod
    def settings_from_live(fields):
        """The live campaign's objective and budget settings as
        level-file cells; name, status and the amount are not copied."""
        cu = CampaignUpload
        return utl.live_settings({
            cu.objective_type: fields.get('objective_type'),
            cu.budget_mode: fields.get('budget_mode'),
            cu.budget_optimize_on: _live_flag(
                fields.get('budget_optimize_on'))})

    def upload_all_campaigns(self, api):
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, c_id in enumerate(self.config):
            cam = Campaign(self.config[c_id], api=api)
            logging.info(
                f'Uploading TikTok campaign {idx + 1} of {total}: '
                f'{cam.name}')
            result = self.upload_campaign(api, cam)
            result['pushed_values'] = utl.snapshot_values(
                self.config[c_id], self.snapshot_cols)
            results.append(result)
        return results

    @staticmethod
    def upload_campaign(api, campaign):
        result = utl.new_result('Campaign', campaign.name, CHANNEL)
        if not campaign.upload_dict:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = 'Missing required campaign fields'
            return result
        if campaign.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = campaign.id
            return result
        _populate_tik_result(result, api.create_entity(campaign))
        if result['status'] == 'created':
            campaign.id = result['platform_id']
        return result


class Campaign(object):
    __slots__ = ['name', 'objective_type', 'operation_status',
                 'budget_mode', 'budget', 'budget_optimize_on',
                 'upload_dict', 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.objective_type = OBJECTIVE_DEFAULT
        self.operation_status = STATUS_DEFAULT
        self.budget_mode = None
        self.budget = None
        self.budget_optimize_on = None
        utl.apply_row(self, row_dict)
        self.api = api
        self.upload_dict = self.create_cam_dict()

    def create_cam_dict(self):
        if not self.name:
            return {}
        d = {
            'campaign_name': str(self.name),
            'objective_type': str(self.objective_type or OBJECTIVE_DEFAULT),
            'operation_status': str(self.operation_status or STATUS_DEFAULT),
        }
        budget = _to_budget(self.budget)
        if budget:
            d['budget'] = budget
            d['budget_mode'] = str(self.budget_mode or BUDGET_MODE_DEFAULT)
        elif str(self.budget_mode or ''):
            # An explicit mode with no budget (BUDGET_MODE_INFINITE) is
            # valid; omitting both lets TikTok default to no cap.
            d['budget_mode'] = str(self.budget_mode)
        # Campaign Budget Optimization moves the budget up to this level
        # and hands bidding/optimization to the ad groups beneath it.
        if _to_bool(self.budget_optimize_on):
            d['budget_optimize_on'] = True
        return d

    def check_exists(self, api):
        found = api.get_id(api.ensure_id_dict('campaign'), self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


class AdGroupUpload(utl.BaseUploadConfig):
    """TikTok's mid-tier object (Adset in LQ parlance).

    Column names stay in the spreadsheet vocabulary the other channels
    use (``name``, ``start_time``, ``end_time``) and are mapped onto
    TikTok's field spellings in ``create_adgroup_dict``, so the app's
    name-builder and apply-across machinery treat TikTok like any
    other channel.
    """
    config_dir = config_path
    config_label = 'TikTok adgroup config'
    file_name = 'adset_upload.xlsx'
    name = 'name'
    campaign = 'campaign'
    status = 'operation_status'
    budget = 'budget'
    budget_mode = 'budget_mode'
    budget_optimize_on = 'budget_optimize_on'
    dayparting = 'dayparting'
    billing_event = 'billing_event'
    optimization_goal = 'optimization_goal'
    optimization_event = 'optimization_event'
    pacing = 'pacing'
    bid_type = 'bid_type'
    bid_price = 'bid_price'
    schedule_type = 'schedule_type'
    start_time = 'start_time'
    end_time = 'end_time'
    promotion_type = 'promotion_type'
    placement_type = 'placement_type'
    placements = 'placements'
    pixel_id = 'pixel_id'
    # Targeting columns (pipe/comma-delimited cells).
    location_ids = 'location_ids'
    age_groups = 'age_groups'
    languages = 'languages'
    interest_category_ids = 'interest_category_ids'
    interest_keyword_ids = 'interest_keyword_ids'
    hashtags = 'hashtags'
    audience_ids = 'audience_ids'
    excluded_audience_ids = 'excluded_audience_ids'
    saved_audience_id = 'saved_audience_id'
    operating_systems = 'operating_systems'
    gender = 'gender'
    targeting_list_cols = (location_ids, age_groups, languages,
                           interest_category_ids, interest_keyword_ids,
                           audience_ids,
                           excluded_audience_ids, operating_systems)
    snapshot_cols = [status, budget, budget_mode, budget_optimize_on,
                     billing_event, optimization_goal, pacing, bid_type,
                     bid_price, schedule_type, start_time, end_time,
                     promotion_type, placement_type, dayparting, pixel_id,
                     audience_ids, excluded_audience_ids, saved_audience_id,
                     location_ids, age_groups, languages,
                     interest_category_ids, interest_keyword_ids, hashtags,
                     operating_systems, gender, placements]
    live_setting_cols = (budget_mode, budget_optimize_on, dayparting,
                         billing_event, optimization_goal,
                         optimization_event, pacing, bid_type,
                         promotion_type, placement_type, placements,
                         pixel_id, location_ids, age_groups, languages,
                         interest_category_ids, interest_keyword_ids,
                         operating_systems, gender,
                         audience_ids, excluded_audience_ids,
                         saved_audience_id)

    @staticmethod
    def settings_from_live(fields):
        """The live ad group's settings as level-file cells; name,
        status, amounts and the schedule (the flight's) are not copied."""
        agu = AdGroupUpload
        values = {col: fields.get(col) for col in agu.live_setting_cols}
        values.update({col: utl.join_list(fields.get(col))
                       for col in (*agu.targeting_list_cols, agu.placements)})
        values[agu.budget_optimize_on] = _live_flag(
            fields.get(agu.budget_optimize_on))
        values[agu.optimization_goal] = agu.live_goal(fields)
        values[agu.hashtags] = utl.join_list(
            live_hashtag_ids(fields.get('actions')))
        return utl.live_settings(values)

    @staticmethod
    def live_goal(fields):
        """The optimization goal as ``create_adgroup_dict`` reads it, a
        Reach goal carrying its frequency cap as ``REACH|days|count``."""
        goal = fields.get('optimization_goal') or ''
        days = fields.get('frequency_schedule')
        count = fields.get('frequency')
        if goal == OPTIMIZATION_GOAL_REACH and days and count:
            return f'{goal}|{days}|{count}'
        return goal

    def upload_all_adgroups(self, api):
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, ag_id in enumerate(self.config):
            ag = AdGroup(self.config[ag_id], api=api)
            logging.info(
                f'Uploading TikTok adgroup {idx + 1} of {total}: {ag.name}')
            result = self.upload_adgroup(api, ag)
            result['pushed_values'] = utl.snapshot_values(
                self.config[ag_id], self.snapshot_cols)
            results.append(result)
        return results

    @staticmethod
    def upload_adgroup(api, adgroup):
        result = utl.new_result('Adset', adgroup.name, CHANNEL,
                                adgroup.campaignId)
        if not adgroup.campaignId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                f'Campaign {adgroup.campaign!r} not found')
            return result
        if not adgroup.upload_dict:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                'Missing required ad group fields: '
                f'{", ".join(adgroup.missing)}')
            return result
        if adgroup.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = adgroup.id
            return result
        _populate_tik_result(
            result, api.create_entity(adgroup, entity_name='adgroup'),
            id_key='adgroup_id')
        if result['status'] == 'created':
            adgroup.id = result['platform_id']
        return result


OBJECTIVE_PROMOTION_TYPES = {'TRAFFIC': 'WEBSITE'}


class AdGroup(object):
    __slots__ = ['name', 'campaign', 'campaignId', 'objective',
                 'operation_status',
                 'budget', 'budget_mode', 'budget_optimize_on',
                 'billing_event', 'optimization_goal', 'optimization_event',
                 'pacing', 'bid_type', 'bid_price', 'schedule_type',
                 'start_time', 'end_time', 'dayparting', 'promotion_type',
                 'placement_type', 'placements', 'pixel_id', 'location_ids',
                 'age_groups', 'languages', 'interest_category_ids',
                 'interest_keyword_ids', 'hashtags', 'audience_ids',
                 'excluded_audience_ids',
                 'saved_audience_id', 'operating_systems', 'gender',
                 'missing', 'upload_dict', 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.campaign = None
        self.campaignId = None
        self.objective = None
        self.operation_status = STATUS_DEFAULT
        self.budget = None
        self.budget_mode = None
        self.budget_optimize_on = None
        self.dayparting = None
        self.billing_event = None
        self.optimization_goal = None
        self.optimization_event = None
        self.pacing = None
        self.bid_type = None
        self.bid_price = None
        self.schedule_type = None
        self.start_time = None
        self.end_time = None
        self.promotion_type = None
        self.placement_type = None
        self.placements = None
        self.pixel_id = None
        self.location_ids = None
        self.age_groups = None
        self.languages = None
        self.interest_category_ids = None
        self.interest_keyword_ids = None
        self.hashtags = None
        self.audience_ids = None
        self.excluded_audience_ids = None
        self.saved_audience_id = None
        self.operating_systems = None
        self.gender = None
        self.missing = []
        utl.apply_row(self, row_dict)
        self.api = api
        if self.api:
            self.resolve_campaign(self.api)
            self.resolve_hashtags(self.api)
        self.upload_dict = self.create_adgroup_dict()

    def resolve_campaign(self, api):
        cam = Campaign({'name': self.campaign}, api=api)
        cam.check_exists(api)
        self.campaignId = cam.id
        self.objective = (api.ensure_id_dict('campaign').get(cam.id)
                          or {}).get('objective_type')

    def resolve_hashtags(self, api):
        """Swap hashtags typed by name for their ids, keeping unknown names
        so the create refuses the row rather than widen the audience."""
        if not self.hashtags:
            return
        ids, unknown = api.hashtag_ids_for(self.hashtags)
        self.hashtags = utl.join_list(ids + unknown)

    def create_adgroup_dict(self):
        """The ``/adgroup/create/`` body. Returns {} — recording what
        was absent in ``missing`` — when a field TikTok makes required
        can't be resolved, so the row fails locally with an actionable
        reason instead of spending a round trip on a rejected create.

        ``budget`` is required here only WITHOUT Campaign Budget
        Optimization — under CBO the budget lives on the campaign, so
        the local check would refuse a valid setup. The flag itself is
        a campaign field and is not forwarded. A blank promotion type
        follows the campaign objective, as TikTok refuses a Traffic ad
        group without one.
        """
        cbo = _to_bool(self.budget_optimize_on)
        budget = _to_budget(self.budget)
        unknown = [tag for tag in utl.split_list(self.hashtags)
                   if not tag.isdigit()]
        self.missing = [label for label, value in
                        (('name', self.name),
                         ('campaign', self.campaignId),
                         ('budget', budget or cbo),
                         (f'a hashtag id for {", ".join(unknown)}',
                          not unknown)) if not value]
        if self.missing:
            return {}
        zone = _account_timezone(self.api)
        start = _to_datetime(self.start_time, timezone=zone)
        end = _to_datetime(self.end_time, end_of_day=True, timezone=zone)
        schedule_type = str(self.schedule_type or '') or (
            SCHEDULE_START_END if end else SCHEDULE_FROM_NOW)
        goal, cap = _reach_cap(
            self.optimization_goal or OPTIMIZATION_GOAL_DEFAULT)
        d = {
            'adgroup_name': str(self.name),
            'campaign_id': str(self.campaignId),
            'operation_status': str(self.operation_status or STATUS_DEFAULT),
            'billing_event': str(
                self.billing_event or BILLING_EVENT_DEFAULT),
            'optimization_goal': goal,
            **cap,
            'pacing': str(self.pacing or PACING_DEFAULT),
            'schedule_type': schedule_type,
            'placement_type': str(
                self.placement_type or PLACEMENT_TYPE_DEFAULT),
        }
        if not cbo:
            d['budget'] = budget
            d['budget_mode'] = str(
                self.budget_mode or ADGROUP_BUDGET_MODE_DEFAULT)
        if self.dayparting:
            d['dayparting'] = str(self.dayparting)
        if start:
            d['schedule_start_time'] = start
        if end and schedule_type == SCHEDULE_START_END:
            d['schedule_end_time'] = end
        if d['placement_type'] == PLACEMENT_TYPE_DEFAULT:
            d['placements'] = (utl.split_list(self.placements)
                               or list(PLACEMENTS_DEFAULT))
        promotion = self.promotion_type or OBJECTIVE_PROMOTION_TYPES.get(
            str(self.objective or '').upper())
        for field, value in (('bid_type', self.bid_type),
                             ('promotion_type', promotion),
                             ('optimization_event', self.optimization_event),
                             ('pixel_id', self.pixel_id)):
            if value:
                d[field] = str(value)
        bid = _to_budget(self.bid_price)
        if bid:
            d['bid_price'] = bid
        d.update(self.create_targeting_dict())
        return d

    def create_targeting_dict(self):
        """Pass through saved platform IDs and enums without guessing IDs."""
        targeting = {}
        for field in AdGroupUpload.targeting_list_cols:
            values = utl.split_list(getattr(self, field))
            if values:
                targeting[field] = values
        gender = str(self.gender or '').strip().upper()
        if gender:
            targeting['gender'] = gender
        if self.saved_audience_id:
            targeting['saved_audience_id'] = str(self.saved_audience_id)
        hashtags = [tag for tag in utl.split_list(self.hashtags)
                    if tag.isdigit()]
        if hashtags:
            targeting['actions'] = hashtag_actions(hashtags)
        return targeting

    def check_exists(self, api):
        found = api.get_id(
            api.ensure_id_dict('adgroup', filter_id=self.campaignId),
            self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


class CreativeUpload(utl.BaseCreativeStore):
    """Local creative files -> Asset Library ids, cached in
    ``tik_creative_ids.csv`` so unchanged files reuse their IDs. Videos and
    images go to different endpoints, so both id columns are kept and
    the file's extension picks the one that gets filled.
    """
    id_cols = ('video_id', 'image_id', 'file_signature', 'advertiser_id')

    def __init__(self, id_file_name='tik_creative_ids.csv',
                 creative_path='creative/'):
        super().__init__(id_file_name, creative_path=creative_path)
        self.errors = {}
        self.signatures = {}

    def upload_all(self, api, filenames):
        """Forget files whose bytes or advertiser changed, then upload."""
        advertiser = str(api.advertiser_id)
        for filename in filenames:
            record = self.records.get(filename, {})
            signature = self._signature(filename)
            changed = signature and signature != record.get('file_signature')
            if changed or record.get('advertiser_id') != advertiser:
                self.records.pop(filename, None)
        return super().upload_all(api, filenames)

    def _signature(self, filename):
        """A local file's SHA-256, hashed once: ads share files, and a
        video runs to 500 MB. ``None`` when the file is not here."""
        path = os.path.join(self.creative_path, filename)
        if filename not in self.signatures and os.path.isfile(path):
            with open(path, 'rb') as source:
                self.signatures[filename] = hashlib.file_digest(
                    source, 'sha256').hexdigest()
        return self.signatures.get(filename)

    def remember_cover(self, filename, video_id, image_id):
        """Persist a generated cover only beside the video it belongs to."""
        record = self.records.get(filename)
        if record and record.get('video_id') == video_id:
            record['image_id'] = image_id
            self.write()

    def _upload_one(self, api, file_path):
        filename = os.path.basename(file_path)
        is_video = os.path.splitext(filename)[1].lower() in VIDEO_EXTENSIONS
        asset_id, error = api.upload_asset(file_path, is_video=is_video)
        if not asset_id:
            self.errors[filename] = error
            logging.warning(f'TikTok creative upload failed for '
                            f'{file_path}: {error}')
            return {}
        self.errors.pop(filename, None)
        return {'video_id' if is_video else 'image_id': asset_id,
                'file_signature': self._signature(filename),
                'advertiser_id': str(api.advertiser_id)}


class AdUpload(utl.BaseUploadConfig):
    config_dir = config_path
    config_label = 'TikTok ad config'
    file_name = 'ad_upload.xlsx'
    name = 'name'
    campaign = 'campaign'
    adgroup = 'adgroup'
    identity_id = 'identity_id'
    creative = 'creative'
    thumbnail = 'thumbnail'
    tiktok_item_id = 'tiktok_item_id'
    ad_format = 'ad_format'
    ad_text = 'ad_text'
    call_to_action = 'call_to_action'
    landing_page_url = 'landing_page_url'
    status = 'operation_status'
    music_id = 'music_id'
    video_id = 'video_id'
    image_ids = 'image_ids'
    display_name = 'display_name'
    identity_type = 'identity_type'
    identity_authorized_bc_id = 'identity_authorized_bc_id'
    snapshot_cols = [status, ad_format, ad_text, call_to_action,
                     landing_page_url, display_name, identity_id,
                     identity_type, identity_authorized_bc_id,
                     tiktok_item_id, music_id, thumbnail]
    live_setting_cols = (ad_format, call_to_action, display_name,
                         identity_id, identity_type,
                         identity_authorized_bc_id, tiktok_item_id, music_id)

    @staticmethod
    def settings_from_live(fields):
        """The live ad's format, identity, call to action, post and music
        as level-file cells; text, landing page and files are each ad's."""
        return utl.live_settings({col: fields.get(col)
                                  for col in AdUpload.live_setting_cols})

    @staticmethod
    def urls_from_live(fields):
        """The live ad's landing page, read for reference, never copied."""
        return utl.live_settings(
            {'landing': fields.get(AdUpload.landing_page_url)})

    def upload_all_ads(self, api, creative_store=None):
        """Push every ad row, each uploading its own files, so one bad
        row fails alone; returns one result dict per row."""
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, row in enumerate(self.config.values()):
            api.progress_line = f'Uploading TikTok ad {idx + 1} of {total}'
            logging.info(f'{api.progress_line}: {row.get("name")}')
            try:
                ad = Ad(row, api=api, creative_store=creative_store)
                result = self.upload_ad(api, ad)
            except AD_ROW_ERRORS as exc:
                result = utl.fail_result(utl.new_result(
                    'Ad', row.get('name'), CHANNEL), str(exc))
            result['pushed_values'] = utl.snapshot_values(
                row, self.snapshot_cols)
            results.append(result)
        return results

    @staticmethod
    def upload_ad(api, ad):
        """Check an existing ad before preparing assets for a new one."""
        result = utl.new_result('Ad', ad.name, CHANNEL, ad.adGroupId)
        if not ad.adGroupId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = f'Ad group {ad.adgroup!r} not found'
            return result
        if ad.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = ad.id
            return result
        if not ad.has_media():
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = utl.NO_CREATIVE
            return result
        ad.prepare(api)
        _populate_tik_result(
            result, api.create_entity(ad, entity_name='ad'), id_key='ad_id')
        if result['status'] == 'created':
            ad.id = result['platform_id']
        return result


class Ad(object):
    """One ad row, with pure validation before worker-side preparation."""

    __slots__ = ['name', 'campaign', 'adgroup', 'adGroupId', 'creative',
                 'operation_status', 'ad_format', 'ad_text',
                 'call_to_action', 'landing_page_url', 'display_name',
                 'identity_id', 'identity_type', 'video_id', 'image_ids',
                 'identity_authorized_bc_id', 'tiktok_item_id', 'thumbnail',
                 'music_id', 'missing', 'upload_dict', 'api',
                 'creative_store', 'id']

    def __init__(self, row_dict, api=None, creative_store=None):
        for field in self.__slots__:
            setattr(self, field, None)
        self.operation_status = STATUS_DEFAULT
        self.identity_type = IDENTITY_LINKED
        utl.apply_row(self, row_dict)
        for field in self.__slots__:
            if isinstance(getattr(self, field), str):
                setattr(self, field, _cell(getattr(self, field)))
        identity, kind, bc = identity_values(
            self.identity_id, self.identity_type,
            self.identity_authorized_bc_id)
        self.identity_id, self.identity_type = identity, kind
        self.identity_authorized_bc_id = bc
        self.api = api
        self.creative_store = creative_store
        if self.api:
            self.resolve_ids(self.api)
        self.upload_dict = self.create_ad_dict()

    def resolve_ids(self, api):
        if self.adgroup:
            ag = AdGroup({'name': self.adgroup, 'campaign': self.campaign},
                         api=api)
            ag.check_exists(api)
            self.adGroupId = ag.id

    def resolve_creative(self):
        """Explicit IDs win; a cover file overrides a cached cover."""
        if not self.creative_store:
            return
        filenames = utl.split_list(self.creative)
        if not self.video_id and len(filenames) == 1:
            self.video_id = self.creative_store.get_id(
                filenames[0], 'video_id')
        if self.image_ids:
            return
        if self.video_id and filenames and not self.thumbnail and (
                self.creative_store.get_id(filenames[0], 'video_id')
                != self.video_id):
            return
        image_files = utl.split_list(self.thumbnail) or filenames
        images = [self.creative_store.get_id(name, 'image_id')
                  for name in image_files]
        if images and all(images):
            self.image_ids = '|'.join(str(value) for value in images)

    def format(self):
        """The chosen format, else the one the assets imply."""
        if self.ad_format:
            return self.ad_format
        video_file = os.path.splitext(str(self.creative or ''))[1].lower()
        if self.video_id or self.tiktok_item_id or (
                video_file in VIDEO_EXTENSIONS):
            return AD_FORMAT_VIDEO
        images = utl.split_list(self.image_ids) or utl.split_list(
            self.creative)
        return AD_FORMAT_CAROUSEL if len(images) > 1 else AD_FORMAT_IMAGE

    def has_media(self):
        """Recognize local files, uploaded media and Spark posts as ad creative."""
        return any(_cell(value) for value in (
            self.creative, self.video_id, self.image_ids,
            self.tiktok_item_id))

    def validation_errors(self, require_assets=True):
        """Reject incomplete or contradictory rows before contacting TikTok."""
        required = [('ad name', self.name), ('ad group', self.adGroupId),
                    ('identity', self.identity_id),
                    ('identity type', self.identity_type),
                    ('call to action', self.call_to_action)]
        if not self.tiktok_item_id:
            required.append(('ad text', self.ad_text))
        if self.identity_type == IDENTITY_BUSINESS:
            required.append(('authorizing Business Center',
                             self.identity_authorized_bc_id))
        if self.identity_type == IDENTITY_AUTHORIZED:
            required.append(('TikTok post ID', self.tiktok_item_id))
        errors = [f'Set {name}.' for name, value in required if not value]
        if self.identity_type not in IDENTITY_LABELS:
            errors.append('Choose a supported identity type.')
        if self.operation_status not in ('ENABLE', 'DISABLE'):
            errors.append('Choose Active or Paused for the ad status.')
        errors += _text_errors('ad name', self.name, 512)
        errors += _text_errors('ad text', self.ad_text, 100)
        if self.landing_page_url:
            url = urlparse(str(self.landing_page_url))
            if url.scheme not in ('http', 'https') or not url.netloc:
                errors.append('Enter a complete website URL.')
        return errors + self._format_errors(require_assets)

    def _format_errors(self, require_assets):
        fmt = self.format()
        if fmt not in AD_FORMATS:
            return ['Choose Single Video, Single Image or Carousel.']
        images = utl.split_list(self.image_ids)
        if self.tiktok_item_id:
            return self._post_errors(fmt, images)
        if fmt == AD_FORMAT_VIDEO:
            return self._video_errors(images, require_assets)
        return self._image_errors(fmt, images, require_assets)

    def _post_errors(self, fmt, images):
        """A post outranks the creative a name usually supplies, so only
        an explicit media ID contradicts it."""
        if self.identity_type == IDENTITY_CUSTOM:
            return ['Choose an authorized TikTok identity for this post.']
        if self.video_id or images:
            return ['Use an existing post or a video or image ID, not both.']
        if fmt == AD_FORMAT_IMAGE:
            return ['Choose Single Video or Carousel for a TikTok post.']
        return []

    def _video_errors(self, images, require_assets):
        errors = []
        if not self.video_id and (require_assets or not self.creative):
            errors.append('Choose a video creative.')
        if len(utl.split_list(self.thumbnail)) > 1 or (
                require_assets and len(images) != 1):
            errors.append('Choose exactly one video cover image.')
        return errors

    def _image_errors(self, fmt, images, require_assets):
        errors = []
        if self.video_id:
            errors.append('Remove the video from this image ad.')
        if require_assets and not images:
            errors.append('Choose an image creative.')
        if fmt == AD_FORMAT_IMAGE and len(images) > 1:
            errors.append('Single Image ads require exactly one image.')
        if fmt == AD_FORMAT_CAROUSEL and len(images) > 35:
            errors.append('Use at most 35 carousel images.')
        if fmt == AD_FORMAT_CAROUSEL and not self.music_id:
            errors.append('Choose music for the carousel.')
        return errors

    def _placement_errors(self, parent):
        placements = parent.get('placements') or []
        automatic = parent.get('placement_type') == 'PLACEMENT_TYPE_AUTOMATIC'
        if not automatic and not placements:
            return ['TikTok did not return the ad group placements. Retry.']
        tiktok = automatic or 'PLACEMENT_TIKTOK' in placements
        errors = []
        if tiktok and self.identity_type == IDENTITY_CUSTOM:
            errors.append('Choose a linked or authorized TikTok identity; '
                          'Custom User cannot create ads on TikTok.')
        if self.format() == AD_FORMAT_IMAGE and (
                automatic or not set(placements).issubset(IMAGE_PLACEMENTS)):
            errors.append('Single Image needs Pangle or Global App Bundle '
                          'placements. Choose video or carousel for TikTok.')
        if parent.get('promotion_type') == 'WEBSITE' and not (
                self.landing_page_url):
            errors.append('Enter the destination website URL.')
        return errors

    def prepare(self, api):
        """Check parent settings, upload assets, then verify usable media."""
        parent = api.get_adgroup_settings(self.adGroupId)
        errors = self.validation_errors(require_assets=False)
        errors.extend(self._placement_errors(parent))
        if errors:
            raise ValueError(' '.join(errors))
        if not self.tiktok_item_id:
            self._upload_files(api)
            self._prepare_media(api, parent)
        self.upload_dict = self.create_ad_dict()
        if not self.upload_dict:
            raise ValueError(' '.join(self.missing))

    def _upload_files(self, api):
        """Upload the files this row names and holds no ID for."""
        if not self.creative_store:
            return
        has_asset = (self.video_id if self.format() == AD_FORMAT_VIDEO
                     else self.image_ids)
        files = [] if has_asset else utl.split_list(self.creative)
        if not self.image_ids:
            files = [*files, *utl.split_list(self.thumbnail)]
        files = list(dict.fromkeys(files))
        self.creative_store.upload_all(api, files)
        errors = [self.creative_store.errors[name] for name in files
                  if name in self.creative_store.errors]
        if errors:
            raise ValueError(' '.join(errors))
        self.resolve_creative()

    def _prepare_media(self, api, parent):
        video = self._ready_video(api, parent) if self.video_id else None
        for image_id in utl.split_list(self.image_ids):
            self._check_image(api.get_image_info(image_id), video)

    def _ready_video(self, api, parent):
        """Wait for the video, then settle its placements and cover."""
        video = api.wait_for_video(
            self.video_id, require_cover=not self.image_ids)
        allowed = video.get('allowed_placements')
        if allowed and set(parent.get('placements') or []) - set(allowed):
            raise ValueError('This video is not allowed on all selected '
                             'placements. Choose another creative.')
        if self.image_ids:
            return video
        if self.thumbnail:
            raise ValueError('The selected cover was not uploaded. '
                             'Check the image file and retry.')
        self.image_ids = api.video_cover(self.video_id, video)
        if self.creative_store:
            self.creative_store.remember_cover(
                self.creative, self.video_id, self.image_ids)
        return video

    def _check_image(self, image, video):
        if video:
            width, height = _dimensions(video)
            cover_width, cover_height = _dimensions(image)
            if abs(width / height - cover_width / cover_height) > 0.01:
                raise ValueError('Choose a cover with the same aspect '
                                 'ratio as the video.')
        if self.format() == AD_FORMAT_CAROUSEL and (
                image.get('is_carousel_usable') is not True):
            raise ValueError('Choose an image TikTok allows in carousels.')

    def create_ad_dict(self):
        """Build one creative only after every required field is available."""
        self.missing = self.validation_errors()
        if self.missing:
            return {}
        creative = {'ad_name': str(self.name), 'ad_format': self.format(),
                    'operation_status': self.operation_status}
        for field in ('ad_text', 'call_to_action', 'landing_page_url',
                      'display_name', 'identity_id', 'identity_type',
                      'identity_authorized_bc_id', 'tiktok_item_id',
                      'video_id', 'music_id'):
            value = getattr(self, field)
            if value:
                creative[field] = str(value)
        images = utl.split_list(self.image_ids)
        if images:
            creative['image_ids'] = images
        return {'adgroup_id': str(self.adGroupId), 'creatives': [creative]}

    def check_exists(self, api):
        found = api.get_id(
            api.ensure_id_dict('ad', filter_id=self.adGroupId), self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


LIVE_UPLOADS = {'Campaign': CampaignUpload, 'Adset': AdGroupUpload,
                'Ad': AdUpload}

UPDATE_COLUMN_FIELDS = {
    'Campaign': {'name': 'campaign_name', 'budget': 'budget'},
    'Adset': {
        'name': 'adgroup_name', 'budget': 'budget', 'bid_price': 'bid_price',
        'optimization_goal': 'frequency', 'pacing': 'pacing',
        'schedule_type': 'schedule_type', 'start_time': 'schedule_start_time',
        'end_time': 'schedule_end_time', 'dayparting': 'dayparting',
        'gender': 'gender', 'saved_audience_id': 'saved_audience_id',
        AdGroupUpload.hashtags: 'actions',
        **{col: col for col in AdGroupUpload.targeting_list_cols}},
    'Ad': {'name': 'ad_name'},
}
CLEARABLE_UPDATE_COLS = frozenset((
    *AdGroupUpload.targeting_list_cols, 'dayparting', 'saved_audience_id'))
