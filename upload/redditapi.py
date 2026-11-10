"""Reddit Ads API uploader. Mirrors the awapi/dcapi class shape so
the relation system, name-create flow, and run telemetry pick it
up without special-casing."""
import hashlib
import itertools
import json
import logging
import os
import re
import sys
import time

import requests
from requests.auth import HTTPBasicAuth

import uploader.upload.utils as utl


reddit_path = 'reddit'
config_path = os.path.join(utl.config_file_path, reddit_path)
base_url = 'https://ads-api.reddit.com/api/v3'
CHANNEL = 'Reddit'

REQUEST_TIMEOUT = (10, 30)
UPLOAD_TIMEOUT = (10, 120)
MAX_LIST_PAGES = 100
LIST_PAGE_SIZE = 100
MEDIA_POLL_ATTEMPTS = 30
MEDIA_POLL_SECONDS = 2
IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.webp')
VIDEO_EXTENSIONS = ('.mp4', '.mov', '.webm', '.m4v')
DEFAULT_USER_AGENT = 'web:liquid-advertising-uploader:v1.0'
COMMUNITY_BATCH = 100
SUGGEST_BATCH = 5
READ_ERRORS = (utl.UploaderAuthError, ValueError,
               requests.exceptions.RequestException)


def normalize_community(value):
    """A community cell as a bare name without ``r/``; '' for a value
    that is not a community name."""
    name = str(value or '').strip().lstrip('/')
    if name.lower().startswith('r/'):
        name = name[2:]
    if not name:
        return ''
    if re.search(r'[\s/]', name):
        logging.warning('%r is not a Reddit community name; skipped.',
                        value)
        return ''
    return name


def _community(row):
    """One targeting community as the readers return it: ``id``,
    ``name`` as Reddit spells it, ``subscribers`` and ``categories``."""
    return {'id': row.get('id'), 'name': row.get('name'),
            'subscribers': row.get('subscriber_count'),
            'categories': list(row.get('categories') or [])}


def _unique_communities(names):
    """``names`` as bare community names, blanks and repeats dropped;
    Reddit names ignore case, so a repeat is a casefold repeat."""
    seen = {}
    for name in map(normalize_community, names or []):
        if name:
            seen.setdefault(name.casefold(), name)
    return list(seen.values())


def _field_errors(err):
    """``field: message`` for each field a v3 refusal names, joined; ''
    when it names none."""
    fields = err.get('fields') if isinstance(err, dict) else None
    return '; '.join(
        f"{item.get('field') or 'request'}: {item['message']}"
        for item in fields or []
        if isinstance(item, dict) and item.get('message'))


def _to_iso(value, end_of_day=False, timezone=utl.FLIGHT_TIMEZONE):
    """Reddit wants ISO-8601 UTC timestamps; plan-derived flight dates
    arrive as MM/DD/YYYY strings (or excel datetimes) typed in the ad
    account's time, and an end date means the end *of* that day."""
    if value is None or value == '':
        return None
    try:
        stamp = utl.flight_utc(value, timezone, end_of_day)
    except utl.FLIGHT_TIME_ERRORS:
        return None
    return stamp.strftime('%Y-%m-%dT%H:%M:%SZ')


def _account_timezone(api):
    """The ad account timezone ``api`` can read; the Pacific default
    for an api without the read, such as a test stub."""
    read = getattr(api, 'account_timezone', None)
    return read() if read else utl.FLIGHT_TIMEZONE


def _to_micros(value):
    """Reddit money fields (spend_cap, budget_value, bids) are in
    microcurrency — 1/1,000,000 of the account currency unit. Plan
    costs arrive as whole-currency floats, so scale up; mirrors the
    processor's RedApi dividing reported spend by 1,000,000 on the way
    in. Returns None for blank/non-numeric values."""
    try:
        return int(round(float(value) * 1_000_000))
    except (TypeError, ValueError):
        return None


# Reddit's post CTA is a fixed Title-Case enum; map free text onto it.
_CTA_CANONICAL = [
    'Apply Now', 'Contact Us', 'Download', 'Get a Quote', 'Get Showtimes',
    'Install', 'Learn More', 'Order Now', 'Play Now', 'Pre-order Now',
    'See Menu', 'Shop Now', 'Sign Up', 'View More', 'Watch Now', 'Book Now',
    'Buy Tickets', 'Get Directions', 'Listen Now', 'Read More', 'Subscribe',
    'Visit Store', 'Donate Now', 'Remind Me']
CTA_DEFAULT = 'Learn More'
_CTA_BY_LOWER = {c.lower(): c for c in _CTA_CANONICAL}

# Plan/processor platform tokens -> Reddit v3 ad-group platform enum.
PLATFORM_MAP = {
    'ALL': 'ALL', 'DESKTOP': 'DESKTOP', 'MOBILE': 'MOBILE_NATIVE',
    'MOBILE_NATIVE': 'MOBILE_NATIVE', 'MOBILE_WEB': 'MOBILE_WEB',
    'APP': 'MOBILE_NATIVE', 'WEB': 'MOBILE_WEB'}

# Common country names -> ISO 3166-1 alpha-2 for the live geo resolver.
COUNTRY_TO_ISO = {
    'united states': 'US', 'usa': 'US', 'u.s.': 'US',
    'united kingdom': 'GB', 'great britain': 'GB',
    'canada': 'CA', 'australia': 'AU', 'germany': 'DE', 'france': 'FR',
    'spain': 'ES', 'italy': 'IT', 'netherlands': 'NL', 'sweden': 'SE',
    'norway': 'NO', 'denmark': 'DK', 'finland': 'FI', 'ireland': 'IE',
    'india': 'IN', 'japan': 'JP', 'brazil': 'BR', 'mexico': 'MX',
    'new zealand': 'NZ'}


def _to_cta(value):
    """Nearest valid Reddit CTA enum string for a free-text value,
    defaulting to 'Learn More'."""
    return _CTA_BY_LOWER.get(str(value or '').strip().lower(), CTA_DEFAULT)


def _extract_error(body):
    """Reddit Ads v3 returns failures as a single ``{"error": {"message",
    "code"}}`` object. Tolerate a legacy ``{"errors": [...]}`` list too so
    older surfaces don't collapse to 'Unknown error'."""
    if not isinstance(body, dict):
        return {}
    err = body.get('error')
    if isinstance(err, dict):
        return err
    errs = body.get('errors')
    if isinstance(errs, list) and errs and isinstance(errs[0], dict):
        return errs[0]
    return {}


_GENERIC_ERROR_MESSAGES = ('', 'bad request', 'unknown error',
                           'unknown error from reddit ads')


def _populate_reddit_result(result, response):
    """Fill ``result`` from a Reddit Ads create response. Success:
    ``{"data": {"id": ...}}``. Failure: ``{"error": {...}}``; generic
    messages ('Bad Request') get the raw body appended — the
    actionable reason often lives outside error.message."""
    body = utl.response_body(response)
    data = body.get('data') or {}
    if isinstance(data, dict) and data.get('id'):
        result['platform_id'] = data['id']
        result['status'] = 'created'
        return
    err = _extract_error(body)
    message = str(err.get('message') or err.get('detail') or '').strip()
    http_status = getattr(response, 'status_code', '') or ''
    fields = _field_errors(err)
    if fields:
        message = f'Reddit refused {fields}'
    elif message.lower() in _GENERIC_ERROR_MESSAGES:
        try:
            raw = json.dumps(body)[:500]
        except (TypeError, ValueError):
            raw = ''
        if raw and raw != '{}':
            message = '{} (HTTP {}): {}'.format(
                message or 'Reddit Ads error', http_status, raw)
    utl.fail_result(result, message or 'Unknown error from Reddit Ads',
                    err.get('code'))
    logging.warning('Reddit create failed (HTTP %s): %s',
                    http_status, result['error_message'])


def _refused_list(url, resp):
    """Why Reddit refused a list, from the body's error message when it
    sent one, so a refusal is never read as an empty account."""
    try:
        message = ((resp.json() or {}).get('error') or {}).get('message')
    except ValueError:
        message = ''
    return (f'Reddit refused {url} (HTTP {resp.status_code}): '
            f'{message or "check the credential can see this ad account"}')


class _InvalidMedia(ValueError):
    """A terminal media refusal, for which the upload must be replaced."""


def _media_response(response):
    """Reject refused or malformed media responses before using their data."""
    body = utl.response_body(response)
    error = _extract_error(body)
    status = getattr(response, 'status_code', 0)
    if error or not 200 <= status < 300 or 'data' not in body:
        reason = _field_errors(error) or error.get('message')
        raise ValueError(
            f'Reddit creative upload failed: {reason or "invalid response"}'
            f' (HTTP {status}). Check the file and Reddit account access.')
    return body['data']


class CreativeUpload(utl.BaseCreativeStore):
    """Persist completed assets and pending handles, scoped to file/profile."""

    id_cols = ('id', 'profile_id', 'file_signature', 'poster_url',
               'media_url', 'media_type', 'upload_id', 'poster_media_url')

    def __init__(self, id_file_name, creative_path='creative/'):
        super().__init__(id_file_name, creative_path)
        self._digests = {}

    def _file_digest(self, path):
        stat = os.stat(path)
        stamp = stat.st_mtime_ns, stat.st_size
        cached_stamp, digest = self._digests.get(path, (None, None))
        if cached_stamp != stamp:
            with open(path, 'rb') as source:
                digest = hashlib.file_digest(source, 'sha256').hexdigest()
            self._digests[path] = stamp, digest
        return digest

    def resolve(self, api, profile_id, filename, poster_url=''):
        """Upload one original file or resume its saved processing handle."""
        path = os.path.realpath(os.path.join(self.creative_path, filename))
        root = os.path.realpath(self.creative_path)
        if os.path.dirname(path) != root:
            raise ValueError('Choose a file from the Creative tab.')
        ext = os.path.splitext(filename)[1].lower()
        if ext not in IMAGE_EXTENSIONS + VIDEO_EXTENSIONS:
            raise ValueError('Choose an image or video for the Reddit ad.')
        kind = 'VIDEO' if ext in VIDEO_EXTENSIONS else 'IMAGE'
        digest = self._file_digest(path)
        scope = {'profile_id': profile_id, 'file_signature': digest,
                 'poster_url': poster_url if kind == 'VIDEO' else ''}
        record = self.records.get(filename) or {}
        if any((record.get(k) or '') != v for k, v in scope.items()):
            record = dict(scope, media_type=kind)
            self.records[filename] = record
            self.write()
        if record.get('id') and record.get('media_url'):
            return record['media_url'], kind
        if not record.get('upload_id'):
            if not api.creative_host:
                raise ValueError(
                    'Upload Reddit creative files through the app worker, '
                    'or choose a hosted URL or Reddit library asset.')
            if kind == 'VIDEO' and not poster_url:
                if not api.creative_cover:
                    raise ValueError(
                        'Choose a Cover Image or run Ad through the app '
                        'to make a cover from the video.')
                poster_url = api.creative_cover(path)
            media_url = api.creative_host(path)
            record['upload_id'] = api.upload_creative(
                profile_id, filename, media_url, kind, poster_url)
            self.records[filename] = record
            self.write()
        return self._finish(api, filename, record)

    def _finish(self, api, filename, record):
        try:
            asset = api.wait_creative(record['upload_id'])
        except _InvalidMedia:
            record['upload_id'] = None
            self.write()
            raise
        poster = (asset.get('poster') or {}).get('permanent_url') or ''
        if record['media_type'] == 'VIDEO' and not (
                poster or record.get('poster_url')):
            raise ValueError(
                'Reddit did not return the automatic video cover. '
                'Choose an image in Cover Image, then run Ad again.')
        record.update(id=asset['id'], poster_media_url=poster,
                      media_url=asset['media']['permanent_url'])
        self.records[filename] = record
        self.write()
        return record['media_url'], record['media_type']


class RedditApi(object):
    def __init__(self, config_file=None, creative_host=None,
                 creative_cover=None):
        self.creative_host = creative_host
        self.creative_cover = creative_cover
        self.creative_store = None
        self.config_file = config_file
        self.config = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_url = None
        self.business_id = None
        self.ad_account_id = None
        self.username = None
        self.user_agent = None
        self.config_list = None
        self.client = None
        self.cam_dict = {}
        self.adgroup_dict = {}
        self.ad_dict = {}
        self.id_dict_scope = {}
        self.profile_dict = {}
        self.post_dict = {}
        self.asset_dict = {}
        self._geo_cache = {}
        self._community_ok = {}
        self._account_ready = False
        self.timezone = None
        self.r = None
        if self.config_file:
            self.input_config(self.config_file)

    def input_config(self, config):
        if str(config) == 'nan':
            logging.warning(
                'Reddit config file not in vendor matrix. Aborting.')
            sys.exit(0)
        logging.info(f'Loading Reddit config file: {config}')
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
        self.client_id = self.config.get('client_id', '')
        self.client_secret = self.config.get('client_secret', '')
        self.access_token = self.config.get('access_token', '')
        self.refresh_token = self.config.get('refresh_token', '')
        self.refresh_url = self.config.get(
            'refresh_url', 'https://www.reddit.com/api/v1/access_token')
        self.business_id = self.config.get('business_id', '')
        self.ad_account_id = self.config.get('ad_account_id', '')
        self.username = self.config.get('username', '')
        self.user_agent = (self.config.get('user_agent')
                           or self.config.get('redirect_uri')
                           or DEFAULT_USER_AGENT)
        self.config_list = [
            self.client_id, self.client_secret,
            self.refresh_token, self.refresh_url]

    def check_config(self):
        for item in self.config_list:
            if item == '':
                logging.warning(
                    f'{item} not in Reddit config file. Aborting.')
                sys.exit(0)

    def get_client(self, force=False):
        """Exchange the refresh token for an access token and build an
        authed ``requests`` session. Reddit's token endpoint *requires*
        HTTP Basic Auth (client_id:client_secret) and a unique
        User-Agent — this mirrors the processor's
        ``RedApi.get_access_token``, the proven flow. The previous
        OAuth2Session call put the client creds in the POST body with
        no User-Agent, which Reddit rejects with a 401."""
        if self.client and not force:
            return
        headers = {'Content-Type': 'application/x-www-form-urlencoded',
                   'User-Agent': self.user_agent}
        data = {'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token}
        auth = HTTPBasicAuth(self.client_id, self.client_secret)
        try:
            r = requests.post(self.refresh_url, headers=headers, data=data,
                              auth=auth, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.RequestException as e:
            self.client = None
            raise utl.UploaderAuthError(
                'Reddit OAuth token request failed: {}'.format(e))
        try:
            token = r.json()
        except ValueError:
            token = {}
        access_token = (token or {}).get('access_token')
        if r.status_code != 200 or not access_token:
            self.client = None
            raise utl.UploaderAuthError(self._auth_error_message(r, token))
        self.access_token = access_token
        self.client = requests.Session()
        self.client.headers.update(
            {'Authorization': 'Bearer {}'.format(access_token),
             'User-Agent': self.user_agent,
             'Accept': 'application/json'})

    @staticmethod
    def _auth_error_message(response, token):
        """Secret-free, accurate diagnosis of a refused token request:
        ``invalid_grant`` means the refresh token is dead, while
        ``invalid_client`` / a bare 401 means client_id/client_secret
        weren't accepted as HTTP Basic Auth — different fixes."""
        status = getattr(response, 'status_code', '')
        error = (token or {}).get('error')
        if error == 'invalid_grant':
            hint = ('refresh token is invalid, expired or revoked — '
                    're-authorize the app and update redditconfig.json.')
        elif error == 'invalid_client' or status == 401:
            hint = ('client_id/client_secret were not accepted (HTTP '
                    'Basic Auth) — verify they match the authorized '
                    'Reddit app.')
        else:
            hint = 're-authorize the app and update redditconfig.json.'
        return ('Reddit OAuth refresh failed (HTTP {}, error={!r}): {}'
                .format(status, error, hint))

    def _entity_url(self, segment):
        return '{}/ad_accounts/{}/{}'.format(
            base_url, self.ensure_account_id(), segment)

    def ensure_account_id(self):
        """The ad-account id for the API path, resolved against the
        accounts the token can actually reach. Accepts the configured
        ``ad_account_id`` (with or without the ``a2_`` prefix) or the
        ``username`` (matched leniently to the account name), and always
        returns the canonical id Reddit expects. Fails early with the real
        account list instead of letting every call 404 as 'Account not
        found'. Resolved once per run."""
        if self._account_ready:
            return self.ad_account_id
        accounts = self.list_ad_accounts()
        resolved = (self._match_account(accounts, self.ad_account_id)
                    or self._match_account(accounts, self.username))
        if not resolved:
            raise utl.UploaderAuthError(self._account_error(accounts))
        self.ad_account_id = resolved
        self._account_ready = True
        logging.info('Using Reddit ad_account_id: %s', resolved)
        return resolved

    def account_timezone(self):
        """The ad account's timezone name, read once from its record,
        else the Pacific default. Flight cells are typed in account time
        and Reddit reads a timestamp as UTC, so every write converts
        through this zone."""
        if self.timezone:
            return self.timezone
        self.timezone = utl.FLIGHT_TIMEZONE
        if not (self.client_id and self.refresh_token):
            return self.timezone
        try:
            url = '{}/ad_accounts/{}'.format(
                base_url, self.ensure_account_id())
            body = utl.response_body(self._get(url))
        except (requests.exceptions.RequestException,
                utl.UploaderAuthError) as e:
            logging.warning('Reddit ad account timezone not read: %s', e)
            return self.timezone
        zone = utl.known_timezone(
            (body.get('data') or {}).get('time_zone_id'))
        if zone:
            self.timezone = zone
        else:
            logging.warning(
                'Reddit ad account %s reported no known timezone; flight '
                'times are read as %s.', self.ad_account_id, self.timezone)
        return self.timezone

    def _list_business_ids(self):
        """All business ids on the token, via ``me/businesses``."""
        url = '{}/me/businesses'.format(base_url)
        return [row['id'] for row in self._paginate(url) if row.get('id')]

    def list_ad_accounts(self):
        """``[{'id','name'}]`` for every ad account the token can reach
        across its businesses — the set valid for the API path."""
        accounts = []
        for business_id in self._list_business_ids():
            url = '{}/businesses/{}/ad_accounts'.format(base_url, business_id)
            try:
                rows = (self._get(url).json() or {}).get('data') or []
            except ValueError:
                continue
            accounts += [{'id': row['id'], 'name': row.get('name', '')}
                         for row in rows if row.get('id')]
        return accounts

    @staticmethod
    def _bare_id(value):
        """An ad-account id without its ``a2_`` prefix, lowercased — users
        often paste the bare id, so match it prefix-insensitively."""
        text = str(value or '').strip().lower()
        return text[3:] if text.startswith('a2_') else text

    @staticmethod
    def _norm_name(value):
        """Lowercased alphanumerics of an account name, so a ``username``
        like ``liquid_etl`` matches an account named ``Liquid ETL``."""
        return re.sub(r'[^a-z0-9]', '', str(value or '').lower())

    def _match_account(self, accounts, value):
        """Canonical id of the account ``value`` identifies — by id (with
        or without the ``a2_`` prefix) or by account name (case / space /
        underscore-insensitive). '' when nothing matches."""
        if not str(value or '').strip():
            return ''
        want_id, want_name = self._bare_id(value), self._norm_name(value)
        for account in accounts:
            if (self._bare_id(account['id']) == want_id
                    or self._norm_name(account['name']) == want_name):
                return account['id']
        return ''

    def _account_error(self, accounts):
        """Actionable message naming the ad accounts the token can reach,
        so a wrong ad_account_id/username is a one-line config fix."""
        if not accounts:
            return ('Reddit token reached no ad accounts — confirm the '
                    'refresh token is authorized for this business with the '
                    'adsedit scope, then re-check redditconfig.json.')
        listing = ', '.join("{} ('{}')".format(a['id'], a['name'])
                            for a in accounts)
        return ('Reddit ad_account_id {!r} / username {!r} matched none of '
                'the ad accounts this token can reach. Set "ad_account_id" '
                'in redditconfig.json to one of: {}'
                .format(self.ad_account_id, self.username, listing))

    def create_entity(self, entity, entity_name=''):
        url = self._entity_url(entity_name)
        return self._post(url, body={'data': entity.upload_dict})

    def _post(self, url, body=None):
        self.get_client()
        try:
            self.r = self.client.post(
                url, json=body or {}, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.SSLError as e:
            logging.warning(f'Reddit SSLError: {e}')
            time.sleep(30)
            self.r = self._post(url, body=body)
        return self.r

    def _get(self, url, params=None):
        self.get_client()
        return self.client.get(
            url, params=params or {}, timeout=REQUEST_TIMEOUT)

    def _patch(self, url, body=None):
        self.get_client()
        try:
            self.r = self.client.patch(
                url, json=body or {}, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.SSLError as e:
            logging.warning(f'Reddit SSLError: {e}')
            time.sleep(30)
            self.r = self._patch(url, body=body)
        return self.r

    @staticmethod
    def get_id(dict_o, match, match_name='name'):
        return [k for k, v in dict_o.items() if v.get(match_name) == match]

    def _paginate(self, url, params=None):
        """Yield every ``data`` row across a v3 list endpoint, following
        ``pagination.next_url`` directly (the cursor is baked into it — the
        docs warn against rebuilding it from query params). Bounded by
        ``MAX_LIST_PAGES`` so a contract change can never spin the worker
        forever."""
        page_params = dict(params or {})
        for _ in range(MAX_LIST_PAGES):
            resp = self._get(url, params=page_params)
            if resp.status_code != 200:
                raise utl.UploaderAuthError(_refused_list(url, resp))
            try:
                body = resp.json()
            except ValueError:
                return
            yield from (body.get('data') or [])
            url = (body.get('pagination') or {}).get('next_url')
            if not url:
                return
            page_params = None  # cursor is in next_url; don't re-append
        logging.warning(
            'Reddit list hit the %s-page cap; results may be truncated.',
            MAX_LIST_PAGES)

    def _list(self, entity_name, params=None):
        """Ad-account resource list keyed by id (campaigns, ad_groups,
        ads, funding_instruments, profiles, pixels)."""
        return {row['id']: row
                for row in self._paginate(self._entity_url(entity_name),
                                          params) if row.get('id')}

    def set_id_dict(self, kind=None, filter_id=None):
        if kind == 'campaign':
            self.cam_dict = self._list('campaigns')
        elif kind == 'adgroup':
            params = {'campaign_id': filter_id} if filter_id else None
            self.adgroup_dict = self._list('ad_groups', params=params)
        elif kind == 'ad':
            params = {'ad_group_id': filter_id} if filter_id else None
            self.ad_dict = self._list('ads', params=params)
        else:
            return
        self.id_dict_scope[kind] = str(filter_id or '')

    def ensure_id_dict(self, kind, filter_id=None):
        """Cache existing objects, including empty lists, by parent scope."""
        attrs = {'campaign': 'cam_dict', 'adgroup': 'adgroup_dict',
                 'ad': 'ad_dict'}
        if self.id_dict_scope.get(kind) != str(filter_id or ''):
            self.set_id_dict(kind, filter_id=filter_id)
        return getattr(self, attrs[kind])

    live_segments = {'Campaign': ('campaigns', None),
                     'Adset': ('ad_groups', 'campaign_id'),
                     'Ad': ('ads', 'ad_group_id')}

    def list_recent(self, level, limit=25, parent_ids=None):
        """The ad account's ``level`` objects for the copy-from-account
        picker, most recently changed first. The lists take no sort and
        one parent per request, so each parent's first
        ``utl.RECENT_SCAN_CAP`` rows are read."""
        if level not in self.live_segments:
            return []
        segment, parent_param = self.live_segments[level]
        scan = max(limit, utl.RECENT_SCAN_CAP)
        param_sets = [None]
        if parent_ids and parent_param:
            param_sets = [{parent_param: str(x)} for x in parent_ids]
        url = self._entity_url(segment)
        found = itertools.chain.from_iterable(
            itertools.islice(self._paginate(url, params), scan)
            for params in param_sets)
        return utl.recent_rows(found, limit, parent_key=parent_param,
                               created_key='created_at',
                               updated_key='modified_at')

    def _id_name_options(self, segment, *name_keys):
        """``[{'id','name'}]`` for an ad-account list, labelled by the
        first present ``name_keys`` value (else the id) — the shared shape
        every picker reader returns."""
        options = []
        for rid, row in self._list(segment).items():
            row = row or {}
            name = next((row[k] for k in name_keys if row.get(k)), rid)
            options.append({'id': rid, 'name': name})
        return options

    def get_funding_instruments(self):
        """Funding instruments on the ad account, labelled by name then
        currency. Feeds the campaign config's ``funding_instrument_id``
        picker — the id every Reddit campaign needs to be billable."""
        return self._id_name_options('funding_instruments', 'name', 'currency')

    def get_pixels(self):
        """Conversion pixels on the ad account, for the ad-group
        ``conversion_pixel_id`` picker — Reddit makes this field required
        for all ad groups starting 2026-07-13."""
        return self._id_name_options('pixels', 'name')

    def get_profiles(self):
        """Profiles on the ad account — the author a Post is created
        under. Feeds the ad-config profile picker."""
        return self._id_name_options('profiles', 'name', 'username')

    def resolve_profile_id(self, value):
        """Profile id for a name/username/id: pass through a value that is
        already a known profile id, else match by name/username
        (case-insensitive). '' when nothing matches."""
        value = str(value or '').strip()
        if not value:
            return ''
        if not self.profile_dict:
            self.profile_dict = self._list('profiles')
        if value in self.profile_dict:
            return value
        target = value.lower()
        for pid, row in self.profile_dict.items():
            names = {str((row or {}).get('name', '')).lower(),
                     str((row or {}).get('username', '')).lower()}
            if target in names:
                return pid
        return ''

    def get_creative_assets(self, profile_id):
        """Asset-library assets for a profile, for the creative picker."""
        return [{'id': aid, 'name': (asset or {}).get('name', aid)}
                for aid, asset
                in self.list_creative_assets(profile_id).items()]

    def list_creative_assets(self, profile_id):
        """Asset-library assets under a profile, keyed by id. Cached per
        profile. Each asset carries ``media.permanent_url`` — the hosted
        url a Post references as its ``media_url``."""
        if not profile_id:
            return {}
        if profile_id in self.asset_dict:
            return self.asset_dict[profile_id]
        url = '{}/profiles/{}/creative_assets'.format(base_url, profile_id)
        assets = {}
        for row in self._paginate(url):
            # List wraps each asset as {'result': ...}; Get returns it bare.
            asset = (row.get('result')
                     if isinstance(row, dict) and 'result' in row else row)
            aid = (asset or {}).get('id')
            if aid:
                assets[aid] = asset
        self.asset_dict[profile_id] = assets
        return assets

    def resolve_asset_media(self, profile_id, name, poster_url=''):
        """Resolve local originals, library names or URLs to media and type.

        Blank local video posters use the worker's cover callback.
        """
        value = str(name or '').strip()
        if not value:
            return '', ''
        if value.lower().startswith(('http://', 'https://')):
            return value, ''
        path = os.path.join('creative', value)
        if os.path.isfile(path):
            if not self.creative_store:
                self.creative_store = CreativeUpload(
                    id_file_name='reddit_creative_ids.csv')
            return self.creative_store.resolve(
                self, profile_id, value, poster_url)
        target = value.lower()
        for asset in self.list_creative_assets(profile_id).values():
            if str((asset or {}).get('name', '')).lower() != target:
                continue
            media = (asset or {}).get('media') or {}
            url = media.get('permanent_url') or ''
            mime = str(media.get('mime_type') or '')
            kind = asset.get('type') or (
                'VIDEO' if mime.startswith('video') else 'IMAGE')
            return url, kind
        return '', ''

    def resolved_asset_poster(self, profile_id, name):
        """A resolved video's permanent cover, from its store or library."""
        value = str(name or '').strip()
        if os.path.isfile(os.path.join('creative', value)):
            record = (self.creative_store.records.get(value) or {}
                      if self.creative_store else {})
            if record.get('profile_id') == profile_id:
                return record.get('poster_media_url') or ''
            return ''
        if value.lower().startswith(('http://', 'https://')):
            return ''
        for asset in self.list_creative_assets(profile_id).values():
            if str(asset.get('name') or '').lower() == value.lower():
                return (asset.get('poster') or {}).get('permanent_url') or ''
        return ''

    def upload_creative(self, profile_id, name, media_url, media_type,
                        poster_url=''):
        """Start a URL-based asset upload; return its pollable upload ID."""
        if media_type == 'VIDEO' and not poster_url:
            raise ValueError('Choose a thumbnail for the Reddit video.')
        asset = {'name': name, 'type': media_type,
                 'media': {'type': 'URL', 'url': media_url}}
        if media_type == 'VIDEO':
            asset['poster'] = {'type': 'URL', 'url': poster_url}
        url = f'{base_url}/profiles/{profile_id}/creative_assets/uploads'
        data = _media_response(self._post(url, body={'data': [asset]}))
        if (not isinstance(data, list) or not data
                or not isinstance(data[0], dict) or not data[0].get('id')):
            raise ValueError('Reddit did not return a creative upload ID.')
        return data[0]['id']

    def wait_creative(self, upload_id):
        """Return usable media; pending uploads stay resumable on timeout."""
        url = f'{base_url}/creative_assets/uploads/{upload_id}'
        for attempt in range(MEDIA_POLL_ATTEMPTS):
            data = _media_response(self._get(url))
            if not isinstance(data, dict):
                raise ValueError('Reddit returned an invalid upload status.')
            asset = data.get('result') or {}
            status = data.get('status')
            if status == 'ACTIVE':
                media = asset.get('media') or {}
                if asset.get('id') and media.get('permanent_url'):
                    return asset
                raise ValueError('Reddit returned an asset without media.')
            if status != 'PROCESSING_MEDIA':
                reason = _field_errors({'fields': asset.get('errors')})
                raise _InvalidMedia(
                    f'Reddit could not process the creative: '
                    f'{reason or status or "unknown status"}. '
                    'Check the file and upload it again.')
            if attempt < MEDIA_POLL_ATTEMPTS - 1:
                time.sleep(MEDIA_POLL_SECONDS)
        raise TimeoutError(
            'Reddit is still processing this creative. Run Ad again to '
            'resume the upload.')

    def resolve_country_geo(self, value):
        """Country-level geolocation id for a country name or ISO-2 code,
        '' when unknown. Cached per code. Reddit targets geolocations by
        id while the plan supplies country names — resolve them live."""
        text = str(value or '').strip()
        if not text:
            return ''
        code = COUNTRY_TO_ISO.get(text.lower(), text.upper())
        if len(code) != 2:
            return ''
        if code in self._geo_cache:
            return self._geo_cache[code]
        gid, rows = '', []
        try:
            url = '{}/targeting/geolocations'.format(base_url)
            rows = (self._get(url, params={'country': code}).json()
                    or {}).get('data') or []
        except (ValueError, requests.exceptions.RequestException) as e:
            logging.warning(
                'Reddit geolocation lookup failed for %s: %s', code, e)
        # Prefer the country-level row (no city/region); else the first.
        for row in rows:
            if not row.get('city') and not row.get('region'):
                gid = row.get('id') or ''
                break
        if not gid and rows:
            gid = rows[0].get('id') or ''
        self._geo_cache[code] = gid
        return gid

    def _communities(self, segment, params):
        """Community rows from one ``targeting/communities`` reader."""
        url = f'{base_url}/targeting/communities{segment}'
        return [_community(row) for row in self._paginate(url, params)
                if row.get('name')]

    def get_communities(self, names):
        """The communities among ``names`` an ad group can target, as
        Reddit spells them; a name left out would fail the upload."""
        wanted = _unique_communities(names)
        found = []
        for start in range(0, len(wanted), COMMUNITY_BATCH):
            batch = wanted[start:start + COMMUNITY_BATCH]
            found += self._communities('', {'names': ','.join(batch)})
        return found

    def search_communities(self, query):
        """Targetable communities Reddit finds for a search phrase."""
        text = re.sub(r'[_\s]+', ' ', str(query or '')).strip()
        return self._communities('/search', {'query': text}) if text else []

    def suggest_communities(self, names):
        """Communities Reddit suggests alongside ``names``, without
        ``names`` themselves, which its answer can repeat."""
        wanted = _unique_communities(names)
        given = {name.casefold() for name in wanted}
        found = {}
        for start in range(0, len(wanted), SUGGEST_BATCH):
            batch = wanted[start:start + SUGGEST_BATCH]
            for row in self._communities('/suggestions',
                                         {'names': ','.join(batch)}):
                if row['name'].casefold() not in given:
                    found.setdefault(row['name'].casefold(), row)
        return list(found.values())

    def community_candidates(self, name):
        """``name``'s own community when targetable, then what a search
        for it finds."""
        exact = self.get_communities([name])
        known = {row['id'] for row in exact}
        return exact + [row for row in self.search_communities(name)
                        if row['id'] not in known]

    def untargetable_communities(self, names):
        """The ``names`` Reddit cannot target, checked once per name per
        run; [] when the lookup cannot be read, which leaves the call to
        Reddit's own check on the ad group."""
        wanted = _unique_communities(names)
        unknown = [name for name in wanted
                   if name.casefold() not in self._community_ok]
        if unknown:
            try:
                found = self.get_communities(unknown)
            except READ_ERRORS as e:
                logging.warning('Reddit community lookup failed: %s', e)
                return []
            ok = {row['name'].casefold() for row in found}
            self._community_ok.update(
                {name.casefold(): name.casefold() in ok for name in unknown})
        return [name for name in wanted
                if not self._community_ok[name.casefold()]]

    def create_post(self, profile_id, post_dict):
        """Create a post and retain it in an already loaded profile cache."""
        url = f'{base_url}/profiles/{profile_id}/posts'
        resp = self._post(url, body={'data': post_dict})
        created = utl.response_body(resp).get('data') or {}
        if profile_id in self.post_dict and created.get('id'):
            self.post_dict[profile_id][created['id']] = dict(
                post_dict, id=created['id'])
        return resp

    def list_posts(self, profile_id):
        """Cache posts by profile so each ad avoids another paginated read."""
        if not profile_id:
            return {}
        if profile_id not in self.post_dict:
            url = f'{base_url}/profiles/{profile_id}/posts'
            self.post_dict[profile_id] = {
                row['id']: row for row in self._paginate(
                    url, {'page.size': LIST_PAGE_SIZE}) if row.get('id')}
        return self.post_dict[profile_id]

    def probe_account(self):
        """(ok, message) — verify the ad account is reachable, for the
        live pre-flight checks."""
        try:
            r = self._get(self._entity_url('funding_instruments'))
            body = r.json() if r is not None else {}
            if not isinstance(body, dict):
                body = {}
            err = _extract_error(body)
            if err:
                return False, str(err.get('message') or err)
            return True, ''
        except Exception as e:
            return False, str(e)

    entity_segments_by_level = {'Campaign': 'campaigns',
                                'Adset': 'ad_groups', 'Ad': 'ads'}

    def update_statuses(self, object_level, platform_ids, activate=True):
        """PATCH ``configured_status`` on existing objects by id.
        Returns one dict per id: {'platform_id', 'status'
        ('updated'|'failed'), 'error_code', 'error_message'}."""
        segment = self.entity_segments_by_level.get(object_level)
        status = 'ACTIVE' if activate else 'PAUSED'
        results = []
        for pid in platform_ids:
            result = utl.new_update_result(pid)
            if not segment:
                results.append(utl.fail_result(
                    result, f'Unknown Reddit level: {object_level}'))
                continue
            try:
                url = f'{self._entity_url(segment)}/{pid}'
                r = self._patch(
                    url, body={'data': {'configured_status': status}})
                self._fail_if_refused(result, r)
            except Exception as e:
                utl.fail_result(result, e)
            results.append(result)
        return results

    @staticmethod
    def _fail_if_refused(result, response):
        """Mark a PATCH result failed when Reddit refuses it."""
        err = _extract_error(utl.response_body(response))
        status = getattr(response, 'status_code', None)
        if response is not None and not err and (status or 0) < 400:
            return result
        return utl.fail_result(
            result, err.get('message') or err.get('detail')
            or 'Unknown error from Reddit Ads',
            err.get('code') or status)

    def update_object(self, object_level, platform_id, changes,
                      context=None):
        """Push one object's whitelisted edits in a single PATCH."""
        result = utl.new_update_result(platform_id)
        try:
            body = self.update_body(object_level, changes,
                                    self.account_timezone())
            segment = self.entity_segments_by_level[object_level]
            url = f'{base_url}/{segment}/{platform_id}'
            self._fail_if_refused(
                result, self._patch(url, body={'data': body}))
        except Exception as e:
            utl.fail_result(result, e)
        return result

    @staticmethod
    def update_body(object_level, changes, timezone=utl.FLIGHT_TIMEZONE):
        """Build one complete Reddit PATCH payload; dates are read in
        the ad account's ``timezone``."""
        fields = UPDATE_COLUMN_FIELDS.get(object_level)
        if not fields:
            raise ValueError(
                f'No update support for Reddit level: {object_level}')
        body = {}
        for col, value in (changes or {}).items():
            if col not in fields:
                raise ValueError(f'Column not updatable: {col}')
            body[fields[col]] = RedditApi.update_value(col, value, timezone)
        if not body:
            raise ValueError('No changes supplied.')
        return body

    @staticmethod
    def update_value(col, value, timezone=utl.FLIGHT_TIMEZONE):
        """Transform spreadsheet money and dates for Reddit."""
        if col in MONEY_UPDATE_COLS:
            micros = _to_micros(value)
            if micros is None:
                raise ValueError(
                    f'Non-numeric money value for {col}: {value!r}')
            return micros
        if col in DATE_UPDATE_COLS:
            iso = _to_iso(value, col == AdGroupUpload.end_time, timezone)
            if iso is None:
                raise ValueError(
                    f'Unparseable datetime for {col}: {value!r}')
            return iso
        return str(value)


class CampaignUpload(utl.BaseUploadConfig):
    config_dir = config_path
    config_label = 'Reddit campaign config'
    file_name = 'campaign_upload.xlsx'
    name = 'name'
    objective = 'objective'
    status = 'configured_status'
    funding_instrument_id = 'funding_instrument_id'
    spend_cap = 'spend_cap'
    snapshot_cols = [objective, status, funding_instrument_id, spend_cap]

    @staticmethod
    def settings_from_live(fields):
        """The live campaign's objective and funding instrument as
        level-file cells; name, status and spend cap are not copied."""
        cu = CampaignUpload
        return utl.live_settings({col: fields.get(col) for col in (
            cu.objective, cu.funding_instrument_id)})

    def upload_all_campaigns(self, api):
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, c_id in enumerate(self.config):
            cam = Campaign(self.config[c_id], api=api)
            logging.info(
                f'Uploading Reddit campaign {idx + 1} of {total}: '
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
        _populate_reddit_result(
            result, api.create_entity(campaign, entity_name='campaigns'))
        if result['status'] == 'created':
            campaign.id = result['platform_id']
        return result


class Campaign(object):
    __slots__ = ['name', 'objective', 'configured_status',
                 'funding_instrument_id', 'spend_cap',
                 'effective_status', 'upload_dict', 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.objective = 'CLICKS'
        self.configured_status = 'PAUSED'
        self.funding_instrument_id = None
        self.spend_cap = None
        self.effective_status = None
        utl.apply_row(self, row_dict)
        self.api = api
        self.upload_dict = self.create_cam_dict()

    def create_cam_dict(self):
        if not self.name:
            return {}
        d = {
            'name': str(self.name),
            'objective': str(self.objective or 'CLICKS'),
            'configured_status': str(self.configured_status or 'PAUSED'),
        }
        if self.funding_instrument_id:
            d['funding_instrument_id'] = str(self.funding_instrument_id)
        if self.spend_cap:
            micros = _to_micros(self.spend_cap)
            if micros is not None:
                d['spend_cap'] = micros
        return d

    def check_exists(self, api):
        if not api.cam_dict:
            api.set_id_dict('campaign')
        found = api.get_id(api.cam_dict, self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


class AdGroupUpload(utl.BaseUploadConfig):
    """Reddit's mid-tier object (Adset in LQ parlance)."""
    config_dir = config_path
    config_label = 'Reddit adgroup config'
    file_name = 'adset_upload.xlsx'
    name = 'name'
    campaign = 'campaign'
    configured_status = 'configured_status'
    bid_strategy = 'bid_strategy'
    bid_type = 'bid_type'
    bid_value = 'bid_value'
    goal_type = 'goal_type'
    goal_value = 'goal_value'
    optimization_goal = 'optimization_goal'
    # Required for ALL ad groups starting 2026-07-13 (optional before).
    conversion_pixel_id = 'conversion_pixel_id'
    start_time = 'start_time'
    end_time = 'end_time'
    # Targeting columns (pipe/comma-delimited cells).
    communities = 'communities'
    geolocations = 'geolocations'
    interests = 'interests'
    devices = 'devices'
    platforms = 'platforms'
    gender = 'gender'
    keywords = 'keywords'
    excluded_communities = 'excluded_communities'
    excluded_keywords = 'excluded_keywords'
    snapshot_cols = [configured_status, bid_strategy, bid_type, bid_value,
                     goal_type, goal_value, optimization_goal,
                     conversion_pixel_id, start_time, end_time]

    @staticmethod
    def settings_from_live(fields):
        """The live ad group's bidding, goal and targeting as level-file
        cells, without name, status, amounts or flight; geolocations stay
        the geo ids ``create_targeting_dict`` passes through."""
        agu = AdGroupUpload
        targeting = fields.get('targeting') or {}
        values = {col: fields.get(col) for col in (
            agu.bid_strategy, agu.bid_type, agu.goal_type,
            agu.optimization_goal, agu.conversion_pixel_id)}
        values.update({col: utl.join_list(targeting.get(col)) for col in (
            agu.communities, agu.geolocations, agu.interests, agu.platforms,
            agu.keywords, agu.excluded_communities, agu.excluded_keywords)})
        values[agu.devices] = utl.join_list(
            (d or {}).get('type') for d in targeting.get('devices') or [])
        values[agu.gender] = targeting.get('gender')
        return utl.live_settings(values)

    def upload_all_adgroups(self, api):
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, ag_id in enumerate(self.config):
            row = self.config[ag_id]
            logging.info(
                f'Uploading Reddit adgroup {idx + 1} of {total}: '
                f'{row.get(self.name)}')
            try:
                result = self.upload_adgroup(api, AdGroup(row, api=api))
            except utl.UploaderTargetingError as e:
                logging.warning('%s was not uploaded: %s',
                                row.get(self.name), e)
                result = utl.fail_result(utl.new_result(
                    'Adset', row.get(self.name), CHANNEL), e)
            result['pushed_values'] = utl.snapshot_values(
                row, self.snapshot_cols)
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
            result['error_message'] = 'Missing required ad group fields'
            return result
        if adgroup.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = adgroup.id
            return result
        _populate_reddit_result(
            result, api.create_entity(adgroup, entity_name='ad_groups'))
        if result['status'] == 'created':
            adgroup.id = result['platform_id']
            api.adgroup_dict[adgroup.id] = dict(
                adgroup.upload_dict, id=adgroup.id)
        return result


OBJECTIVE_BID_TYPES = {'CLICKS': 'CPC', 'IMPRESSIONS': 'CPM',
                       'VIDEO_VIEWABLE_IMPRESSIONS': 'CPV'}


class AdGroup(object):
    __slots__ = ['name', 'campaign', 'campaignId', 'configured_status',
                 'bid_strategy', 'bid_type', 'bid_value', 'goal_type',
                 'goal_value', 'optimization_goal', 'conversion_pixel_id',
                 'start_time', 'end_time', 'communities', 'geolocations',
                 'interests', 'devices', 'platforms', 'gender',
                 'keywords', 'excluded_communities', 'excluded_keywords',
                 'objective', 'upload_dict', 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.campaign = None
        self.campaignId = None
        self.configured_status = 'PAUSED'
        self.bid_strategy = 'MAXIMIZE_VOLUME'
        self.bid_type = None
        self.bid_value = None
        self.goal_type = 'DAILY_SPEND'
        self.goal_value = None
        self.optimization_goal = None
        self.conversion_pixel_id = None
        self.start_time = None
        self.end_time = None
        self.communities = None
        self.geolocations = None
        self.interests = None
        self.devices = None
        self.platforms = None
        self.gender = None
        self.keywords = None
        self.excluded_communities = None
        self.excluded_keywords = None
        self.objective = None
        utl.apply_row(self, row_dict)
        self.api = api
        if self.api:
            self.resolve_campaign(self.api)
        self.upload_dict = self.create_adgroup_dict()

    def resolve_campaign(self, api):
        cam = Campaign({'name': self.campaign}, api=api)
        cam.check_exists(api)
        self.campaignId = cam.id
        self.objective = (api.cam_dict.get(cam.id) or {}).get('objective')

    def create_adgroup_dict(self):
        """The ad group body. Reddit refuses one without a bid type, so a
        blank one follows the campaign objective, and Maximize Volume
        without a bid amount, so that goes out ``BIDLESS``."""
        if not (self.name and self.campaignId):
            return {}
        bid = _to_micros(self.bid_value)
        strategy = str(self.bid_strategy or 'MAXIMIZE_VOLUME')
        if bid is None and strategy == 'MAXIMIZE_VOLUME':
            strategy = 'BIDLESS'
        d = {
            'name': str(self.name),
            'campaign_id': str(self.campaignId),
            'configured_status': str(self.configured_status or 'PAUSED'),
            'bid_strategy': strategy,
            'goal_type': str(self.goal_type or 'DAILY_SPEND'),
        }
        goal = _to_micros(self.goal_value)
        if goal is not None:
            d['goal_value'] = goal
        bid_type = self.bid_type or OBJECTIVE_BID_TYPES.get(
            str(self.objective or '').upper())
        if bid_type:
            d['bid_type'] = str(bid_type)
        if bid is not None:
            d['bid_value'] = bid
        if self.optimization_goal:
            d['optimization_goal'] = str(self.optimization_goal)
        if self.conversion_pixel_id:
            d['conversion_pixel_id'] = str(self.conversion_pixel_id)
        zone = _account_timezone(self.api)
        for col, value in ((AdGroupUpload.start_time, self.start_time),
                           (AdGroupUpload.end_time, self.end_time)):
            iso = _to_iso(value, col == AdGroupUpload.end_time, zone)
            if iso:
                d[col] = iso
        targeting = self.create_targeting_dict()
        if targeting:
            d['targeting'] = targeting
        return d

    def create_targeting_dict(self):
        """Assemble the v3 ad-group ``targeting`` object from the pipe-
        delimited targeting columns. Communities/interests pass through as
        the ids/names the config supplies; geolocations resolve country
        names -> Reddit geo ids live; devices/platforms/gender map to the
        v3 enums. Keywords and the two block lists (communities and
        keywords the ads must stay out of) pass through as typed. Empty
        when nothing is set (Reddit then runs broad)."""
        targeting = {}
        communities = [normalize_community(value)
                       for value in utl.split_list(self.communities)]
        if not all(communities):
            raise utl.UploaderTargetingError(
                'Verify every Reddit community before uploading.')
        refused = (self.api.untargetable_communities(communities)
                   if self.api and communities else [])
        if refused:
            one = len(refused) == 1
            raise utl.UploaderTargetingError(
                f"Reddit cannot target "
                f"{'this community' if one else 'these communities'}: "
                f"{', '.join(refused)}. Remove {'it' if one else 'them'} "
                'from Communities on the Adset tab, then upload again.')
        if communities:
            targeting['communities'] = communities
        blocked = [normalize_community(value) for value in
                   utl.split_list(self.excluded_communities)]
        if not all(blocked):
            raise utl.UploaderTargetingError(
                'Blocked Communities holds a value that is not a '
                'community name. Fix it on the Adset tab, then upload '
                'again.')
        for key, values in (
                ('excluded_communities', blocked),
                ('keywords', utl.split_list(self.keywords)),
                ('excluded_keywords',
                 utl.split_list(self.excluded_keywords))):
            if values:
                targeting[key] = values
        interests = utl.split_list(self.interests)
        if interests:
            targeting['interests'] = interests
        geos = []
        for value in utl.split_list(self.geolocations):
            gid = self.api.resolve_country_geo(value) if self.api else ''
            # Resolved country -> geo id; else assume it already is one.
            geos.append(gid or value)
        if geos:
            targeting['geolocations'] = geos
        devices = [{'type': d.upper()} for d in utl.split_list(self.devices)
                   if d.upper() in ('DESKTOP', 'MOBILE')]
        if devices:
            targeting['devices'] = devices
        platforms = []
        for p in utl.split_list(self.platforms):
            mapped = PLATFORM_MAP.get(p.upper())
            if mapped and mapped not in platforms:
                platforms.append(mapped)
        if platforms:
            targeting['platforms'] = platforms
        gender = str(self.gender or '').strip().upper()
        if gender in ('MALE', 'FEMALE'):
            targeting['gender'] = gender
        return targeting

    def check_exists(self, api):
        found = api.get_id(api.ensure_id_dict(
            'adgroup', filter_id=self.campaignId), self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


class AdUpload(utl.BaseUploadConfig):
    config_dir = config_path
    config_label = 'Reddit ad config'
    file_name = 'ad_upload.xlsx'
    name = 'name'
    campaign = 'campaign'
    adgroup = 'ad_group'
    creative = 'creative'
    configured_status = 'configured_status'
    profile = 'profile'
    headline = 'headline'
    call_to_action = 'call_to_action'
    destination_url = 'destination_url'
    thumbnail = 'thumbnail'
    post_type = 'post_type'
    snapshot_cols = [configured_status, headline, call_to_action,
                     destination_url, creative, thumbnail, post_type]

    @staticmethod
    def settings_from_live(fields):
        """The profile a live ad posts as, as a level-file cell; the
        headline and call to action live on the post."""
        return utl.live_settings(
            {AdUpload.profile: fields.get('profile_id')})

    @staticmethod
    def urls_from_live(fields):
        """The live ad's click URL, read for reference, never copied."""
        return utl.live_settings({'landing': fields.get('click_url')})

    def upload_all_ads(self, api):
        if not self.config:
            return []
        results = []
        total = len(self.config)
        for idx, a_id in enumerate(self.config):
            row = self.config[a_id]
            try:
                ad = Ad(row, api=api)
                logging.info(
                    f'Uploading Reddit ad {idx + 1} of {total}: {ad.name}')
                result = self.upload_ad(api, ad)
            except (OSError, utl.UploaderAuthError, ValueError,
                    requests.exceptions.RequestException) as exc:
                result = utl.new_result('Ad', row.get(self.name), CHANNEL)
                results.append(utl.fail_result(result, str(exc)))
                logging.warning(
                    'Reddit ad %r failed: %s', row.get(self.name), exc)
                continue
            result['pushed_values'] = {
                **utl.snapshot_values(self.config[a_id], self.snapshot_cols),
                'post_id': str(ad.postId or ''),
                'profile_id': str(ad.profileId or '')}
            results.append(result)
        return results

    @staticmethod
    def upload_ad(api, ad):
        result = utl.new_result('Ad', ad.name, CHANNEL, ad.adGroupId)
        if not ad.adGroupId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = f'Ad group {ad.ad_group!r} not found'
            return result
        if not ad.id and not ad.postId:
            result['status'] = 'skipped_dep_missing'
            result['error_message'] = (
                f'Post for creative {ad.creative!r} could not be resolved '
                f'or created')
            return result
        if ad.id or ad.check_exists(api):
            result['status'] = 'skipped_exists'
            result['platform_id'] = ad.id
            return result
        _populate_reddit_result(
            result, api.create_entity(ad, entity_name='ads'))
        if result['status'] == 'created':
            ad.id = result['platform_id']
            api.ad_dict[ad.id] = dict(ad.upload_dict, id=ad.id)
        return result


class Ad(object):
    __slots__ = ['name', 'campaign', 'ad_group', 'adGroupId',
                 'creative', 'configured_status', 'profile', 'profileId',
                 'headline', 'call_to_action', 'destination_url',
                 'thumbnail', 'post_type', 'postId', 'upload_dict',
                 'api', 'id']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.campaign = None
        self.ad_group = None
        self.adGroupId = None
        self.creative = None
        self.configured_status = 'PAUSED'
        self.profile = None
        self.profileId = None
        self.headline = None
        self.call_to_action = None
        self.destination_url = None
        self.thumbnail = None
        self.post_type = None
        self.postId = None
        utl.apply_row(self, row_dict)
        self.api = api
        if self.api:
            self.resolve_ids(self.api)
        self.upload_dict = self.create_ad_dict()

    def resolve_ids(self, api):
        if self.ad_group:
            ag = AdGroup({'name': self.ad_group,
                          'campaign': self.campaign}, api=api)
            ag.check_exists(api)
            self.adGroupId = ag.id
        if not self.adGroupId or self.check_exists(api):
            return
        self.profileId = api.resolve_profile_id(self.profile)
        if self.creative:
            post = Post({'name': self.creative,
                         'profile': self.profile,
                         'profileId': self.profileId,
                         'headline': self.headline,
                         'call_to_action': self.call_to_action,
                         'destination_url': self.destination_url,
                         'thumbnail': self.thumbnail,
                         'post_type': self.post_type}, api=api)
            self.postId = post.id

    def create_ad_dict(self):
        if not (self.name and self.adGroupId and self.postId):
            return {}
        d = {
            'name': str(self.name),
            'ad_group_id': str(self.adGroupId),
            'post_id': str(self.postId),
            'configured_status': str(self.configured_status or 'PAUSED'),
        }
        if self.destination_url:
            d['click_url'] = str(self.destination_url)
        if self.profileId:
            d['profile_id'] = str(self.profileId)
        return d

    def check_exists(self, api):
        found = api.get_id(api.ensure_id_dict(
            'ad', filter_id=self.adGroupId), self.name)
        if found:
            self.id = found[0]
            logging.warning(f'{self.name} already in account.')
            return True
        return False


POST_TYPE_WORDS = {'STATIC': 'IMAGE'}
MEDIA_POST_TYPES = ('IMAGE', 'VIDEO')


def post_type(value, derived=''):
    """Normalize copy-sheet types, preferring the asset's image/video type."""
    word = str(value or '').strip().upper()
    word = POST_TYPE_WORDS.get(word, word)
    if derived in MEDIA_POST_TYPES and word in MEDIA_POST_TYPES:
        return derived
    return word or derived or 'IMAGE'


class Post(object):
    """Resolve uploaded files, library assets or URLs into an ad's post.

    Reuse requires matching media and copy, since headlines are not unique.
    """
    __slots__ = ['name', 'profile', 'profileId', 'headline',
                 'call_to_action', 'destination_url', 'thumbnail',
                 'post_type', 'id', 'api']

    def __init__(self, row_dict, api=None):
        self.id = None
        self.name = None
        self.profile = None
        self.profileId = None
        self.headline = None
        self.call_to_action = None
        self.destination_url = None
        self.thumbnail = None
        self.post_type = None
        utl.apply_row(self, row_dict)
        self.api = api
        if self.api:
            self.set_id(self.api)

    def set_id(self, api):
        if not self.profileId:
            self.profileId = api.resolve_profile_id(self.profile)
        if not self.profileId:
            logging.warning(
                'Reddit creative %r has no resolvable profile; an ad needs '
                'a profile to author its post.', self.name)
            return
        self.create(api)

    def resolve_existing(self, api, payload):
        """Reuse only a post with the same media, headline, CTA and URL."""
        for pid, row in api.list_posts(self.profileId).items():
            if any(row.get(key) != payload.get(key) for key in
                   ('headline', 'type', 'thumbnail_url')):
                continue
            content = row.get('content') or []
            wanted = payload['content']
            if len(content) != len(wanted):
                continue
            if all(all(actual.get(k) == v for k, v in expected.items())
                   for actual, expected in zip(content, wanted)):
                self.id = pid
                return True
        return False

    def create(self, api):
        thumb_url = ''
        if self.thumbnail:
            thumb_url, thumb_type = api.resolve_asset_media(
                self.profileId, self.thumbnail)
            if not thumb_url or thumb_type == 'VIDEO':
                raise ValueError('Choose an image thumbnail for the video.')
        if thumb_url:
            media_url, derived_type = api.resolve_asset_media(
                self.profileId, self.name, poster_url=thumb_url)
        else:
            media_url, derived_type = api.resolve_asset_media(
                self.profileId, self.name)
        if not media_url:
            logging.warning(
                'Reddit creative %r did not resolve to a media url '
                '(asset-library asset name or direct url); post not '
                'created.', self.name)
            return
        ptype = post_type(self.post_type, derived_type)
        # Pre-check type rules so a miss logs a reason, not a raw 400.
        if ptype in ('IMAGE', 'CAROUSEL') and not self.destination_url:
            logging.warning(
                'Reddit %s post %r needs a destination_url; post not '
                'created.', ptype, self.name)
            return
        if ptype == 'VIDEO' and not thumb_url:
            thumb_url = api.resolved_asset_poster(self.profileId, self.name)
            if not thumb_url:
                raise ValueError(
                    'Choose a Cover Image, or upload the video on Creative '
                    'to make its cover automatically.')
        payload = self.create_post_dict(ptype, media_url, thumb_url)
        if self.resolve_existing(api, payload):
            return
        result = utl.new_result('Post', self.name, CHANNEL)
        _populate_reddit_result(result, api.create_post(
            self.profileId, payload))
        if result['status'] == 'created':
            self.id = result['platform_id']
        else:
            logging.warning('Reddit post %r create failed: %s',
                            self.name, result['error_message'])

    def create_post_dict(self, post_type, media_url, thumb_url=''):
        content = {'media_url': str(media_url),
                   'call_to_action': _to_cta(self.call_to_action)}
        if self.destination_url:
            content['destination_url'] = str(self.destination_url)
        d = {'type': str(post_type),
             'headline': str(self.headline or self.name or ''),
             'content': [content]}
        if thumb_url:
            d['thumbnail_url'] = str(thumb_url)
        return d


# Spreadsheet columns mapped to Reddit PATCH fields.
UPDATE_COLUMN_FIELDS = {
    'Campaign': {
        CampaignUpload.name: 'name',
        CampaignUpload.spend_cap: 'spend_cap',
    },
    'Adset': {
        AdGroupUpload.name: 'name',
        AdGroupUpload.goal_value: 'goal_value',
        AdGroupUpload.bid_value: 'bid_value',
        AdGroupUpload.start_time: 'start_time',
        AdGroupUpload.end_time: 'end_time',
    },
    'Ad': {AdUpload.name: 'name'},
}

LIVE_UPLOADS = {'Campaign': CampaignUpload, 'Adset': AdGroupUpload,
                'Ad': AdUpload}

MONEY_UPDATE_COLS = (CampaignUpload.spend_cap, AdGroupUpload.goal_value,
                     AdGroupUpload.bid_value)

DATE_UPDATE_COLS = (AdGroupUpload.start_time, AdGroupUpload.end_time)
