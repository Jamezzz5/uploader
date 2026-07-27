import os
import re
import time
import logging
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import datetime as dt

config_file_path = 'config/'
err_file_path = 'ERROR_REPORTS/'
static_types = ['jpg', 'png', 'jpeg']
video_types = ['mp4', 'mpg', 'm4v', 'mkv', 'webm', 'mov', 'avi', 'wmv', 'flv']


def dir_check(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def _json_safe(val):
    """Coerce a single config value to a JSON-serializable scalar."""
    try:
        if val is None or pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(val, 'item') and not isinstance(val, str):
        try:
            val = val.item()
        except (AttributeError, ValueError):
            pass
    if isinstance(val, (str, bool, int, float)):
        return val
    return str(val)


def apply_row(instance, row):
    """Copy excel-row keys onto a slotted upload object, logging
    keys that aren't declared in ``__slots__``."""
    for k, v in row.items():
        try:
            setattr(instance, k, v)
        except AttributeError as e:
            logging.warning(f'AttributeError: {e}')


def split_list(value):
    """Pipe/comma-delimited cell -> de-duped list of non-empty trimmed
    strings. Targeting columns arrive as ``'gaming|technology'`` cells.

    :param value: raw spreadsheet cell
    :returns: list of trimmed, de-duped strings ([] when blank/NaN)
    """
    if value is None:
        return []
    text = str(value).strip()
    if not text or text.lower() == 'nan':
        return []
    out = []
    for part in re.split(r'[|,]', text):
        part = part.strip()
        if part and part not in out:
            out.append(part)
    return out


def snapshot_values(row, columns):
    """JSON-safe dict of ``columns`` pulled from an upload config row.

    Attached to each result row as ``pushed_values`` so the app can
    persist what was actually sent per object and later diff config
    edits against it. Values are kept in row (spreadsheet) space —
    pre platform transforms like cent/micro scaling — so they compare
    directly against regenerated upload files.
    """
    snap = {}
    for col in columns:
        if col not in row:
            continue
        val = row[col]
        if isinstance(val, (list, tuple)):
            snap[col] = [_json_safe(v) for v in val]
        else:
            snap[col] = _json_safe(val)
    return snap


def new_result(object_level, source_name, uploader_type, parent_id=None):
    """The per-object result row every channel's upload loop returns.

    :param object_level: Campaign / Adset / Ad / Post
    :param source_name: name in the upload config
    :param uploader_type: channel name, e.g. 'Reddit'
    :param parent_id: platform id of the level above, when resolved
    :returns: result dict the app persists as an UploaderUploadedItem
    """
    return {
        'source_name': source_name,
        'object_level': object_level,
        'uploader_type': uploader_type,
        'platform_id': None,
        'parent_platform_id': str(parent_id) if parent_id else None,
        'status': None,
        'error_code': None,
        'error_message': None,
    }


class UploaderAuthError(Exception):
    """Channel credential/refresh failure — fatal, message secret-free."""


class BaseUploadConfig(object):
    """Excel-backed upload config shared by every channel's level.

    Subclasses set ``config_dir`` (the channel's config folder),
    ``file_name`` (the default workbook), ``name`` (the column a row is
    worthless without) and ``config_label`` (what a missing file is
    called in the log), then own only their own ``upload_all_*`` loop.
    """
    config_dir = config_file_path
    file_name = ''
    name = 'name'
    config_label = 'config'

    def __init__(self, config_file=None):
        self.config_file = config_file
        self.config = None
        if self.config_file:
            self.load_config(self.config_file)

    def load_config(self, config_file=''):
        """Read the workbook into ``self.config`` as {row index: row}.

        :param config_file: workbook name, defaulting to ``file_name``
        :returns: True when loaded, False when the file is absent
        """
        file_name = os.path.join(
            self.config_dir, config_file or self.file_name)
        if not os.path.exists(file_name):
            logging.warning(
                f'{self.config_label} missing: {file_name}')
            return False
        df = pd.read_excel(file_name)
        df = df.dropna(subset=[self.name]).fillna('')
        self.config = df.to_dict(orient='index')
        return True


class BaseCreativeStore(object):
    """Shared filename->platform-id bookkeeping for creative upload.

    Each ad channel keeps a small CSV mapping a local creative
    filename to the id(s) the platform returned, so re-runs skip files
    already uploaded. Subclasses set ``id_cols`` and implement
    ``_upload_one(api, file_path)`` returning ``{id_col: value}``; this
    base owns the find-new / upload-loop / persist / resolve cycle so
    each channel writes one method, not a bespoke pipeline.

    The shape was extracted from ``fbapi.Creative`` (the production
    reference), which keeps its own ``{path: hash}`` CSV format and is
    intentionally left untouched so existing ``creative_hashes.csv``
    files stay valid.
    """
    fn_col = 'filename'
    id_cols = ('id',)

    def __init__(self, id_file_name, creative_path='creative/'):
        self.creative_path = creative_path
        self.id_file_name = os.path.join(creative_path, id_file_name)
        self.records = {}
        self.load_config()

    def load_config(self):
        cols = [self.fn_col, *self.id_cols]
        if not os.path.isfile(self.id_file_name):
            dir_check(os.path.dirname(
                os.path.abspath(self.id_file_name)))
            pd.DataFrame(columns=cols).to_csv(
                self.id_file_name, index=False)
        df = pd.read_csv(self.id_file_name)
        self.records = {}
        for _, row in df.iterrows():
            fn = row.get(self.fn_col)
            if pd.isna(fn):
                continue
            self.records[str(fn)] = {
                c: (None if pd.isna(row.get(c)) else row.get(c))
                for c in self.id_cols}
        return self.records

    def get_new(self, filenames):
        """Bare filenames not already in the store (and not NaN)."""
        return [fn for fn in filenames
                if fn and str(fn) != 'nan' and fn not in self.records]

    def upload_all(self, api, filenames):
        new = self.get_new(filenames)
        total = len(new)
        for idx, fn in enumerate(new):
            logging.info('Uploading creative {} of {}.  Creative '
                         'Name: {}'.format(idx + 1, total, fn))
            path = os.path.join(self.creative_path, fn)
            if not os.path.isfile(path):
                logging.warning('{} not found.  It was not '
                                'uploaded'.format(path))
                continue
            ids = self._upload_one(api, path) or {}
            if not any(ids.get(c) for c in self.id_cols):
                logging.warning('{} did not return a creative id.  It '
                                'will be retried on the next run.'.format(fn))
                continue
            self.records[fn] = {c: ids.get(c) for c in self.id_cols}
        self.write()
        return self

    def _upload_one(self, api, file_path):
        """Push one local file to the platform; return ``{id_col:
        value}``. Implemented per channel."""
        raise NotImplementedError

    def get_id(self, filename, id_col=None):
        rec = self.records.get(filename)
        if not rec:
            return None
        return rec.get(id_col or self.id_cols[0])

    def write(self):
        rows = []
        for fn, rec in self.records.items():
            row = {self.fn_col: fn}
            for col in self.id_cols:
                row[col] = rec.get(col)
            rows.append(row)
        df = pd.DataFrame(rows, columns=[self.fn_col, *self.id_cols])
        try:
            df.to_csv(self.id_file_name, index=False)
        except IOError:
            logging.warning('{} could not be opened. Creative ids '
                            'not saved.'.format(self.id_file_name))


def dir_remove(directory):
    if os.path.isdir(directory):
        if not os.listdir(directory):
            os.rmdir(directory)


def write_df(df, file_name, sheet_name='Sheet1'):
    dir_name = os.path.dirname(os.path.abspath(file_name))
    dir_check(dir_name)
    writer = pd.ExcelWriter(file_name)
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    writer.close()


def remove_file(file_name):
    try:
        os.remove(file_name)
    except OSError:
        pass


def exceldate_to_datetime(excel_date):
    epoch = dt.datetime(1899, 12, 30)
    delta = dt.timedelta(hours=round(excel_date * 24))
    return epoch + delta


def string_to_date(my_string):
    month_list = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec']
    if ('/' in my_string and my_string[-4:][:2] != '20' and
            ':' not in my_string and len(my_string) in [6, 7, 8]):
        try:
            return dt.datetime.strptime(my_string, '%m/%d/%y')
        except ValueError:
            logging.warning('Could not parse date: {}'.format(my_string))
            return pd.NaT
    elif ('/' in my_string and my_string[-4:][:2] == '20' and
          ':' not in my_string):
        return dt.datetime.strptime(my_string, '%m/%d/%Y')
    elif (((len(my_string) == 5) and (my_string[0] == '4')) or
          ((len(my_string) == 7) and ('.' in my_string))):
        return exceldate_to_datetime(float(my_string))
    elif len(my_string) == 8 and my_string.isdigit() and my_string[0] == '2':
        try:
            return dt.datetime.strptime(my_string, '%Y%m%d')
        except ValueError:
            logging.warning('Could not parse date: {}'.format(my_string))
            return pd.NaT
    elif len(my_string) == 8 and '.' in my_string:
        return dt.datetime.strptime(my_string, '%m.%d.%y')
    elif my_string == '0' or my_string == '0.0':
        return pd.NaT
    elif ((len(my_string) == 22) and (':' in my_string) and
          ('+' in my_string)):
        my_string = my_string[:-6]
        return dt.datetime.strptime(my_string, '%Y-%m-%d %M:%S')
    elif ((':' in my_string) and ('/' in my_string) and my_string[1] == '/' and
          my_string[4] == '/'):
        my_string = my_string[:9]
        return dt.datetime.strptime(my_string, '%m/%d/%Y')
    elif (('PST' in my_string) and (len(my_string) == 28) and
          (':' in my_string)):
        my_string = my_string.replace('PST ', '')
        return dt.datetime.strptime(my_string, '%a %b %d %M:%S:%H %Y')
    elif (('-' in my_string) and (my_string[:2] == '20') and
          len(my_string) == 10):
        try:
            return dt.datetime.strptime(my_string, '%Y-%m-%d')
        except ValueError:
            try:
                return dt.datetime.strptime(my_string, '%Y-%d-%m')
            except ValueError:
                logging.warning('Could not parse date: {}'.format(my_string))
                return pd.NaT
    elif ((len(my_string) == 19) and (my_string[:2] == '20') and
          ('-' in my_string) and (':' in my_string)):
        try:
            return dt.datetime.strptime(my_string, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            logging.warning('Could not parse date: {}'.format(my_string))
            return pd.NaT
    elif ((len(my_string) == 7 or len(my_string) == 8) and
          my_string[-4:-2] == '20'):
        return dt.datetime.strptime(my_string, '%m%d%Y')
    elif ((len(my_string) == 6 or len(my_string) == 5) and
          my_string[-3:] in month_list):
        my_string = my_string + '-' + dt.datetime.today().strftime('%Y')
        return dt.datetime.strptime(my_string, '%d-%b-%Y')
    elif len(my_string) == 24 and my_string[-3:] == 'GMT':
        my_string = my_string[4:-11]
        return dt.datetime.strptime(my_string, '%d%b%Y')
    else:
        return my_string


def data_to_type(df, float_col=None, date_col=None, str_col=None, int_col=None,
                 fill_empty=True):
    if float_col is None:
        float_col = []
    if date_col is None:
        date_col = []
    if str_col is None:
        str_col = []
    if int_col is None:
        int_col = []
    for col in float_col:
        if col not in df:
            continue
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: x.replace('$', ''))
        df[col] = df[col].apply(lambda x: x.replace(',', ''))
        df[col] = df[col].replace(['nan', 'NA'], 0)
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].astype(float)
    for col in date_col:
        if col not in df:
            continue
        df[col] = df[col].replace(['1/0/1900', '1/1/1970'], '0')
        if fill_empty:
            df[col] = df[col].fillna(dt.datetime.today())
        else:
            df[col] = df[col].fillna(pd.Timestamp('nat'))
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: string_to_date(x))
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.normalize()
    for col in str_col:
        if col not in df:
            continue
        df[col] = df[col].astype('U')
        df[col] = df[col].str.strip()
        df[col] = df[col].apply(lambda x: ' '.join(x.split()))
    for col in int_col:
        if col not in df:
            continue
        df[col] = df[col].astype(int)
    return df


def read_excel(file_name, kwargs=None):
    """
    Read excel with a wrapper on zipfile to prevent error if file is saving

    :param file_name:
    :return:
    """
    if not kwargs:
        kwargs = {}
    df = pd.DataFrame()
    for _ in range(5):
        try:
            df = pd.read_excel(file_name, **kwargs)
            break
        except (zipfile.BadZipFile, ValueError, EOFError,
                ET.ParseError) as e:
            logging.warning(e)
            time.sleep(1)
        except FileNotFoundError as e:
            logging.warning(e)
            break
    return df
