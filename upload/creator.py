import os
import logging
import itertools
import pandas as pd
import upload.utils as utl

file_path = utl.config_file_path
log = logging.getLogger()


class CreatorConfig(object):
    col_file_name = 'file_name'
    col_new_file = 'new_file'
    col_create_type = 'create_type'
    col_column_name = 'column_name'
    col_overwrite = 'overwrite'
    col_filter = 'file_filter'

    def __init__(self, file_name=None):
        self.file_name = file_name
        self.full_file_name = os.path.join(file_path, self.file_name)
        self.cur_file_name = None
        self.cur_new_file = None
        self.cur_create_type = None
        self.cur_column_name = None
        self.cur_overwrite = None
        self.job_dict = None
        self.error_dict = {}
        if self.file_name:
            self.config = self.read_config(self.full_file_name)

    @staticmethod
    def read_config(file_name):
        logging.info('Loading config file: {}'.format(file_name))
        df = pd.read_excel(file_name)
        df_dict = df.to_dict(orient='index')
        return df_dict

    def do_all(self):
        for key in self.config:
            job = self.set_job(key)
            error_dict = self.do_job(key)
            self.error_dict[job.new_file] = error_dict
        utl.dir_remove(utl.err_file_path)
        return self.error_dict

    def set_job(self, key):
        job = Job(self.config[key])
        return job

    def do_job(self, key):
        job = self.set_job(key)
        logging.info('Doing job from {} on {} of type {}.'
                     ''.format(job.file_name, job.new_file, job.create_type))
        error_dict = job.do_job()
        return error_dict


class Job(object):
    create = 'create'
    duplicate = 'duplicate'
    relation = 'relation'
    mediaplan = 'mediaplan'

    def __init__(self, job_dict=None, file_name=None, new_file=None,
                 create_type=None, column_name=None, overwrite=None,
                 file_filter=None):
        self.file_name = file_name
        self.new_file = new_file
        self.create_type = create_type
        self.column_name = column_name
        self.overwrite = overwrite
        self.file_filter = file_filter
        self.df = None
        if job_dict:
            for k in job_dict:
                setattr(self, k, job_dict[k])

    def get_df(self):
        if self.create_type == self.mediaplan:
            mp = MediaPlan(self.file_name, first_row=0)
            df = mp.df
        else:
            df = pd.read_excel(file_path + self.file_name, dtype=object,
                               keep_default_na=False, na_values=[''])
        if str(self.file_filter) != 'nan':
            df = self.filter_df(df)
        return df

    def filter_df(self, df):
        self.file_filter = self.file_filter.split('::')
        filter_col = self.file_filter[0]
        filter_vals = self.file_filter[1].split('|')
        df = df[df[filter_col].isin(filter_vals)].copy()
        return df

    def do_job(self):
        df = self.get_df()
        cr = Creator(self.column_name, self.overwrite,
                     self.new_file, file_path, df=df)
        error_dict = {}
        if self.create_type == self.create:
            cr.create_upload_file()
        elif self.create_type == self.duplicate:
            cr.apply_duplication()
        elif self.create_type == self.relation:
            error_dict = cr.apply_relations()
        elif self.create_type == self.mediaplan:
            cr.get_plan_names()
        return error_dict


class Creator(object):
    def __init__(self, col_name, overwrite, new_file,
                 cc_file_path='config/create/', df=None, config_file=None):
        self.df = df
        self.col_name = col_name
        self.overwrite = overwrite
        self.new_file = new_file
        self.config_file = config_file
        self.error_dict = {}
        if cc_file_path and self.new_file:
            self.new_file = os.path.join(file_path, self.new_file)
        if cc_file_path and self.config_file:
            self.config_file = os.path.join(file_path, self.config_file)
        if self.config_file:
            self.df = pd.read_excel(file_path + self.config_file)

    def get_combined_list(self):
        z = list(itertools.product(*[self.df[x].dropna().values
                                     for x in self.df.columns]))
        combined_list = ['_'.join(map(str, x)) for x in z]
        return combined_list

    def create_df(self, new_values):
        df = pd.read_excel(self.new_file)
        ndf = pd.DataFrame(data={self.col_name: pd.Series(new_values)},
                           columns=df.columns)
        if not self.overwrite:
            df = df.append(ndf, ignore_index=True,
                           sort=False).reset_index(drop=True)
        else:
            df = ndf
        return df

    def create_upload_file(self):
        combined_list = self.get_combined_list()
        df = self.create_df(combined_list)
        utl.write_df(df, self.new_file)

    def apply_relations(self):
        cdf = pd.read_excel(self.new_file)
        for imp_col in self.df['impacted_column_name'].unique():
            df = self.df[self.df['impacted_column_name'] == imp_col]
            par_col = str(df['column_name'].values[0]).split('|')
            position = str(df['position'].values[0]).split('|')
            if position == ['Constant']:
                cdf[imp_col] = df['impacted_column_new_value'].values[0]
            else:
                rel_dict = self.create_relation_dictionary(df)
                cdf = self.set_values_to_imp_col(cdf, position, par_col,
                                                 imp_col)
                cdf = self.check_undefined_relation(cdf, rel_dict, imp_col)
                cdf[imp_col] = cdf[imp_col].replace(rel_dict)
        utl.write_df(cdf, self.new_file)
        return self.error_dict

    @staticmethod
    def create_relation_dictionary(df):
        df = df[['column_value', 'impacted_column_new_value']]
        rel_dict = pd.Series(df['impacted_column_new_value'].values,
                             index=df['column_value']).to_dict()
        return rel_dict

    @staticmethod
    def set_values_to_imp_col(df, position, par_col, imp_col):
        if position == ['nan']:
            df[imp_col] = df[par_col[0]]
        else:
            if len(position) != len(par_col):
                logging.warning('Length mismatch between {} and {}'
                                ''.format(par_col, position))
                par_col = par_col + [
                    par_col[0] for x in range(len(position) - len(par_col))]
            for idx, pos in enumerate(position):
                if str(pos) == '':
                    new_series = df[par_col[int(idx)]]
                else:
                    new_series = (df[par_col[int(idx)]].str.split('_')
                                  .str[int(pos)])
                if idx == 0:
                    df[imp_col] = new_series
                else:
                    df[imp_col] = df[imp_col] + '|' + new_series
        return df

    def check_undefined_relation(self, df, rel_dict, imp_col):
        undefined = df.loc[~df[imp_col].isin(rel_dict), imp_col]
        imp_file = self.new_file.split('.')[0].replace(file_path, '')
        file_name = utl.err_file_path + imp_file + '_' + imp_col + '.xlsx'
        if not undefined.empty:
            logging.warning('No match found for the following values, '
                            'they were left blank.  An error report was '
                            'generated ' + str(undefined.head().values))
            df.loc[~df[imp_col].isin(rel_dict), imp_col] = ''
            self.error_dict[imp_col] = len(undefined.unique())
            err_file_path = os.path.join(
                *[x for x in file_name.split('/') if '.' not in x])
            utl.dir_check(err_file_path)
            utl.write_df(undefined.drop_duplicates(), file_name)
        else:
            self.error_dict[imp_col] = 0
            utl.remove_file(file_name)
        return df

    def apply_upload_filter(self, df):
        primary_col = self.col_name.split('::')[0]
        file_name = self.col_name.split('::')[2]
        file_name = file_path + file_name
        filter_df = pd.read_excel(file_name)
        filter_cols = [x.split('::') for x in filter_df.columns
                       if x not in primary_col]
        filter_dicts = filter_df.to_dict('records')
        ndf = pd.DataFrame()
        for filter_dict in filter_dicts:
            filtered_df = df.copy()
            for col in filter_cols:
                filter_col = '::'.join(col)
                col_name = col[0]
                col_position = int(col[1])
                filtered_df[filter_col] = filtered_df[
                    col_name].str.split('_').str[col_position]
                primary_val = filter_dict[primary_col]
                filter_vals = filter_dict[filter_col].split('|')
                filtered_df = filtered_df[
                    (filtered_df[primary_col] == primary_val) &
                    (filtered_df[filter_col].isin(filter_vals))]
            ndf = ndf.append(filtered_df, ignore_index=True, sort=False)
            ndf = ndf.reset_index(drop=True)
        for col in filter_cols:
            ndf = ndf.drop('::'.join(col), axis=1)
        return ndf

    def apply_duplication(self):
        cdf = pd.read_excel(self.new_file)
        original_cols = cdf.columns
        duplicated_col = self.col_name.split('::')[0]
        unique_list = cdf[duplicated_col].unique()
        cdf = pd.DataFrame(columns=original_cols)
        self.df = self.df[self.col_name.split('::')[1].split('|')][:]
        for item in unique_list:
            self.df[duplicated_col] = item
            cdf = cdf.append(self.df, ignore_index=True, sort=False)
        cdf = cdf.reset_index(drop=True)
        cdf = cdf[original_cols]
        if len(self.col_name.split('::')) > 2:
            cdf = self.apply_upload_filter(cdf)
        utl.write_df(cdf, self.new_file)

    def get_plan_names(self):
        self.col_name = self.col_name.split('|')
        df_dict = {}
        for col in self.col_name:
            if col in self.df.columns:
                df_dict[col] = pd.Series(self.df[col].unique())
            else:
                logging.warning('{} not in df.  Continuing.'.format(col))
        ndf = pd.DataFrame(df_dict)
        utl.write_df(ndf, './' + self.new_file)


class MediaPlan(object):
    campaign_id = 'Campaign ID'
    campaign_name = 'Campaign Name'
    partner_name = 'Partner Name'
    ad_type_name = 'Ad Type'
    ad_serving_name = 'Ad Serving Type'
    old_placement_phase = 'Placement Phase\n(If Needed) '
    old_campaign_phase = 'Campaign Phase\n(If Needed) '
    placement_phase = 'Placement Phase (If Needed) '
    campaign_phase = 'Campaign Phase (If Needed) '
    country_name = 'Country'
    placement_name = 'Placement Name'

    def __init__(self, file_name, sheet_name='Media Plan', first_row=2):
        self.file_name = file_name
        self.sheet_name = sheet_name
        self.first_row = first_row
        self.campaign_omit_list = ['_____']
        if self.file_name:
            self.df = self.load_df()

    def load_df(self):
        df = pd.read_excel(
            self.file_name,
            sheet_name=self.sheet_name,
            header=self.first_row,
            keep_default_na=False,
            na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN',
                       '-NaN', 'null', '-nan', '1.#IND', '1.#QNAN', 'N/A',
                       'NULL', 'NaN', 'n/a', 'nan'])
        df = df.rename(columns={
            self.old_placement_phase: self.placement_phase,
            self.old_campaign_phase: self.campaign_phase})
        for val in self.campaign_omit_list:
            df[self.campaign_name] = df[self.campaign_name].replace(val, '')
        # df = self.apply_match_dict(df)
        return df

    def apply_match_dict(self, df, file_name='mediaplan/mp_dcm_match.xlsx'):
        for col in [self.partner_name, self.ad_type_name, self.ad_serving_name]:
            match_dict = pd.read_excel(file_name, sheet_name=col)
            match_dict = match_dict.set_index('MP').to_dict()['DBM']
            df[col] = df[col].replace(match_dict)
        return df

    def set_campaign_name(self):
        cnames = self.df[self.campaign_name].unique()
        cnames = [x for x in cnames if x and x not in self.campaign_omit_list]
        self.df[self.campaign_name] = cnames[0]
