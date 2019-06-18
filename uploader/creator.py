import logging
import itertools
import pandas as pd
import uploader.utils as utl

file_path = utl.config_file_path
log = logging.getLogger()


class CreatorConfig(object):
    def __init__(self, file_name=None):
        self.file_name = file_name
        self.full_file_name = file_path + self.file_name
        self.col_file_name = 'file_name'
        self.col_new_file = 'new_file'
        self.col_create_type = 'create_type'
        self.col_column_name = 'column_name'
        self.col_overwrite = 'overwrite'
        self.cur_file_name = None
        self.cur_new_file = None
        self.cur_create_type = None
        self.cur_column_name = None
        self.cur_overwrite = None
        self.job_dict = None
        if self.file_name:
            self.config = self.read_config(self.full_file_name)

    @staticmethod
    def read_config(file_name):
        logging.info('Loading config file: ' + str(file_name))
        df = pd.read_excel(file_name)
        df_dict = df.to_dict(orient='index')
        return df_dict

    def do_all(self):
        for key in self.config:
            self.set_job(key)
            self.do_job()
        utl.dir_remove(utl.err_file_path)

    def set_job(self, key):
        self.cur_file_name = self.config[key][self.col_file_name]
        self.cur_new_file = self.config[key][self.col_new_file]
        self.cur_create_type = self.config[key][self.col_create_type]
        self.cur_column_name = self.config[key][self.col_column_name]
        self.cur_overwrite = self.config[key][self.col_overwrite]

    def do_job(self):
        logging.info('Doing job from ' + str(self.cur_file_name) + ' on ' +
                     str(self.cur_new_file) + ' of type ' +
                     str(self.cur_create_type))
        df = pd.read_excel(file_path + self.cur_file_name, dtype=object,
                           keep_default_na=False, na_values=[''])
        cr = Creator(self.cur_column_name, self.cur_overwrite,
                     self.cur_new_file, file_path, df=df)
        if self.cur_create_type == 'create':
            cr.create_upload_file()
        elif self.cur_create_type == 'duplicate':
            cr.apply_duplication()
        elif self.cur_create_type == 'relation':
            cr.apply_relations()


class Creator(object):
    def __init__(self, col_name, overwrite, new_file,
                 cc_file_path='config/create/', df=None, config_file=None):
        self.df = df
        self.col_name = col_name
        self.overwrite = overwrite
        self.new_file = new_file
        self.config_file = config_file
        if cc_file_path and self.new_file:
            self.new_file = file_path + self.new_file
        if cc_file_path and self.config_file:
            self.config_file = file_path + self.config_file
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
            df = df.append(ndf).reset_index(drop=True)
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
            rel_dict = self.create_relation_dictionary(df)
            cdf = self.set_values_to_imp_col(cdf, position, par_col, imp_col)
            cdf = self.check_undefined_relation(cdf, rel_dict, imp_col)
            cdf[imp_col] = cdf[imp_col].replace(rel_dict)
        utl.write_df(cdf, self.new_file)

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
            for idx, pos in enumerate(position):
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
            utl.dir_check(utl.err_file_path)
            utl.write_df(undefined.drop_duplicates(), file_name)
        else:
            utl.remove_file(file_name)
        return df

    def apply_duplication(self):
        cdf = pd.read_excel(self.new_file)
        original_cols = cdf.columns
        duplicated_col = self.col_name.split('::')[0]
        unique_list = cdf[duplicated_col].unique()
        cdf = pd.DataFrame(columns=original_cols)
        self.df = self.df[self.col_name.split('::')[1].split('|')][:]
        for item in unique_list:
            self.df[duplicated_col] = item
            cdf = cdf.append(self.df)
        cdf = cdf.reset_index(drop=True)
        cdf = cdf[original_cols]
        utl.write_df(cdf, self.new_file)


class MediaPlan(object):
    campaign_name = 'Campaign Name'
    partner_name = 'Partner Name'
    ad_type_name = 'Ad Type'
    ad_serving_name = 'Ad Serving Type'
    placement_phase = 'Placement Phase\n(If Needed) '

    def __init__(self, file_name, sheet_name='Media Plan', first_row=2):
        self.file_name = file_name
        self.sheet_name = sheet_name
        self.first_row = first_row
        self.campaign_omit_list = ['_____']
        if self.file_name:
            self.df = self.load_df()

    def load_df(self):
        df = pd.read_excel(self.file_name,
                           sheet_name=self.sheet_name,
                           header=self.first_row)
        df = self.apply_match_dict(df)
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
