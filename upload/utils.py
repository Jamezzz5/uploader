import os
import pandas as pd

config_file_path = 'config/'
err_file_path = 'ERROR_REPORTS/'
static_types = ['jpg', 'png', 'jpeg']


def dir_check(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def dir_remove(directory):
    if os.path.isdir(directory):
        if not os.listdir(directory):
            os.rmdir(directory)


def write_df(df, file_name):
    writer = pd.ExcelWriter(file_name)
    df.to_excel(writer, index=False)
    writer.save()


def remove_file(file_name):
    try:
        os.remove(file_name)
    except OSError:
        pass
