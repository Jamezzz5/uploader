import os
import pandas as pd

config_file_path = 'config/'
err_file_path = 'ERROR_REPORTS/'
static_types = ['jpg', 'png', 'jpeg']
video_types = ['mp4', 'mpg', 'm4v', 'mkv', 'webm', 'mov', 'avi', 'wmv', 'flv']


def dir_check(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def dir_remove(directory):
    if os.path.isdir(directory):
        if not os.listdir(directory):
            os.rmdir(directory)


def write_df(df, file_name, sheet_name='Sheet1'):
    writer = pd.ExcelWriter(file_name)
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    writer.save()


def remove_file(file_name):
    try:
        os.remove(file_name)
    except OSError:
        pass
