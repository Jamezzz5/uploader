import logging

import openpyxl
import pandas as pd
import pytest

import upload.creator as cre
import upload.utils as utl

REL_COLS = [cre.Creator.rel_col_name, cre.Creator.rel_col_pos,
            cre.Creator.rel_col_val, cre.Creator.rel_col_imp,
            cre.Creator.rel_col_imp_new_value]


def _blank_workbook(path):
    """A workbook a person saved with every cell cleared -- no header."""
    workbook = openpyxl.Workbook()
    workbook.save(path)


def _make_job(tmp_path, relation_file, relation_df):
    """A relation job in the current directory (a test chdirs into
    tmp_path first): the target already holds the rows relations would
    modify, and the relation file sits where get_df expects it."""
    target = 'config/target.xlsx'
    utl.write_df(pd.DataFrame({'name': ['Ad One', 'Ad Two']}), target)
    if isinstance(relation_df, str):
        utl.dir_check('config')
        _blank_workbook('config/' + relation_file)
    else:
        utl.write_df(relation_df, 'config/' + relation_file)
    job = cre.Job(file_name=relation_file, new_file='target.xlsx',
                  create_type=cre.Job.relation, overwrite=False,
                  file_filter='nan')
    return job, target


def test_apply_relations_blank_relation_file(tmp_path, monkeypatch,
                                              caplog):
    """The ticket's crash: an empty workbook reads with no columns, so
    the job must warn, leave the target rows untouched and finish."""
    monkeypatch.chdir(tmp_path)
    job, target = _make_job(tmp_path, 'rel.xlsx', 'blank')
    with caplog.at_level(logging.WARNING):
        error_dict = job.do_job()
    assert error_dict == {}
    out = utl.read_excel(target)
    assert out['name'].to_list() == ['Ad One', 'Ad Two']
    assert any(r.levelno == logging.WARNING
               and cre.Creator.rel_col_imp in r.getMessage()
               for r in caplog.records)


def test_apply_relations_rows_without_relation_columns(tmp_path,
                                                       monkeypatch):
    """Rows present but the expected column missing is a bad file, not
    a blank one: fail loudly and drop nothing silently."""
    monkeypatch.chdir(tmp_path)
    job, target = _make_job(
        tmp_path, 'rel.xlsx',
        pd.DataFrame({'column_name': ['ad_name'],
                      'position': ['Constant']},
                     columns=['column_name', 'position']))
    with pytest.raises(ValueError, match=cre.Creator.rel_col_imp):
        job.do_job()


def test_apply_relations_empty_frame_with_columns(tmp_path, monkeypatch):
    """A header-only relation file keeps the existing no-op behavior."""
    monkeypatch.chdir(tmp_path)
    job, target = _make_job(tmp_path, 'rel.xlsx',
                            pd.DataFrame(columns=REL_COLS))
    error_dict = job.do_job()
    assert error_dict == {}
    out = utl.read_excel(target)
    assert out['name'].to_list() == ['Ad One', 'Ad Two']


def test_apply_relations_still_applies_relations(tmp_path, monkeypatch):
    """The guard must not swallow a well-formed relation file."""
    monkeypatch.chdir(tmp_path)
    job, target = _make_job(
        tmp_path, 'rel.xlsx',
        pd.DataFrame({cre.Creator.rel_col_name: ['ad_name'],
                      cre.Creator.rel_col_pos: ['Constant'],
                      cre.Creator.rel_col_val: ['x'],
                      cre.Creator.rel_col_imp: ['title'],
                      cre.Creator.rel_col_imp_new_value: ['My Title']},
                     columns=REL_COLS))
    error_dict = job.do_job()
    assert error_dict == {}
    out = utl.read_excel(target)
    assert out['title'].to_list() == ['My Title', 'My Title']
