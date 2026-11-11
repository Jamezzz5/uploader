"""The creator relations pass on a file that is not a relation table.

A relation file lacking the ``impacted_column_name`` column (or one
that reads back empty) used to kill the whole ``run_uploader`` task
with a KeyError before any target file was written; it must now report
the rows it could not apply, name what the file really holds, and
leave the target file alone.  The well-formed path still applies the
values and records its errors exactly as before.
"""
import logging

import pandas as pd
import pytest

import upload.creator as cre

REL_COLS = [cre.Creator.rel_col_name, cre.Creator.rel_col_pos,
            cre.Creator.rel_col_val, cre.Creator.rel_col_imp,
            cre.Creator.rel_col_imp_new_value]


@pytest.fixture
def target_file(tmp_path):
    """The campaign target file the relations pass writes to."""
    df = pd.DataFrame({'name': ['a', 'b'], 'budget': ['100', '200']})
    path = tmp_path / 'target.xlsx'
    df.to_excel(path, index=False)
    return path


def make_creator(rel_df, target_file, **kwargs):
    """A Creator on the relations pass over ``rel_df`` and the target."""
    return cre.Creator('campaign_name', 'False', str(target_file),
                       df=rel_df, **kwargs)


def test_relation_file_without_impacted_column_is_reported(
        target_file, caplog):
    """A relation table without impacted_column_name is reported, and
    the target file is left as it was."""
    rel = pd.DataFrame({'column_name': ['name', 'budget'],
                        'position': ['Constant', '0'],
                        'region': ['x', 'y']})
    creator = make_creator(rel, target_file)
    with caplog.at_level(logging.WARNING):
        error_dict = creator.apply_relations()
    assert error_dict == {'impacted_column_name': 2}
    assert any('impacted_column_name' in message and 'region' in message
               for message in caplog.messages)
    written = pd.read_excel(target_file, dtype=object,
                            keep_default_na=False, na_values=[''])
    assert written['name'].to_list() == ['a', 'b']
    assert written['budget'].to_list() == ['100', '200']


def test_empty_relation_file_is_reported_not_raised(target_file):
    """A relation file that reads back empty is reported as 0 rows."""
    creator = make_creator(pd.DataFrame(), target_file)
    error_dict = creator.apply_relations()
    assert error_dict == {'impacted_column_name': 0}
    written = pd.read_excel(target_file, dtype=object,
                            keep_default_na=False, na_values=[''])
    assert written['budget'].to_list() == ['100', '200']


def test_constant_relation_is_still_applied(target_file):
    """A Constant relation still stamps its value onto the target."""
    rel = pd.DataFrame({
        cre.Creator.rel_col_name: ['name'],
        cre.Creator.rel_col_pos: ['Constant'],
        cre.Creator.rel_col_val: ['x'],
        cre.Creator.rel_col_imp: ['budget'],
        cre.Creator.rel_col_imp_new_value: ['500']})
    creator = make_creator(rel, target_file)
    error_dict = creator.apply_relations()
    assert error_dict == {}
    written = pd.read_excel(target_file, dtype=object,
                            keep_default_na=False, na_values=[''])
    assert written['budget'].to_list() == ['500', '500']


def test_position_relation_records_undefined_values(
        tmp_path, monkeypatch, caplog):
    """A position relation applies what matches and reports the rest."""
    monkeypatch.chdir(tmp_path)
    target = tmp_path / 'target.xlsx'
    pd.DataFrame({'name': ['game_edition', 'other_edition'],
                  'budget': ['100', '200']}).to_excel(
                      target, index=False)
    rel = pd.DataFrame({
        cre.Creator.rel_col_name: ['name'],
        cre.Creator.rel_col_pos: ['0'],
        cre.Creator.rel_col_val: ['game'],
        cre.Creator.rel_col_imp: ['budget'],
        cre.Creator.rel_col_imp_new_value: ['700']})
    creator = make_creator(rel, target)
    with caplog.at_level(logging.WARNING, logger='upload.creator'):
        error_dict = creator.apply_relations()
    assert error_dict == {'budget': 1}
    written = pd.read_excel(target, dtype=object, keep_default_na=False,
                            na_values=[''])
    assert written['budget'].to_list()[0] == '700'
    assert pd.isna(written['budget'].to_list()[1])
    report = tmp_path / 'ERROR_REPORTS' / 'tmp' / 'pytest-of-lqapp' / \
        'pytest-0' / 'test_position_relation_records0' / \
        'target_budget.xlsx'
    assert report.exists()
    reported = pd.read_excel(report, dtype=object,
                             keep_default_na=False, na_values=[''])
    assert reported['budget'].to_list() == ['other']
