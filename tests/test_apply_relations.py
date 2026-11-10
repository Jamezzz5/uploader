import logging
import functools

import pandas as pd
import pytest

import upload.creator as cre
import upload.utils as utl


def fake_read(state, file_name, kwargs=None):
    """Return a copy of the frame the relation pass would read."""
    frame = state['read'].setdefault(file_name, pd.DataFrame())
    return frame.copy()


def fake_write(state, frame, file_name, sheet_name='Sheet1'):
    """Record the frame the relation pass writes."""
    state['written'][file_name] = frame.copy()


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    """Run the creator against paths inside a per-test directory."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def excel_io(monkeypatch):
    """Stand in for the workbook read/write around the relation pass."""
    state = {'read': {}, 'written': {}}
    monkeypatch.setattr(utl, 'read_excel',
                        functools.partial(fake_read, state))
    monkeypatch.setattr(utl, 'write_df',
                        functools.partial(fake_write, state))
    return state


def make_creator(df, cdf, state):
    """Build a Creator on the relation frame and target workbook."""
    state['read']['new_file.xlsx'] = cdf
    return cre.Creator('name', False, 'new_file.xlsx',
                       cc_file_path=None, df=df)


def test_missing_relation_column_does_not_raise(workdir, excel_io,
                                                caplog):
    """A relation frame missing the header warns, not KeyError."""
    cdf = pd.DataFrame({'name': ['row1', 'row2'], 'other': ['a', 'b']})
    cr = make_creator(
        pd.DataFrame({'wrong_header': ['x'], 'another': ['y']}), cdf,
        excel_io)
    with caplog.at_level(logging.WARNING):
        error_dict = cr.apply_relations()
    assert error_dict == {}
    assert 'impacted_column_name' in caplog.text
    back = excel_io['written']['new_file.xlsx']
    assert back.columns.tolist() == cdf.columns.tolist()
    assert back.values.tolist() == cdf.values.tolist()


def test_empty_relation_frame_leaves_file_unchanged(workdir, excel_io):
    """An empty relation frame leaves the target file unchanged."""
    cdf = pd.DataFrame({'name': ['row1'], 'other': ['a']})
    cr = make_creator(
        pd.DataFrame(columns=['column_name', 'column_value', 'position',
                              'impacted_column_name',
                              'impacted_column_new_value']), cdf,
        excel_io)
    assert cr.apply_relations() == {}
    back = excel_io['written']['new_file.xlsx']
    assert back.values.tolist() == cdf.values.tolist()


def test_constant_relation_still_applied(workdir, excel_io):
    """A Constant position still rewrites the impacted column."""
    cdf = pd.DataFrame({'name': ['row1', 'row2'], 'other': ['a', 'b']})
    rel = pd.DataFrame({
        'column_name': ['name'],
        'column_value': ['row1'],
        'position': ['Constant'],
        'impacted_column_name': ['other'],
        'impacted_column_new_value': ['NEW'],
    })
    cr = make_creator(rel, cdf, excel_io)
    assert cr.apply_relations() == {}
    back = excel_io['written']['new_file.xlsx']
    assert back['other'].tolist() == ['NEW', 'NEW']


def test_position_relation_still_applied(workdir, excel_io):
    """A position-based relation still rewrites the impacted column."""
    cdf = pd.DataFrame({'name': ['x_a', 'y_a'], 'other': ['', '']})
    rel = pd.DataFrame({
        'column_name': ['name'],
        'column_value': ['a'],
        'position': ['1'],
        'impacted_column_name': ['other'],
        'impacted_column_new_value': ['NEW'],
    })
    cr = make_creator(rel, cdf, excel_io)
    assert cr.apply_relations() == {'other': 0}
    back = excel_io['written']['new_file.xlsx']
    assert back['other'].tolist() == ['NEW', 'NEW']
