import upload.utils as utl
import upload.creator as cre


def make_creator(tmp_path, columns):
    """A Creator whose relation frame is exactly ``columns``, with the
    upload file it would apply relations onto living on disk."""
    new_file = tmp_path / 'upload.xlsx'
    utl.write_df(_frame(['name', 'placement_name']), str(new_file))
    creator = cre.Creator('placement_name', False, str(new_file),
                          cc_file_path='')
    creator.df = _frame(columns)
    return creator, new_file


def _frame(columns):
    return utl.pd.DataFrame({c: ['v'] for c in columns})


def test_apply_relations_missing_impacted_column(tmp_path):
    """A relation frame lacking ``impacted_column_name`` applies nothing
    and returns the empty error dict, leaving the upload file's columns
    unchanged instead of raising KeyError."""
    creator, new_file = make_creator(
        tmp_path, ['column_name', 'column_value'])
    result = creator.apply_relations()
    assert result == creator.error_dict == {}
    written = utl.read_excel(str(new_file))
    assert written.columns.to_list() == ['name', 'placement_name']


def test_apply_relations_empty_frame(tmp_path):
    """A frame with no columns at all (an unreadable or empty relation
    sheet) is skipped the same way, reporting no errors."""
    creator, new_file = make_creator(tmp_path, [])
    assert creator.apply_relations() == {}
    assert utl.read_excel(str(new_file)).columns.to_list() == [
        'name', 'placement_name']


def test_apply_relations_with_column_unchanged(tmp_path):
    """With the relation columns present the Constant branch still writes
    the new value into the upload file and returns the error dict."""
    cols = [cre.Creator.rel_col_name, cre.Creator.rel_col_pos,
            cre.Creator.rel_col_val, cre.Creator.rel_col_imp,
            cre.Creator.rel_col_imp_new_value]
    creator, new_file = make_creator(tmp_path, cols)
    creator.df = utl.pd.DataFrame({
        cre.Creator.rel_col_name: ['placement_name'],
        cre.Creator.rel_col_pos: ['Constant'],
        cre.Creator.rel_col_val: ['v'],
        cre.Creator.rel_col_imp: ['body'],
        cre.Creator.rel_col_imp_new_value: ['copy']})
    assert creator.apply_relations() == {}
    written = utl.read_excel(str(new_file))
    assert 'body' in written.columns
    assert written['body'].to_list() == ['copy']
