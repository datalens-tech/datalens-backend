from __future__ import annotations

from dl_query_processing.postprocessing.postprocessors.markup import MarkupProcessing, MarkupProcessingDC


def _test_markup_postprocessing_i(cls, check_nodes=True):
    MPP = cls()
    formulated = MPP.n_concat(
        'url: "',
        MPP.n_url('col1', 'col2'), '"; "',
        MPP.n_i(MPP.n_b(MPP.n_url('col3', 'col4'))),
        '"',
        '')
    if MPP._dbg:
        print('formulated:', formulated)
    assert formulated
    if check_nodes:
        expected = (
            'c', 'url: "',
            ('a', 'col1', 'col2'),
            '"; "',
            ('i', ('b', ('a', 'col3', 'col4'))),
            '"', '')
        assert formulated == expected
    dumped = MPP.dump(formulated)
    if MPP._dbg:
        print('dumped:', repr(dumped))
    assert dumped == '(c "url: """ (a "col1" "col2") """; """ (i (b (a "col3" "col4"))) """" "")'
    parsed = MPP.parse(dumped)
    if MPP._dbg:
        print('parsed:    ', parsed)
        print('formulated:', formulated)
    assert parsed == formulated
    verbalized = MPP.verbalize(parsed)
    if MPP._dbg:
        print('verbalized:', verbalized)
    expected = {'type': 'concat', 'children': [
        {'type': 'text', 'content': 'url: "'},
        {'type': 'url', 'url': 'col1', 'content': {'type': 'text', 'content': 'col2'}},
        {'type': 'text', 'content': '"; "'},
        {'type': 'italics', 'content': {
            'type': 'bold', 'content': {
                'type': 'url', 'url': 'col3', 'content': {
                    'type': 'text', 'content': 'col4'}}}},
        {'type': 'text', 'content': '"'}, {'type': 'text', 'content': ''}]}
    assert verbalized == expected


def test_markup_postprocessing():
    _test_markup_postprocessing_i(MarkupProcessing, check_nodes=True)
    _test_markup_postprocessing_i(MarkupProcessingDC, check_nodes=False)
