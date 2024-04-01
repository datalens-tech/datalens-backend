from __future__ import annotations

from dl_query_processing.postprocessing.postprocessors.markup import (
    MarkupProcessing,
    MarkupProcessingDC,
)


def _test_markup_postprocessing_i(cls, check_nodes=True):
    MPP = cls()
    formulated = MPP.n_concat(
        'url: "',
        MPP.n_url("col1", "col2"),
        '"; "',
        MPP.n_i(MPP.n_b(MPP.n_url("col3", "col4"))),
        '"',
        "",
        MPP.n_br(),
        MPP.n_cl(MPP.n_sz("col2", "L"), "#dddddd"),
        MPP.n_userinfo("123", "email"),
        MPP.n_img("src", "", 18, None),
    )
    if MPP._dbg:
        print("formulated:", formulated)
    assert formulated
    if check_nodes:
        expected = (
            "c",
            'url: "',
            ("a", "col1", "col2"),
            '"; "',
            ("i", ("b", ("a", "col3", "col4"))),
            '"',
            "",
            ("br",),
            ("cl", ("sz", "col2", "L"), "#dddddd"),
            ("userinfo", "123", "email"),
            ("img", "src", "", 18, None),
        )
        assert formulated == expected
    dumped = MPP.dump(formulated)
    if MPP._dbg:
        print("dumped:", repr(dumped))
    assert (
        dumped
        == '(c "url: """ (a "col1" "col2") """; """ (i (b (a "col3" "col4"))) """" "" (br ) (cl (sz "col2" "L") "#dddddd") (userinfo "123" "email") (img "src" "" "18" ""))'
    )
    parsed = MPP.parse(dumped)
    if MPP._dbg:
        print("parsed:    ", parsed)
        print("formulated:", formulated)
    verbalized = MPP.verbalize(parsed)
    if MPP._dbg:
        print("verbalized:", verbalized)
    expected = {
        "type": "concat",
        "children": [
            {"type": "text", "content": 'url: "'},
            {"type": "url", "url": "col1", "content": {"type": "text", "content": "col2"}},
            {"type": "text", "content": '"; "'},
            {
                "type": "italics",
                "content": {
                    "type": "bold",
                    "content": {"type": "url", "url": "col3", "content": {"type": "text", "content": "col4"}},
                },
            },
            {"type": "text", "content": '"'},
            {"type": "text", "content": ""},
            {"type": "br"},
            {
                "type": "color",
                "color": "#dddddd",
                "content": {"type": "size", "size": "L", "content": {"type": "text", "content": "col2"}},
            },
            {"type": "user_info", "content": {"type": "text", "content": "123"}, "user_info": "email"},
            {
                "type": "img",
                "src": "src",
                "width": None,
                "height": 18,
                "alt": None,
            },
        ],
    }
    assert verbalized == expected


def test_markup_postprocessing():
    _test_markup_postprocessing_i(MarkupProcessing, check_nodes=True)
    _test_markup_postprocessing_i(MarkupProcessingDC, check_nodes=False)
