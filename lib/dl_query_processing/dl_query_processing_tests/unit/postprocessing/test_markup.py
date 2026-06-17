from __future__ import annotations

from dl_query_processing.postprocessing.postprocessors.markup import (
    MarkupProcessing,
    MarkupProcessingDC,
)


def _test_markup_postprocessing_i(cls, check_nodes=True):
    mpp = cls()
    formulated = mpp.n_concat(
        'url: "',
        mpp.n_url("col1", "col2"),
        '"; "',
        mpp.n_i(mpp.n_b(mpp.n_url("col3", "col4"))),
        '"',
        "",
        mpp.n_br(),
        mpp.n_cl(mpp.n_sz("col2", "L"), "#dddddd"),
        mpp.n_userinfo("123", "email"),
        mpp.n_img("src", "", 18, None),
        mpp.n_tooltip("tooltip_text", "tooltip_tooltip"),
    )
    if mpp._dbg:
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
            ("tooltip", "tooltip_text", "tooltip_tooltip"),
        )
        assert formulated == expected
    dumped = mpp.dump(formulated)
    if mpp._dbg:
        print("dumped:", repr(dumped))
    assert (
        dumped
        == '(c "url: """ (a "col1" "col2") """; """ (i (b (a "col3" "col4"))) """" "" (br ) (cl (sz "col2" "L") "#dddddd") (userinfo "123" "email") (img "src" "" "18" "") (tooltip "tooltip_text" "tooltip_tooltip"))'
    )
    parsed = mpp.parse(dumped)
    if mpp._dbg:
        print("parsed:    ", parsed)
        print("formulated:", formulated)
    verbalized = mpp.verbalize(parsed)
    if mpp._dbg:
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
            {
                "type": "tooltip",
                "content": {"type": "text", "content": "tooltip_text"},
                "tooltip": {"type": "text", "content": "tooltip_tooltip"},
                "placement": "auto",
            },
        ],
    }
    assert verbalized == expected


def test_markup_postprocessing():
    _test_markup_postprocessing_i(MarkupProcessing, check_nodes=True)
    _test_markup_postprocessing_i(MarkupProcessingDC, check_nodes=False)
