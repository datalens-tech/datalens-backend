import re
from typing import (
    Callable,
    Optional,
    Sequence,
)

from bi_external_api.attrs_model_mapper_docs.render_units import DocLink

INLINE_LINK_RE = re.compile(r"\[([^]]+)]\(([^)]+)\)")


def process_links(
    txt: str,
    link_processor: Optional[Callable[[DocLink], Optional[DocLink]]] = None,
) -> Sequence[str | DocLink]:
    """
    Tokenize `txt` into 2 types of tokens: plain text & links.
    Links can be processed by `link_processor`.
    If `link_processor` returns `None`
     - link will be treated as plain text and will not be extracted as dedicated token.
    If `link_processor` is `None` all links will be extracted as-is.
    """
    ret: list[str | DocLink] = []
    effective_link_processor: Callable[[DocLink], Optional[DocLink]] = (
        link_processor if link_processor is not None else lambda x: x
    )
    pos = 0

    for matcher in INLINE_LINK_RE.finditer(txt):
        span_start = matcher.span()[0]
        span_end = matcher.span()[1]

        doc_link = DocLink(text=matcher.group(1), href=matcher.group(2))
        processed_link = effective_link_processor(doc_link)

        if processed_link is not None:
            if pos != span_start:
                ret.append(txt[pos:span_start])
            ret.append(processed_link)
            pos = span_end

    if pos < len(txt):
        ret.append(txt[pos : len(txt)])

    return ret
