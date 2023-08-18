from bi_formula_ref.rich_text.elements import (
    MacroReplacementKey as MRK, RichText,
    ConditionalBlock, NoteBlock, AudienceBlock,
    CodeSpanTextElement, TermTextElement, ExtMacroTextElement,
    ListTextElement, LinkTextElement, TableTextElement,
)
from bi_formula_ref.rich_text.renderer import MdRichTextRenderer, RichTextRenderEnvironment


def test_term():
    renderer = MdRichTextRenderer()
    text = renderer.render(RichText(
        text='Hi ', replacements={MRK(3, 3): TermTextElement(term='int')}
    ))
    assert text == 'Hi `int`'
    text = renderer.render(RichText(
        text='Hi ', replacements={MRK(3, 3): TermTextElement(term='int', wrap=False)}
    ))
    assert text == 'Hi int'


def test_code_span():
    renderer = MdRichTextRenderer()
    text = renderer.render(RichText(
        text='Hi ', replacements={MRK(3, 3): CodeSpanTextElement(text='int', wrap=True)}
    ))
    assert text == 'Hi `int`'
    text = renderer.render(RichText(
        text='Hi ', replacements={MRK(3, 3): CodeSpanTextElement(text='int', wrap=False)}
    ))
    assert text == 'Hi int'


def test_ext_macro():
    renderer = MdRichTextRenderer()
    text = renderer.render(RichText(
        text='Hi ', replacements={MRK(3, 3): ExtMacroTextElement(macro_name='the_macro')}
    ))
    assert text == 'Hi {{ the_macro }}'


def test_list():
    renderer = MdRichTextRenderer()
    text = renderer.render(RichText(
        text='Hi ',
        replacements={MRK(3, 3): ListTextElement(
            items=[
                TermTextElement(term='int'),
                ExtMacroTextElement(macro_name='my_macro'),
            ],
            sep=' : ',
        )}
    ))
    assert text == 'Hi `int` : {{ my_macro }}'

    text = renderer.render(RichText(
        text='Hi ',
        replacements={MRK(3, 3): ListTextElement(
            items=[
                TermTextElement(term='int', wrap=False),
                ExtMacroTextElement(macro_name='my_macro'),
            ],
            sep=' : ',
            wrap=True,
        )}
    ))
    assert text == 'Hi `int : {{ my_macro }}`'


def test_link():
    renderer = MdRichTextRenderer()
    text = renderer.render(RichText(
        text='Hi ', replacements={MRK(3, 3): LinkTextElement(text='Whatever', url='site.com')}
    ))
    assert text == 'Hi [Whatever](site.com)'


def test_table():
    renderer = MdRichTextRenderer()
    text = renderer.render(RichText(
        text='Hi\n',
        replacements={MRK(3, 3): TableTextElement(
            table_body=[
                [RichText('My'), RichText('friend')],
                [TermTextElement(term='smth'), RichText('')],
            ],
        )},
    ))
    assert text == '''
Hi
| My     | friend   |
|:-------|:---------|
| `smth` |          |
'''.strip()


def test_conditional_block():
    rich_text = RichText(
        text='qwerty {if some_cond} uio{if smth_else}pp{end}p{end}',
        replacements={
            MRK(7, 52): ConditionalBlock(
                condition='some_cond',
                rich_text=RichText(
                    text=' uio{if smth_else}pp{end}p',
                    replacements={
                        MRK(4, 25): ConditionalBlock(
                            condition='smth_else',
                            rich_text=RichText(text='pp', replacements={}),
                        ),
                    },
                ),
            ),
        }
    )

    renderer = MdRichTextRenderer()
    env = RichTextRenderEnvironment(block_conditions={'some_cond': False, 'smth_else': False})
    assert renderer.render(rich_text, env=env) == 'qwerty '

    env = RichTextRenderEnvironment(block_conditions={'some_cond': True, 'smth_else': False})
    assert renderer.render(rich_text, env=env) == 'qwerty  uiop'

    env = RichTextRenderEnvironment(block_conditions={'some_cond': False, 'smth_else': True})
    assert renderer.render(rich_text, env=env) == 'qwerty '

    env = RichTextRenderEnvironment(block_conditions={'some_cond': True, 'smth_else': True})
    assert renderer.render(rich_text, env=env) == 'qwerty  uioppp'


def test_note_block():
    rich_text = RichText(
        text='qwerty {note} uio{end}{note warning}pp{end}',
        replacements={
            MRK(7, 21): NoteBlock(
                level='info', rich_text=RichText(text=' uio', replacements={}),
            ),
            MRK(21, 43): NoteBlock(
                level='warning', rich_text=RichText(text='pp', replacements={}),
            ),
        }
    )

    renderer = MdRichTextRenderer()
    env = RichTextRenderEnvironment()
    assert renderer.render(rich_text, env=env) == (
        'qwerty {% note info %} uio{% endnote %}{% note warning %}pp{% endnote %}'
    )


def test_audience_block():
    rich_text = RichText(
        text='qwerty {audience internal} uio{end}{audience this,that} pp{end}',
        replacements={
            MRK(7, 35): AudienceBlock(
                audience_types=['internal'], rich_text=RichText(text=' uio', replacements={}),
            ),
            MRK(35, 63): AudienceBlock(
                audience_types=['this', 'that'], rich_text=RichText(text=' pp', replacements={}),
            ),
        }
    )

    renderer = MdRichTextRenderer()
    env = RichTextRenderEnvironment()
    assert renderer.render(rich_text, env=env) == (
        'qwerty {% if audience == \'internal\' %} uio{% endif %}'
        '{% if audience in [\'this\', \'that\'] %} pp{% endif %}'
    )
