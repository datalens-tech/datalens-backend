from dl_constants import DataSourceType
from dl_core.connection_models import DataSourceTemplateDisabledText
from dl_core.i18n.localizer import (
    CONFIGS,
    Translatable,
)
from dl_core.us_connection_base import (
    make_subselect_datasource_template,
    make_table_datasource_template,
)
from dl_i18n.localizer_base import LocalizerLoader

SUBSELECT_SOURCE_TYPE = DataSourceType.declare("test_disabled_text_subselect_source")
TABLE_SOURCE_TYPE = DataSourceType.declare("test_disabled_text_table_source")

DISABLED_TEXT = DataSourceTemplateDisabledText(
    title=Translatable("source_templates-disabled-subselect-title"),
    description=Translatable("source_templates-disabled-subselect-description"),
)


def _get_localizer():
    return LocalizerLoader(configs=CONFIGS).load().get_for_locale("en")


def _translated(localizer):
    return {
        "title": localizer.translate(DISABLED_TEXT.title),
        "description": localizer.translate(DISABLED_TEXT.description),
    }


def test_subselect_template_attaches_translated_disabled_text():
    localizer = _get_localizer()
    template = make_subselect_datasource_template(
        connection_id="conn-1",
        source_type=SUBSELECT_SOURCE_TYPE,
        localizer=localizer,
        disabled=True,
        disabled_text=DISABLED_TEXT,
    )
    assert template.disabled is True
    assert template.disabled_text == _translated(localizer)


def test_subselect_template_attaches_text_even_when_enabled():
    # The helper always stores the provided text; the caller supplies state-appropriate
    # wording, so an enabled template still carries a (non-empty) message.
    localizer = _get_localizer()
    template = make_subselect_datasource_template(
        connection_id="conn-1",
        source_type=SUBSELECT_SOURCE_TYPE,
        localizer=localizer,
        disabled=False,
        disabled_text=DISABLED_TEXT,
    )
    assert template.disabled is False
    assert template.disabled_text == _translated(localizer)


def test_table_template_attaches_translated_disabled_text():
    localizer = _get_localizer()
    template = make_table_datasource_template(
        connection_id="conn-1",
        source_type=TABLE_SOURCE_TYPE,
        localizer=localizer,
        disabled=True,
        disabled_text=DISABLED_TEXT,
    )
    assert template.disabled is True
    assert template.disabled_text == _translated(localizer)
