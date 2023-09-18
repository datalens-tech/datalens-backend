from __future__ import annotations

from bi_connector_bundle_ch_filtered.base.bi.connection_form.form_config import (
    ServiceConnectionWithTokenBaseFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import Translatable
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.bi.connection_info import (
    CHYaMusicPodcastStatsConnectionInfoProvider,
)


class CHYaMusicPodcastStatsConnectionFormFactory(ServiceConnectionWithTokenBaseFormFactory):
    template_name = "podcast_analytics"

    def _title(self) -> str:
        return CHYaMusicPodcastStatsConnectionInfoProvider.get_title(self._localizer)

    def _description(self) -> str:
        docs_url = self._localizer.translate(Translatable("cloud-datalens-docs-url"))
        docs_url += "/security"

        passport_url = "https://passport.yandex.ru/profile"

        return self._localizer.translate(Translatable("label_ya-podcasts-conn-description")).format(
            SEC_LINK=docs_url,
            PASSPORT_LINK=passport_url,
        )
