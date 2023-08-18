# Merge updated messages.pot template file into .po files
.PHONY: update-po
update-po:
	find . -name "*.py" > POTFILES
	xgettext -d i18n_test --keyword=Translatable --files-from=POTFILES
	for locale in $$(ls locales/) ; do \
		msgmerge locales/$$locale/LC_MESSAGES/i18n_test.po i18n_test.po > locales/$$locale/LC_MESSAGES/i18n_test.po_tmp ; \
		mv locales/$$locale/LC_MESSAGES/i18n_test.po_tmp locales/$$locale/LC_MESSAGES/i18n_test.po ; \
	done
	rm POTFILES
	rm i18n_test.po

.PHONY: msgfmt
msgfmt:
	for locale in $$(ls locales/) ; do \
		msgfmt -o locales/$$locale/LC_MESSAGES/i18n_test.mo locales/$$locale/LC_MESSAGES/i18n_test.po ; \
	done
