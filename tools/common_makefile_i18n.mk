# Common targets related to i18n

# Merge updated messages.pot template file into .po files for a specific domain
.PHONY: update-po-domain
update-po-domain:
	cd ../../docker_build && \
	./run-project-bake update-po \
	    --set update-po.args.PACKAGE_NAME=$(PACKAGE_NAME) \
	    --set update-po.args.DOMAIN_NAME=$(DOMAIN_NAME) \
	    --set update-po.args.PATH_MASK=$(PATH_MASK) \
	    --set update-po.contexts.src=$(PWD) \
	    --set update-po.output="type=local,dest=$(PWD)/$(PACKAGE_NAME)"

.PHONY: update-po-domain-api
update-po-domain-api:
	DOMAIN_NAME=$(PACKAGE_NAME) PATH_MASK="/bi/" make update-po-domain


.PHONY: update-po-domain-formula-ref
update-po-domain-formula-ref:
	DOMAIN_NAME=bi_formula_ref_$(PACKAGE_NAME) PATH_MASK="/formula_ref/" make update-po-domain


.PHONY: update-po
update-po:
	@echo "Running for 'api'"
	@make update-po-domain-api  2> /dev/null || true
	@echo "Running for 'formula_ref'"
	@make update-po-domain-formula-ref 2> /dev/null || true


# Compile .po files into .mo files for a specific domain
.PHONY: msgfmt-domain
msgfmt-domain:
	cd ../../docker_build && \
	./run-project-bake msgfmt \
	    --set msgfmt.args.PACKAGE_NAME=$(PACKAGE_NAME) \
	    --set msgfmt.args.DOMAIN_NAME=$(DOMAIN_NAME) \
	    --set msgfmt.contexts.src=$(PWD) \
	    --set msgfmt.output="type=local,dest=$(PWD)/$(PACKAGE_NAME)"


.PHONY: msgfmt-domain-api
msgfmt-domain-api:
	DOMAIN_NAME=$(PACKAGE_NAME) make msgfmt-domain


.PHONY: msgfmt-domain-formula-ref
msgfmt-domain-formula-ref:
	DOMAIN_NAME=bi_formula_ref_$(PACKAGE_NAME) make msgfmt-domain


.PHONY: msgfmt
msgfmt:
	@echo "Running for 'api'"
	@make msgfmt-domain-api  2> /dev/null || true
	@echo "Running for 'formula_ref'"
	@make msgfmt-domain-formula-ref 2> /dev/null || true
