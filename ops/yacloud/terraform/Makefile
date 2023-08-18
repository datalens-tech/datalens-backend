SHIMS = .shims
SCRIPTS = .scripts

TERRAFORM = $(SHIMS)/terraform
TERRAFORM_ALL = $(SCRIPTS)/terraform_all
TERRAFORM_PLUGIN_CACHE = $(HOME)/.terraform.d/plugin-cache

SHELL = /bin/bash
.PHONY: init
init:
	@echo 'Create directory for terraform plugin cache...'
	@mkdir -p $(TERRAFORM_PLUGIN_CACHE)

.PHONY: lint
lint:
	@echo 'Running terraform fmt checks...'
	@$(TERRAFORM) fmt -recursive -check

.PHONY: lint-fix
lint-fix:
	@echo 'Running terraform fmt autofixes...'
	@$(TERRAFORM) fmt -recursive -diff

.PHONY: clean
clean:
	@echo 'Cleaning terraform shims...'
	@rm -rf .shims/bin

	@echo 'Cleaning local terraform directories'
	@$(foreach layer, $(sort $(wildcard src/*/*)), \
		echo "Cleaning $(layer)/.terraform..."; \
		rm -rf "$(layer)/.terraform" || exit 1; \
	)

.PHONY: clean-plugin-cache
clean-plugin-cache:
	@echo 'Cleaning terraform plugin cache...'
	@rm -rf $(TERRAFORM_PLUGIN_CACHE)

.PHONY: yc-preprod-init
yc-preprod-init:
	@$(TERRAFORM_ALL) init -env yc-preprod

.PHONY: yc-preprod-validate
yc-preprod-validate:
	@$(TERRAFORM_ALL) validate -env yc-preprod

.PHONY: yc-preprod-plan
yc-preprod-plan:
	@$(TERRAFORM_ALL) plan -env yc-preprod

.PHONY: yc-preprod-apply
yc-preprod-apply:
	@$(TERRAFORM_ALL) apply -env yc-preprod

.PHONY: yc-preprod-refresh
yc-preprod-refresh:
	@$(TERRAFORM_ALL) refresh -env yc-preprod
