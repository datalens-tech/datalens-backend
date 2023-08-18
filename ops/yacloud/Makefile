.PHONY: kyverno-policies-set-audit
kyverno-policies-set-audit:
	kubectl apply -f kyverno-policies-audit.yaml

.PHONY: kyverno-policies-check
kyverno-policies-check:
	 kubectl get clusterpolicies.kyverno.io -A

.PHONY: kyverno-pods-check
kyverno-pods-check:
	kubectl -n yc-security get pod -l app=kyverno
