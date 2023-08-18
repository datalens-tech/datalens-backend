# TODO: generate certs in https://st.yandex-team.ru/BI-4738
# keep certs, remove .arcignore
.PHONY: gen-certs
gen-certs:
	@echo "Generating CA certificate and key..."
	@openssl genrsa -out marsnet_ca.key 2048
	@openssl req -x509 -subj "/CN=localhost CA" -nodes -key marsnet_ca.key -days 1095 -out marsnet_ca.crt

	@echo "Generating server certificate and key..."
	@openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
		-addext "subjectAltName = DNS:127.0.0.1,IP:127.0.0.1" \
 		-keyout chnode.key -out chnode.csr
	@openssl x509 -req -in chnode.csr -out chnode.crt -CAcreateserial -CA marsnet_ca.crt -CAkey marsnet_ca.key
