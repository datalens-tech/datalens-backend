# TODO: generate certs in https://st.yandex-team.ru/BI-4701
# keep root.crt, server.crt, server.key in repo, evict all other generated files, remove .arcignore
.PHONY: gen-certs
gen-certs:
	@echo "Generating certificates..."
	@echo "URL: https://www.postgresql.org/docs/current/ssl-tcp.html#SSL-CERTIFICATE-CREATION"

	@echo "Generating self-signed certificate for the server..."
	@openssl req -new -x509 -days 365 -nodes -text -out server.crt \
  		-keyout server.key -subj "/CN=root.yourdomain.com"
	@chmod og-rwx server.key

	@echo "Generating a certificate signing request for the server..."
	@openssl req -new -nodes -text -out root.csr \
	  -keyout root.key -subj "/CN=root.yourdomain.com"
	@chmod og-rwx root.key

	@echo "Signing the request with the key to create a root certificate authority..."
	@openssl x509 -req -in root.csr -text -days 3650 \
	  -extfile /etc/ssl/openssl.cnf -extensions v3_ca \
	  -signkey root.key -out root.crt

	@echo "Generating a server certificate signed by the new root certificate authority..."
	@openssl req -new -nodes -text -out server.csr \
	  -keyout server.key -subj "/CN=dbhost.yourdomain.com"
	@chmod og-rwx server.key
	@openssl x509 -req -in server.csr -text -days 365 \
	  -CA root.crt -CAkey root.key -CAcreateserial \
	  -out server.crt
