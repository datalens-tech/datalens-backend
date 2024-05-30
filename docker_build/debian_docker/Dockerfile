FROM ghcr.io/datalens-tech/docker-mirror/debian:bookworm

# run all scripts in ./scripts in alphabetical order
COPY ./scripts /tmp/scripts
RUN chmod a+x /tmp/scripts/*.sh && \
    run-parts --regex '.*sh$' \
    --exit-on-error \
    /tmp/scripts && \
    rm -rf /tmp/scripts
