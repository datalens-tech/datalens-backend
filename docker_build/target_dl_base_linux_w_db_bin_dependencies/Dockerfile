ARG BASE_IMG
FROM $BASE_IMG

COPY ./scripts /tmp/scripts
RUN chmod a+x /tmp/scripts/*.sh && \
    run-parts --regex '.*sh$' \
    --exit-on-error \
    /tmp/scripts && \
    rm -rf /tmp/scripts
