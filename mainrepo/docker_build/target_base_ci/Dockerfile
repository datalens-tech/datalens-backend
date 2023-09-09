ARG BASE_IMG
FROM $BASE_IMG

RUN mkdir -p /src
COPY --from=metapkg / /src/

COPY ./scripts /tmp/scripts
RUN chmod a+x /tmp/scripts/*.sh && \
    run-parts --regex '.*sh$' \
    --exit-on-error \
    /tmp/scripts && \
    rm -rf /tmp/scripts
