ARG BASE_IMG
FROM $BASE_IMG

COPY ./scripts /tmp/scripts
RUN chmod a+x /tmp/scripts/*.sh && \
    run-parts --regex '.*sh$' \
    --exit-on-error \
    /tmp/scripts && \
    rm -rf /tmp/scripts

# Configure libraries for MSSQL
RUN cat /usr/share/tdsodbc/odbcinst.ini >> /etc/odbcinst.ini
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/lib/x86_64-linux-gnu/odbc
