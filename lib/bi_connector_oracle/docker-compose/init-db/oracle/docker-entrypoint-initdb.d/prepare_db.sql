GRANT SELECT ON sys.V_$SESSION TO datalens;


CREATE TABLE datalens.empty_table (
    -- So that template listings are not empty
    "value" VARCHAR2(255)
);