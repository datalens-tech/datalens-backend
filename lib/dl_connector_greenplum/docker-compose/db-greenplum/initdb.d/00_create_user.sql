DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'datalens') THEN
        CREATE ROLE datalens WITH LOGIN PASSWORD 'qwerty' SUPERUSER;
    END IF;
END
$$;