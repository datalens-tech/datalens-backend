-- Create schemas that test edge cases for schema filtering
-- This schema name starts with 'pg' but doesn't have an underscore after it,
-- so it should NOT be filtered out by the pattern 'pg\_%'
CREATE SCHEMA IF NOT EXISTS pgmyschema;
CREATE TABLE pgmyschema.test_table (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255)
);
INSERT INTO pgmyschema.test_table (id, name) VALUES (1, 'test');
