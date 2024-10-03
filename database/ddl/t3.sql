-- Create the database
CREATE SCHEMA s1;

-- Create the enum type for the schema 'type' column
CREATE TYPE s1.schema_type AS ENUM ('avro', 'json', 'protobuf', 'xsd', 'thrift', 'confluent');

-- Create the schema table
CREATE TABLE s1.schema (
                           id SERIAL PRIMARY KEY,
                           name VARCHAR(255) NOT NULL,
                           type schema_type NOT NULL,
                           version VARCHAR(15) NOT NULL,
                           schema_data JSONB NOT NULL,
                           created     timestamp,
                           modified    timestamp
);

ALTER TABLE s1.schema
    ADD CONSTRAINT unique_name_type_version
        UNIQUE (name, type, version);