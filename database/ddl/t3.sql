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

-- Create a schema for a user account table tied to the user credentials
CREATE TABLE s1.user_account (
                                 id SERIAL PRIMARY KEY,
                                 username VARCHAR(255) NOT NULL,
                                 password VARCHAR(255) NOT NULL,
                                 email VARCHAR(255) NOT NULL,
                                 created timestamp,
                                 modified timestamp
);


ALTER TABLE s1.schema
    ADD CONSTRAINT unique_name_type_version
        UNIQUE (name, type, version);

-- Add unique constraint to the user_account table on the username and email columns
ALTER TABLE s1.user_account
    ADD CONSTRAINT unique_username_email
        UNIQUE (username, email);