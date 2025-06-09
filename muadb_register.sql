-- MuaDB Table Access Method Registration Script
-- This script registers the MuaDB table access method in PostgreSQL

-- First, create the handler function
CREATE OR REPLACE FUNCTION muadb_tableam_handler(internal)
RETURNS table_am_handler
AS 'muadb_tableam_handler'
LANGUAGE internal STRICT;

-- Create the table access method
CREATE ACCESS METHOD muadb TYPE TABLE HANDLER muadb_tableam_handler;

-- Verify the registration
SELECT oid, amname, amhandler FROM pg_am WHERE amname = 'muadb'; 