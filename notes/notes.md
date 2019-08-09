# SQLite Notes

To dump the DB schema:

~~~~~

> sqlite3 mydatabase.sqlite3
.output mydb_schema.sql
.schema
.exit

~~~~~

As a single command line invocation: 

~~~~~

sqlite3 change_request.db ".output cr_schema.sql" ".schema" ".exit"

~~~~~


To create a database with schema in a file:

~~~~~

> sqlite3 newdb.db < schema.sql

