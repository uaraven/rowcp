# Row copy

Performs deep row copy from one database to another, including all rows from child and parent tables.

## Purpose

Ever needed to test something locally on a couple of rows of data from staging/uat/prod data? You only need a couple of
entities, but the tables are intertwined with all the relationships, so you spend an hour copying all the required data
from a dozen different tables just to have that two rows that you're interested in. Then next day you need to do the
same, but with a different rows. And you've shut down your postgres container, so the database is clean, and you need to
start from scratch.

Enters `rowcp`. Rowcp has a very limited purpose - copy a couple of rows from the table in one database into a same
table in the different database while satisfying all the dependencies. All the rows from the tables that your table has
a foreign key to, and all the rows they have a foreign key to, and all rows from the tables that have a foreign key
pointing to these two rows that you're copying and so on.

## Usage

rowcp copies table rows from the source database to the target database. Source and target schemas must be the same.
Source and target DBMS can be different. You can use rowcp to copy data from mysql to postgres and vice versa.

Currently, only postgresql, mysql, mariadb and h2 are supported.

To copy rows run

    java -jar rowcp.jar [options] Query 

To see all supported command line arguments run rowcp with `--help` parameter.

Absolutely required are `--source-connection` and `--target-connection` that specify JDBC connection string for source
and target databases respectively.

Query is a seed query that returns rows that need to be copied to the target database.

There are some rules this query must abide:

- query must be a `SELECT *` query
- it must have only one table in the `FROM` clause. No joins are allowed.
- `WHERE` clause must be present to limit the number of rows selected
- it must be a valid SQL query in the SQL dialect of source database

There is no limitation on what you can have in `WHERE` clause, but it is highly recommented to limit number of rows
copied to be low (in the 1 to 10 range), as depending on number of relationships, actual number of copied rows can grow
exponentially and rowcp is not build for speed and efficiency.

## Limitations

Rowcp is intended to help with testing, not to copy databases. It is highly recommended that seed query select a limited
number of rows.

It is assumed that target database is empty or almost empty and there would be no conflicts in the data. Rowcp does not
try to clean up target tables or update existing data, if there is a duplicate key situation, then copying **will**
fail.

Autogenerated fields in target database are not supported. All the autoincremented fields will be inserted as they are
in the source database.