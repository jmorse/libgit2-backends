#!/bin/bash

conffile=$1
mysqlcmd="mysql --defaults-file=$1"

# Corresponding test simply creates the relevant tables in the database. Just
# check for this, the schema will be exercised by later tests.

text=`echo "SHOW TABLES;" | $mysqlcmd`;

# There should be two tables, one for odb, the other refdb. Most importantly,
# there should be nothing else! Output from mysql has newlines embedded in it
if test "$text" != "Tables_in_$LIBGIT2_MYSQL_TEST_DBNAME
git2_odb
git2_refdb"; then
	echo "Unexpected create_db table output:" >&2
	echo $text >&2
	exit 1
fi
