#!/bin/bash

conffile=$1
mysqlcmd="mysql --defaults-file=$1"

# Corresponding test simply creates the relevant tables in the database. Just
# check for this, the schema will be exercised by later tests.

echo "SHOW TABLES;" | $mysqlcmd | tail -n +2 | while read x; do
	echo "DROP TABLE $x;" | $mysqlcmd;
	if test "$?" != 0; then
		echo "Failed to drop / reset database table" >&2;
		exit 1;
	fi
done
