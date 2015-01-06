#!/bin/bash

# NB: no attempt has been made here to make these tests run anywhere other than
# the ubuntu 14.4 machine I'm sitting at right now. Investment in portability
# will need to happen some other time.

# The test procedure is as follows:
# 1) Compile a bunch of object files and link binaries for running tests that
#    exercise libgit2 with this backend
# 2) Check that a mysql configuration is available, and works
# 3) Proceed to run a set of programs that exercise libgit2, checking the
#    contents of the db after each program, and clearing the db once done.
# 4) Report on results

# 1. Compile object files and programs that make up the tests.
gcc common.c -c

# 2. Test and/or configure mysql connection

if test -z "$LIBGIT2_MYSQL_TEST_HOSTNAME"; then
  LIBGIT2_MYSQL_TEST_HOSTNAME="localhost"
fi

if test -z "$LIBGIT2_MYSQL_TEST_USERNAME"; then
  LIBGIT2_MYSQL_TEST_USERNAME="root"
fi

if test -z "$LIBGIT2_MYSQL_TEST_PASSWORD"; then
  LIBGIT2_MYSQL_TEST_PASSWORD="" # Yup
fi

if test -z "$LIBGIT2_MYSQL_TEST_DBNAME"; then
  LIBGIT2_MYSQL_TEST_DBNAME="gitdb"
fi

# Actually test this configuration
# Privacy please
umask 177
optionfile=`mktemp`

trap "rm $optionfile;" 0 SIGINT SIGTERM

echo "[client]
host=$LIBGIT2_MYSQL_TEST_HOSTNAME
user=$LIBGIT2_MYSQL_TEST_USERNAME
password=$LIBGIT2_MYSQL_TEST_PASSWORD
database=$LIBGIT2_MYSQL_TEST_DBNAME" > $optionfile

mysqlcmd="mysql --defaults-file=$optionfile "

# Test connection

echo "show databases;" | $mysqlcmd > /dev/null
if test "$?" != 0; then
  echo "Could not connect to test database" >&2
  exit 1
fi

# Check that there's nothing in the current db

text=`echo "show tables;" | $mysqlcmd`
if test ! -z "$text"; then
  echo "Non-empty test database: refusing to insert stuff into it" >&2
  exit 1
fi

# Check we have permissions on this db

echo 'CREATE TABLE `testtable` (`bees` tinyint(1));' | $mysqlcmd;
if test "$?" != 0; then
  echo "Could not create test table; does the test account have perms on the test db?" >&2
  exit 1
fi

# Once that's done, drop it

echo 'DROP TABLE `testtable`;' | $mysqlcmd;
if test "$?" != 0; then
  echo "Created test table in DB, but can't drop it: help!" >&2
  exit 1
fi

# 3. Run tests
