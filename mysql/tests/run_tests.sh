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


