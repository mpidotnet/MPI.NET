# Copyright (C) 2017  CSIRO
#
# Use, modification and distribution is subject to the Boost Software
# License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt)
#  
# Authors: Jean-Michel Perraud
#
# Linux batch script that executes all of the regression tests locally.
#
# Usage:
#   cd path/to/MPI.NET
#   ./Tests/runtests.sh

# CONFIGURATION=%1%
# set FAILURES=
# echo exename=$0
tests_folder=`dirname $0`
mpidotnet_folder=$tests_folder/..

OLDDIR=`pwd`

cd $tests_folder
test_names=`ls -d *Test`
cd $OLDDIR

for test_name in $test_names ; do 
  echo $tests_folder/runtest.sh $mpidotnet_folder ${tests_folder}/${test_name}/${test_name}.exe  
  $tests_folder/runtest.sh $mpidotnet_folder ${tests_folder}/${test_name}/${test_name}.exe  
  if test $? == 0 ; then
    echo "PASS $test_name (with $procs processes)"
  else
    echo "FAIL $test_name (with $procs processes)"
    result_code=-1
  fi
done

