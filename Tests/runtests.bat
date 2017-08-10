@echo off
REM Copyright (C) 2007,2008  The Trustees of Indiana University
REM
REM Use, modification and distribution is subject to the Boost Software
REM License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
REM http://www.boost.org/LICENSE_1_0.txt)
REM  
REM Authors: Douglas Gregor
REM          Andrew Lumsdaine
REM
REM Windows batch script that executes all of the regression tests locally.
REM
REM Usage:
REM   runtests Debug [Testname] [Number of processes]

setlocal ENABLEEXTENSIONS
setlocal ENABLEDELAYEDEXPANSION
set CONFIGURATION=%1%
set MPIEXEC=mpiexec
set DEFAULT_SCHEDULE=1 2 7 8 13 16 21
set FAILURES=
set TESTS=*Test

if not "%CONFIGURATION%"=="Release" if not "%CONFIGURATION%"=="Debug" set CONFIGURATION=Debug

if "%2x" neq "x" ( 
  set TESTS=%2
) 

if "%3x" neq "x" (
  set FORCE_SCHEDULE=%3
)

for /D %%T in (%TESTS%) do (
  rem Determine the testing schedule for this test program
  
  if defined FORCE_SCHEDULE (
    set SCHEDULE=%FORCE_SCHEDULE%
  ) else if %%T equ CartTest ( 
    set SCHEDULE=8
  ) else if %%T equ DatatypesTest ( 
    set SCHEDULE=2
  ) else if %%T equ ExceptionTest (
    set SCHEDULE=2
  ) else if %%T equ GraphTest (
    set SCHEDULE=4 7 8 13 16 21
  ) else if %%T equ IntercommunicatorTest (
    set SCHEDULE=2 7 8 13 16 21
  ) else (
    set SCHEDULE=%DEFAULT_SCHEDULE%
  )

  cd "%%T\bin\%CONFIGURATION%"
  REM echo we are in "%%T\bin\%CONFIGURATION%"
  

  rem Loop over each number of processes in the schedule
  for %%P in (!SCHEDULE!) do (  
    echo Executing %%T with %%P processes...
    set  exit_code=0
    echo call %MPIEXEC% -exitcodes -n %%P %%T
    %MPIEXEC% -exitcodes -n %%P %%T
    set exit_code=!ERRORLEVEL!
    if !exit_code! neq 0 (
      if "!FAILURES!x" equ "x" ( 
        set FAILURES=%%T:%%P
      ) else (
        set FAILURES=!FAILURES! %%T:%%P
      )
      echo %%T with %%P processes... FAILED!
    ) else (
      echo %%T with %%P processes... PASSED
    )
    REM goto end
  )
  cd ..\..\..
)

if "%FAILURES%x" == "x" (
  echo All regression tests passed.
) else (
  echo ===Regression test failures===
  for %%F in (%FAILURES%) do (
    echo FAILED      %%F
  )
)

:end
