@echo off

setlocal ENABLEEXTENSIONS
setlocal ENABLEDELAYEDEXPANSION

set  exit_code=0
set  error_msg=""

set  BuildConfiguration=%1
set  BuildMode=%2

@if not "%BuildConfiguration%"=="Release" if not "%BuildConfiguration%"=="Debug" set BuildConfiguration=Release
@if not "%BuildMode%"=="Build" if not "%BuildMode%"=="Rebuild" set BuildMode=Build
set BuildPlatform="Any CPU"
@set build_options=/t:%BuildMode% /p:Configuration=%BuildConfiguration% /p:Platform=%BuildPlatform% /m  /consoleloggerparameters:ErrorsOnly

@echo INFO: try to locate MSBuild.exe
where MSBuild.exe
set MSBuild_where=%errorlevel%
if %MSBuild_where%==0 echo INFO: MSBuild.exe found.
if %MSBuild_where%==0 goto vcpp_ok
@echo INFO: MSBuild.exe not found. 

REM Note to self: devenv/MSBuild detection is a duplication of a subset
REM  of https://github.com/jmp75/config-utils/blob/master/R/packages/msvs/exec/setup_vcpp.cmd
set VSCOMNTOOLS=%VS140COMNTOOLS%
@if "%VSCOMNTOOLS%"=="" goto error_no_VS140COMNTOOLSDIR

set VSDEVENV="%VSCOMNTOOLS%..\..\VC\vcvarsall.bat"
REM echo VSDEVENV="%VSCOMNTOOLS%..\..\VC\vcvarsall.bat"
@if not exist %VSDEVENV% goto error_no_vcvarsall
@echo calling %VSDEVENV%
@call %VSDEVENV%
goto vcpp_ok

:error_no_VS140COMNTOOLSDIR
set  error_msg="ERROR: setup_vcpp cannot determine the location of the VS Common Tools folder."
set  exit_code=1
@goto end

:error_no_vcvarsall
set  error_msg="ERROR: Cannot find file %VSDEVENV%""
set  exit_code=1
@goto end

if errorlevel 0  goto vcpp_ok
set  error_msg="ERROR: call to setup_vcpp failed"
set exit_code=%errorlevel%
goto end

:vcpp_ok
set current_dir=%~d0%~p0.\
REM Build the solution
set  SLN=%current_dir%..\MPI.sln
echo MSBuild.exe %SLN% %build_options%
MSBuild.exe %SLN% %build_options%
@if errorlevel 1 goto Build_fail
goto end

REM copy the binaries to a single folder?

:Build_fail
set  error_msg="ERROR: Build failed""
set  exit_code=1
@goto end

:end
REM @echo exit_code=%exit_code%
@if not %exit_code%==0 echo %error_msg%
exit /b %exit_code%
