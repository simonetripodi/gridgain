::
:: Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html
:: _________        _____ __________________        _____
:: __  ____/___________(_)______  /__  ____/______ ____(_)_______
:: _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
:: / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
:: \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
::
:: Version: 3.1.1c.17062011
::

::
:: Starts Scala REPL with GridGain on the classpath.
:: Note that 'scala' must be on PATH.
::

@echo off

if "%OS%" == "Windows_NT"  setlocal

:: Check Scala
for %%A in (scala) do if not "%%~$PATH:A" == "" goto checkGridGainHome1
    echo %0, ERROR: 'scala' must be on PATH.
goto error_finish

:: Check GRIDGAIN_HOME.
:checkGridGainHome1
if not "%GRIDGAIN_HOME%" == "" goto checkGridGainHome2
    echo %0, WARN: GRIDGAIN_HOME environment variable is not found.
    pushd "%~dp0"/..
    set GRIDGAIN_HOME=%CD%
    popd

:checkGridGainHome2
if exist "%GRIDGAIN_HOME%\config" goto run
    echo %0, ERROR: GRIDGAIN_HOME environment variable is not valid installation home.
    echo %0, ERROR: GRIDGAIN_HOME variable must point to GridGain installation folder.
goto error_finish

:run

:: This is Ant-augmented variable.
set ANT_AUGMENTED_GGJAR=gridgain-3.1.1c.jar

::
:: Set GRIDGAIN_LIBS
::
call "%GRIDGAIN_HOME%\bin\setenv.bat"

set CP=%GRIDGAIN_LIBS%;%GRIDGAIN_HOME%\%ANT_AUGMENTED_GGJAR%

::
:: Process 'verbose' mode and optional Spring configuration file.
::
if [%1] == [-v] (
    set QUIET=-DGRIDGAIN_QUIET=false
) else (
    set QUIET=-DGRIDGAIN_QUIET=true
)

::
:: Set program name.
::
set PROG_NAME=gridgain.bat
if "%OS%" == "Windows_NT" set PROG_NAME=%~nx0%

::
:: Set Java options.
::
set JAVA_OPTS=-Xss2m

::
:: Start REPL.
::
scala %QUIET%  -DGRIDGAIN_HOME="%GRIDGAIN_HOME%" -DGRIDGAIN_PROG_NAME="%PROG_NAME%" -cp "%CP%" -i %GRIDGAIN_HOME%\bin\scalar.scala

:error_finish
