::
:: Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html
:: _________        _____ __________________        _____
:: __  ____/___________(_)______  /__  ____/______ ____(_)_______
:: _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
:: / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
:: \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
::
:: Version: 3.1.1c.05062011
::

@echo off

:: Check GROOVY_HOME.
if not "%GROOVY_HOME%" == "" goto run
    echo %0, ERROR: GROOVY_HOME environment variable is not found.
    echo %0, ERROR: GROOVY_HOME variable must point to Groovy installation folder.
goto error_finish

:run

:: This is Ant-augmented variable.
set ANT_AUGMENTED_GGJAR=gridgain-3.1.1c.jar

"%GROOVY_HOME%\bin\groovyc.bat" -cp "%GRIDGAIN_HOME%\%ANT_AUGMENTED_GGJAR%"  .\org\gridgain\grid\groovy\examples\helloworld\gridify\basic\gridifyhelloworldgroovybasicexample.groovy


:error_finish

pause
