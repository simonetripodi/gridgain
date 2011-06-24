::
:: Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html
:: _________        _____ __________________        _____
:: __  ____/___________(_)______  /__  ____/______ ____(_)_______
:: _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
:: / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
:: \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
::
:: Version: 3.1.1c.24062011
::

@echo off

if "%OS%" == "Windows_NT"  setlocal

:: Check JAVA_HOME.
if not "%JAVA_HOME%" == "" goto checkJdk
    echo %0, ERROR: JAVA_HOME environment variable is not found.
    echo %0, ERROR: Please create JAVA_HOME variable pointing to location of JDK 1.5 or JDK 1.6.
    echo %0, ERROR: You can also download latest JDK at: http://java.sun.com/getjava
goto error_finish

:checkJdk
:: Check that JDK is where it should be.
if exist "%JAVA_HOME%\bin\java.exe" goto checkGridGainHome1
    echo %0, ERROR: The JDK is not found in %JAVA_HOME%.
    echo %0, ERROR: Please modify your script so that JAVA_HOME would point to valid location of JDK.
goto error_finish

:: Check GRIDGAIN_HOME.
:checkGridGainHome1
if not "%GRIDGAIN_HOME%" == "" goto checkGridGainHome2
    echo %0, ERROR: GRIDGAIN_HOME environment variable is not found.
    echo %0, ERROR: GRIDGAIN_HOME variable must point to GridGain installation folder.
goto error_finish

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

::
:: Set GROOVY_LIBS for version 1.5.7
::
set GROOVY_LIBS=%GROOVY_HOME%\embeddable\groovy-all-1.5.7.jar

set CP=%GRIDGAIN_LIBS%;%GRIDGAIN_HOME%\%ANT_AUGMENTED_GGJAR%;%GROOVY_LIBS%

if "%1" == "" goto dfltCfg
    set CONFIG=%1
goto java
:dfltCfg
set CONFIG=%GRIDGAIN_HOME%\config\default-spring.xml

:java

::
:: This variable defines necessary parameters for JMX
:: monitoring and management.
:: ADD YOUR ADDITIONAL PARAMETERS/OPTIONS HERE
::
set JMX_MON=-Dcom.sun.management.jmxremote

::
:: Uncomment this to enable remote unsecure access
:: to JConsole.
:: ADD YOUR ADDITIONAL PARAMETERS/OPTIONS HERE
::
:: set JMX_MON=%JMX_MON% -Dcom.sun.management.jmxremote.port=49112 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false

::
:: JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp
:: for more details. Note that default settings use parallel GC.
:: ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
::
set JVM_OPTS=-ea -XX:MaxPermSize=128m -XX:+UseParNewGC -XX:MaxNewSize=32m -XX:NewSize=32m -Xms256m -Xmx256m -XX:SurvivorRatio=128 -XX:MaxTenuringThreshold=0  -XX:+UseTLAB -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled

"%JAVA_HOME%\bin\java.exe" -version 2>&1 | findstr /I "1.5." > nul
if not errorlevel == 1 (set JVM_OPTS=%JVM_OPTS%  -XX:+CMSPermGenSweepingEnabled)

::
:: Set program name.
::
set PROG_NAME=gridgain.bat
if "%OS%" == "Windows_NT" set PROG_NAME=%~nx0%

::
:: Remote debugging (JPDA).
:: Uncomment and change if remote debugging is required.
:: set JVM_OPTS=-Xdebug -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=n %JVM_OPTS%
::

"%JAVA_HOME%\bin\java.exe" %JVM_OPTS% %JMX_MON% -DGRIDGAIN_HOME="%GRIDGAIN_HOME%" -DGRIDGAIN_PROG_NAME="%PROG_NAME%" -cp "%CP%;." org.gridgain.grid.groovy.examples.helloworld.api.GridHelloWorldGroovyExample "%CONFIG%"

:error_finish

pause
