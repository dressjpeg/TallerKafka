@echo off

set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot
set HADOOP_HOME=C:\hadoop
set PATH=%JAVA_HOME%\bin;C:\hadoop\bin;%PATH%

python consumer.py

pause