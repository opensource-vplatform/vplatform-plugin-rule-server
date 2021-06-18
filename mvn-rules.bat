@echo off
rem chcp 65001 == utf-8字符集
chcp 65001 
set setting=E:\works\toone-maven-settings.xml
echo ==================maven 仓库设置(%setting%)====================
rd /s /Q target
mkdir target 

echo =================批量打包=================
for %%i in (
    Serverrule_EntityRecordRecycling
	Serverrule_DeleteConditionRelationData
) do (
	echo "mvn clean package -f %%i\pom.xml --settings %setting%" 
	TIMEOUT /T 1
	call mvn clean package -f %%i\pom.xml --settings %setting% >mvn%%i.log
    call copy %%i\target\*.jar target\
)
   
del target\*-javadoc.jar
del target\*-sources.jar
pause
