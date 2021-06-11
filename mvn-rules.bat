@echo off
set setting=E:\works\toone-maven-settings.xml
echo ==================maven 仓库设置(%setting%)====================
rd /s /Q target
mkdir target 

echo =================批量打包=================
for %%i in (
    Serverrule_AbortLoop
    Serverrule_AbortRule
    Serverrule_AddTableRecord
    Serverrule_ClearEntityData
    Serverrule_ExceptionAbort
    Serverrule_ExecExpression
    Serverrule_SetEntityVarControlValue
    Serverrule_SetLoopVariant
) do (
	echo "处理:===%%i ===" 
	call mvn clean package -f %%i\pom.xml --settings %setting%
    call copy %%i\target\*.jar target\
)
   
del target\*-javadoc.jar
del target\*-sources.jar
pause
