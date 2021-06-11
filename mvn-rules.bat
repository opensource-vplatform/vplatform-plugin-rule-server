@echo off
echo ====================����jar�����ֿ�====================
rd /s /Q target
mkdir target 

echo =================�������=================
set setting=E:\works\toone-maven-settings.xml
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
	echo "����:===%%i ===" 
	call mvn clean package -f %%i\pom.xml --settings %setting%
    call copy %%i\target\*.jar target\
)
   
del target\*-javadoc.jar
del target\*-sources.jar
pause
