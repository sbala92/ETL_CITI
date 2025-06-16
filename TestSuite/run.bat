@echo off
setlocal enabledelayedexpansion

REM Set timestamp for log and result filenames
for /f "tokens=1-4 delims=/ " %%a in ("%date%") do (
    set month=%%a
    set day=%%b
    set year=%%c
)
for /f "tokens=1-2 delims=:." %%a in ("%time%") do (
    set hour=%%a
    set minute=%%b
)
set timestamp=%day%-%month%-%year%_%hour%-%minute%

REM Define paths for logs and results
set log_path=C:\Users\suren\PycharmProjects\ETLAutomation_CITI\Logs\Combined_log_%timestamp%.log
set result_path=C:\Users\suren\PycharmProjects\ETLAutomation_CITI\TestResults\DomainCheckResult_%timestamp%.xlsx

REM Ensure TestResults directory exists
if not exist "C:\Users\suren\PycharmProjects\ETLAutomation_CITI\TestResults" mkdir "C:\Users\suren\PycharmProjects\ETLAutomation_CITI\TestResults"

REM Initialize variables
set files=
set columns=

REM Extract arguments dynamically
set arg_count=0
set columns_found=0

for %%a in (%*) do (
    if "!columns_found!"=="0" (
        if exist "C:\Users\suren\PycharmProjects\ETLAutomation_CITI\ConfigFiles_Scenarios\%%a" (
            REM If argument is a valid file, treat it as a file
            if "!files!"=="" (
                set files=%%a
            ) else (
                set files=!files!,%%a
            )
        ) else (
            REM If argument is NOT a valid file, treat it as columns
            set columns=%%a
            set columns_found=1
        )
    )
)

REM Scenario 1: No files and no columns provided - Process all JSON files dynamically
if "%files%"=="" (
    echo Running DomainCheck for all JSON files and columns...

    REM Correctly collect all JSON files into the `files` variable
    set files=
    for %%f in (C:\Users\suren\PycharmProjects\ETLAutomation_CITI\ConfigFiles_Scenarios\*.json) do (
        if not "!files!"=="" (
            set files=!files!,%%f
        ) else (
            set files=%%f
        )
    )

    REM Debugging Output
    echo Final detected files: %files%

    REM If files are still empty, exit
    if "%files%"=="" (
        echo No JSON files found in ConfigFiles_Scenarios! Exiting.
        goto end
    )

    python Domaincheck1.py --files "%files%" --logfile "%log_path%" --resultfile "%result_path%"
    goto end
)

REM Scenario 2: Files provided with no columns - Process specified files with all columns
if "%columns%"=="" (
    echo Running DomainCheck for specified files with all columns...
    python Domaincheck1.py --files "%files%" --logfile "%log_path%" --resultfile "%result_path%"
    goto end
)

REM Scenario 3: Files and columns provided - Process specified files with specific columns
echo Running DomainCheck for specified files with specific columns...
python Domaincheck1.py --files "%files%" --columns "%columns%" --logfile "%log_path%" --resultfile "%result_path%"

:end
echo Script execution complete.
pause