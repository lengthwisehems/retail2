@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ---------- Python from Site Extension ----------
set "PY=D:\home\python3111x64\python.exe"
if not exist "%PY%" (
    echo [ERROR] Python not found at "%PY%". Ensure App Service is 64-bit and Python 3.11 x64 Site Extension is installed.
    exit /b 9009
)
for %%D in ("%PY%") do set "PY_DIR=%%~dpD"
set "PATH=%PY_DIR%;%PY_DIR%Scripts;%PATH%"

REM ---------- Persistent folders ----------
set "ROOT=%~dp0"
set "OUT=%HOME%\data\outputs"
set "LOGS=%HOME%\data\logs"
if not exist "%OUT%"  mkdir "%OUT%"
if not exist "%LOGS%" mkdir "%LOGS%"

set "FINAL_EXIT=0"
set "SIZE_THRESHOLD=102400"

echo Ensuring required Python packages...
"%PY%" -m pip install --disable-pip-version-check -q --upgrade pip
if errorlevel 1 echo [WARN] Pip upgrade reported an error; continuing with existing pip.
"%PY%" -m pip install --disable-pip-version-check -q requests openpyxl beautifulsoup4 lxml html5lib
if errorlevel 1 (
    echo [WARN] Package installation reported an error; scrapers may fail if dependencies are missing.
) else (
    echo Packages verified.
)
"%PY%" -V

REM ---------- Run each scraper ----------
call :RunScraper "ABrand"      "%ROOT%ABrand\abrand_inventory.py"                   "%OUT%\ABrand\Output"           "%LOGS%\ABrand\abrand_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "AGJeans"     "%ROOT%AGJeans\agjeans_inventory.py"                 "%OUT%\AGJeans\Output"          "%LOGS%\AGJeans\agjeans_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "AMO"         "%ROOT%AMO\amo_inventory.py"                         "%OUT%\AMO\Output"              "%LOGS%\AMO\amo_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "DL1961"      "%ROOT%DL1961\dl1961_inventory.py"                   "%OUT%\DL1961\Output"           "%LOGS%\DL1961\dl1961_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Edyson"      "%ROOT%Edyson\edyson_inventory.py"                  "%OUT%\Edyson\Output"           "%LOGS%\Edyson\edyson_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "GoodAmerican" "%ROOT%GoodAmerican\Goodamerican_inventory.py"      "%OUT%\GoodAmerican\Output"     "%LOGS%\GoodAmerican\ga_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Fidelity"    "%ROOT%Fidelity\fidelity_inventory.py"               "%OUT%\Fidelity\Output"         "%LOGS%\Fidelity\fidelity_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Frame"       "%ROOT%Frame\frame_inventory.py"                     "%OUT%\Frame\Output"            "%LOGS%\Frame\frame_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Haikure"     "%ROOT%Haikure\haikure_inventory.py"                 "%OUT%\Haikure\Output"          "%LOGS%\Haikure\haikure_inventory.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "IconDenim"   "%ROOT%Icon\icon_inventory.py"                       "%OUT%\IconDenim\Output"        "%LOGS%\IconDenim\icon_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "LAgence"     "%ROOT%LAgence\lagence_inventory.py"                 "%OUT%\LAgence\Output"          "%LOGS%\LAgence\lagence_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "MotherDenim" "%ROOT%MotherDenim\motherdenim_inventory.py"         "%OUT%\MotherDenim\Output"      "%LOGS%\MotherDenim\motherdenim_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Neuw"        "%ROOT%Neuw\neuw_inventory.py"                       "%OUT%\Neuw\Output"             "%LOGS%\Neuw\neuw_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Paige"       "%ROOT%Paige\paige_inventory.py"                     "%OUT%\Paige\Output"            "%LOGS%\Paige\paige_inventory.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Pistola"     "%ROOT%Pistola\pistola_inventory.py"                 "%OUT%\Pistola\Output"          "%LOGS%\Pistola\pistola_inventory.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "RamyBrook"   "%ROOT%RamyBrook\ramybrook_pants_inventory.py"       "%OUT%\RamyBrook\Output"        "%LOGS%\RamyBrook\ramybrook_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "ReDone"      "%ROOT%ReDone\redone_inventory.py"                   "%OUT%\ReDone\Output"           "%LOGS%\ReDone\redone_inventory.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Rollas"      "%ROOT%Rollas\rollas_inventory.py"                   "%OUT%\Rollas\Output"           "%LOGS%\Rollas\rollas_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Rudes"       "%ROOT%Rudes\rudes_inventory.py"                     "%OUT%\Rudes\Output"            "%LOGS%\Rudes\rudes_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Selfcontrast" "%ROOT%Selfcontrast\selfcontrast_inventory.py"      "%OUT%\Selfcontrast\Output"     "%LOGS%\Selfcontrast\selfcontrast_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Staud"       "%ROOT%Staud\staud_inventory.py"                     "%OUT%\Staud\Output"            "%LOGS%\Staud\staud_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Triarchy"    "%ROOT%Triarchy\triarchy_inventory.py"                "%OUT%\Triarchy\Output"       "%LOGS%\Triarchy\triarchy_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Warpweft"    "%ROOT%Warpweft\warpweft_inventory.py"               "%OUT%\Warpweft\Output"         "%LOGS%\Warpweft\warpweft_run.log"
call :UpdateFinalExit %ERRORLEVEL%

REM ---------- Upload CSV files to Azure Blob Storage ----------
echo Copying files to Azure Storage (Managed Identity)...
set "LOCALAPPDATA=%HOME%\data\azcopy"
if not exist "%LOCALAPPDATA%" mkdir "%LOCALAPPDATA%"
set "AZCOPY_LOG_LOCATION=%LOCALAPPDATA%\logs"
if not exist "%AZCOPY_LOG_LOCATION%" mkdir "%AZCOPY_LOG_LOCATION%"
set "AZCOPY_JOB_PLAN_LOCATION=%LOCALAPPDATA%\plans"
if not exist "%AZCOPY_JOB_PLAN_LOCATION%" mkdir "%AZCOPY_JOB_PLAN_LOCATION%"
set "AZCOPY_DISABLE_STRICT_ENCRYPTION=true"

azcopy login --identity
if errorlevel 1 (
    echo [WARN] azcopy login failed while using Managed Identity. Upload skipped.
) else (
    set "HAS_CSV=0"
    for /f %%G in ('dir /b /s "%OUT%\*.csv" 2^>nul') do (
        set "HAS_CSV=1"
        goto :AfterScan
    )
:AfterScan
    if "!HAS_CSV!"=="1" (
        azcopy copy "%OUT%" "https://lengthwisescraperstorage.blob.core.windows.net/scraperoutput" --recursive --from-to=LocalBlob --include-pattern="*.csv" --overwrite=ifSourceNewer
        if errorlevel 1 (
            echo [WARN] azcopy copy failed ^(non-fatal^). Review AzCopy logs under "%AZCOPY_LOG_LOCATION%".
        ) else (
            echo AzCopy completed.
        )
    ) else (
        echo [INFO] No CSV files found to upload.
    )
)

echo [INFO] Output directory snapshot:
if exist "%OUT%" (
    for /d %%B in ("%OUT%\*") do (
        echo   %%~fB
        if exist "%%~fB\Output" (
            for /f "delims=" %%F in ('dir /b "%%~fB\Output" 2^>nul') do (
                echo       %%F
            )
        )
    )
) else (
    echo   %OUT% does not exist.
)

call :ReturnFinalExit

REM ---------------------------------------------------------------------------
REM  Subroutine that runs one scraper, harvests CSVs, and logs the outcome
REM    %1 - friendly name
REM    %2 - Python script (full path)
REM    %3 - destination output directory under %OUT%
REM    %4 - log file path under %LOGS%
REM ---------------------------------------------------------------------------
:RunScraper
setlocal EnableExtensions EnableDelayedExpansion
set "SCRAPER_NAME=%~1"
set "SCRIPT=%~2"
set "OUTDIR=%~3"
set "LOGFILE=%~4"

set "SCRAPER_START=%DATE% %TIME%"
set "PYTHON_EXIT=0"
set "SCRAPER_EXIT=0"
set "FAIL_REASON="
set "CSV_COUNT=0"
set "CSV_OVER_THRESHOLD=0"
set "MAX_CSV_SIZE=0"
set "MIN_CSV_SIZE="
set "FIRST_CSV="
set "ROBO_EXIT=0"

if not exist "!OUTDIR!" mkdir "!OUTDIR!"
for %%L in ("!LOGFILE!") do if not exist "%%~dpL" mkdir "%%~dpL"

for %%S in ("!SCRIPT!") do set "SCRIPT_DIR=%%~dpS"
set "LEGACY_SOURCE=!SCRIPT_DIR!Output"

echo ===== !SCRAPER_START! START - !SCRAPER_NAME! =====>>"!LOGFILE!"
"%PY%" "!SCRIPT!" >>"!LOGFILE!" 2>&1
set "PYTHON_EXIT=%ERRORLEVEL%"
set "SCRAPER_EXIT=!PYTHON_EXIT!"
if not "!PYTHON_EXIT!"=="0" (
    set "FAIL_REASON=Python exited with code !PYTHON_EXIT!."
)

if exist "!LEGACY_SOURCE!" (
    robocopy "!LEGACY_SOURCE!" "!OUTDIR!" *.csv /E /XO /NFL /NDL >>"!LOGFILE!" 2>&1
    set "ROBO_EXIT=%ERRORLEVEL%"
    if !ROBO_EXIT! geq 8 (
        echo [WARN] Robocopy reported an error (exit !ROBO_EXIT!) while harvesting !SCRAPER_NAME! CSVs from "!LEGACY_SOURCE!".
    ) else if !ROBO_EXIT! gtr 0 (
        echo [INFO] Harvested CSV updates for !SCRAPER_NAME! from "!LEGACY_SOURCE!" (exit !ROBO_EXIT!).
    )
) else (
    echo [INFO] No legacy CSV directory to harvest for !SCRAPER_NAME!.
)

for /f "delims=" %%C in ('dir /b "!OUTDIR!\*.csv" 2^>nul') do (
    set /a CSV_COUNT+=1
    if not defined FIRST_CSV set "FIRST_CSV=%%C"
    for %%S in ("!OUTDIR!\%%C") do (
        set "CURRENT_SIZE=%%~zS"
    )
    if defined CURRENT_SIZE (
        if !CURRENT_SIZE! gtr !MAX_CSV_SIZE! set "MAX_CSV_SIZE=!CURRENT_SIZE!"
        if not defined MIN_CSV_SIZE (
            set "MIN_CSV_SIZE=!CURRENT_SIZE!"
        ) else if !CURRENT_SIZE! lss !MIN_CSV_SIZE! set "MIN_CSV_SIZE=!CURRENT_SIZE!"
        if !CURRENT_SIZE! geq !SIZE_THRESHOLD! set /a CSV_OVER_THRESHOLD+=1
    )
)

if defined FIRST_CSV (
    set "CSV_SAMPLE_DESC= (e.g., !FIRST_CSV!)"
) else (
    set "CSV_SAMPLE_DESC="
)

if !CSV_COUNT! gtr 0 (
    if !CSV_OVER_THRESHOLD! gtr 0 (
        if !SCRAPER_EXIT! equ 0 (
            rem success path handled later
        ) else (
            if defined FAIL_REASON (
                set "FAIL_REASON=!FAIL_REASON! CSVs were generated (largest !MAX_CSV_SIZE! bytes!CSV_SAMPLE_DESC!) but the process returned exit code !SCRAPER_EXIT!."
            ) else (
                set "FAIL_REASON=CSVs were generated (largest !MAX_CSV_SIZE! bytes!CSV_SAMPLE_DESC!) but the process returned exit code !SCRAPER_EXIT!."
            )
        )
    ) else (
        if !SCRAPER_EXIT! equ 0 set "SCRAPER_EXIT=3"
        if defined FAIL_REASON (
            set "FAIL_REASON=!FAIL_REASON! Largest CSV is !MAX_CSV_SIZE! bytes!CSV_SAMPLE_DESC!, below the 100KB threshold."
        ) else (
            set "FAIL_REASON=Largest CSV under !OUTDIR! is !MAX_CSV_SIZE! bytes!CSV_SAMPLE_DESC!, below the 100KB threshold (!SIZE_THRESHOLD! bytes)."
        )
    )
) else (
    if !SCRAPER_EXIT! equ 0 (
        set "SCRAPER_EXIT=2"
        set "FAIL_REASON=No CSV files were copied into !OUTDIR! despite a zero exit code."
    ) else if not defined FAIL_REASON (
        set "FAIL_REASON=Python exited with code !SCRAPER_EXIT! and no CSV files were copied into !OUTDIR!."
    )
)

set "SCRAPER_END=%DATE% %TIME%"

if !SCRAPER_EXIT! equ 0 (
    echo ===== !SCRAPER_END! DONE - !SCRAPER_NAME! =====>>"!LOGFILE!"
    echo [OK] !SCRAPER_NAME! generated !CSV_COUNT! CSV^(s^) under !OUTDIR! (largest !MAX_CSV_SIZE! bytes!CSV_SAMPLE_DESC!).
) else (
    echo ===== !SCRAPER_END! FAIL - !SCRAPER_NAME! exit !SCRAPER_EXIT! =====>>"!LOGFILE!"
    if not defined FAIL_REASON set "FAIL_REASON=Unknown failure; review the brand log for details."
    echo [ERROR] !SCRAPER_NAME! failed: !FAIL_REASON!
    if exist "!LOGFILE!" (
        echo [INFO] Review log for details: !LOGFILE!
        powershell -NoProfile -Command "Write-Output '----- Last 20 log lines for !SCRAPER_NAME! -----'; Get-Content -LiteralPath '!LOGFILE!' -Tail 20" 2>nul
    ) else (
        echo [WARN] Log file could not be created at !LOGFILE!.
    )
)

echo End: !SCRAPER_END! exit !SCRAPER_EXIT!>>"!LOGFILE!"
echo.>>"!LOGFILE!"

endlocal & exit /b %SCRAPER_EXIT%

REM ---------------------------------------------------------------------------
REM  Capture the first non-zero exit code so Task Scheduler can retry
REM ---------------------------------------------------------------------------
:UpdateFinalExit
if not "%~1"=="0" if "%FINAL_EXIT%"=="0" set "FINAL_EXIT=%~1"
exit /b 0

:ReturnFinalExit
endlocal & exit /b %FINAL_EXIT%
