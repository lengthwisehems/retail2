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

REM ---------- Folders ----------
set "ROOT=%~dp0"
set "OUT=%HOME%\data\outputs"
set "LOGS=%HOME%\data\logs"
if not exist "%OUT%"  mkdir "%OUT%"
if not exist "%LOGS%" mkdir "%LOGS%"

set "FINAL_EXIT=0"
set "MIN_CSV_SIZE=102400"

echo Ensuring required Python packages...
"%PY%" -m pip install --disable-pip-version-check -q --upgrade pip
"%PY%" -m pip install --disable-pip-version-check -q requests openpyxl
echo Packages verified.

REM ---------- Run each scraper ----------
call :RunScraper "ABrand" ^
                  "%ROOT%ABrand\abrand_inventory.py" ^
                  "%OUT%ABrand\Output" ^
                  "%LOGS%abrand_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "AGJeans" ^
                  "%ROOT%AGJeans\agjeans_inventory.py" ^
                  "%OUT%AGJeans\Output" ^
                  "%LOGS%agjeans_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "AMO" ^
                  "%ROOT%AMO\amo_inventory.py" ^
                  "%OUT%AMO\Output" ^
                  "%LOGS%AMO\Output\amo_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "DL1961" ^
                  "%ROOT%DL1961\dl1961_inventory.py" ^
                  "%OUT%DL1961\Output" ^
                  "%LOGS%DL1961\dl1961_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Edyson" ^
                  "%ROOT%Edyson\edyson_products_all_to_layout.py" ^
                  "%OUT%Edyson\Output" ^
                  "%LOGS%Edyson\edyson_all_layout.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "GoodAmerican" ^
                  "%ROOT%GoodAmerican\Goodamerican_inventory.py" ^
                  "%OUT%GoodAmerican\Output" ^
                  "%LOGS%GoodAmerican\ga_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Fidelity" ^
                  "%ROOT%Fidelity\fidelity_inventory.py" ^
                  "%OUT%Fidelity\Output" ^
                  "%LOGS%Fidelity\fidelity_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Frame" ^
                  "%ROOT%Frame\frame_inventory.py" ^
                  "%OUT%Frame\Output" ^
                  "%LOGS%Frame\frame_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Haikure" ^
                  "%ROOT%Haikure\haikure_inventory.py" ^
                  "%OUT%Haikure\Output" ^
                  "%LOGS%Haikure\haikure_inventory.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "IconDenim" ^
                  "%ROOT%Icon\icon_inventory.py" ^
                  "%OUT%Icon\Output" ^
                  "%LOGS%Icon\icon_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "LAgence" ^
                  "%ROOT%LAgence\lagence_inventory.py" ^
                  "%OUT%LAgence\Output" ^
                  "%LOGS%LAgence\lagence_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "MotherDenim" ^
                  "%ROOT%MotherDenim\Motherdenim_inventory.py" ^
                  "%OUT%MotherDenim\Output" ^
                  "%LOGS%MotherDenim\Output\motherdenim_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Neuw" ^
                  "%ROOT%Neuw\neuw_inventory.py" ^
                  "%OUT%Neuw\Output" ^
                  "%LOGS%Neuw\neuw_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Paige" ^
                  "%ROOT%Paige\paige_inventory.py" ^
                  "%OUT%Paige\Output" ^
                  "%LOGS%Paige\paige_inventory.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Pistola" ^
                  "%ROOT%Pistola\pistola_inventory.py" ^
                  "%OUT%Pistola\Output" ^
                  "%LOGS%Pistola\pistola_inventory.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "RamyBrook" ^
                  "%ROOT%RamyBrook\ramybrook_pants_inventory.py" ^
                  "%OUT%RamyBrook\Output" ^
                  "%LOGS%RamyBrook\Output\ramybrook_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "ReDone" ^
                  "%ROOT%ReDone\redone_inventory.py" ^
                  "%OUT%ReDone\Output" ^
                  "%LOGS%ReDone\shopredone_inventory.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Rollas" ^
                  "%ROOT%Rollas\rollas_inventory.py" ^
                  "%OUT%Rollas\Output" ^
                  "%LOGS%Rollas\rollas_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Rudes" ^
                  "%ROOT%Rudes\rudes_inventory.py" ^
                  "%OUT%Rudes\Output" ^
                  "%LOGS%Rudes\Logs\rudes_run_+.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Selfcontrast" ^
                  "%ROOT%Selfcontrast\selfcontrast_inventory.py" ^
                  "%OUT%Selfcontrast\Output" ^
                  "%LOGS%Selfcontrast\selfcontrast_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Staud" ^
                  "%ROOT%Staud\staud_inventory.py" ^
                  "%OUT%Staud\Output" ^
                  "%LOGS%Staud\Output\staud_run.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Triarchy" ^
                  "%ROOT%Triarchy\triarchy_inline_inventory_to_layout.py" ^
                  "%OUT%Triarchy\Output" ^
                  "%LOGS%Triarchy\triarchy_inline_inventory.log"
call :UpdateFinalExit %ERRORLEVEL%

call :RunScraper "Warpweft" ^
                  "%ROOT%Warpweft\warpweft_inventory.py" ^
                  "%OUT%Warpweft\Output" ^
                  "%LOGS%Warpweft\warpweft_run.log"
call :UpdateFinalExit %ERRORLEVEL%

REM ---------- Upload CSV files to Azure Blob Storage ----------
echo Copying files to Azure Storage (Managed Identity)...
rem ---- AzCopy cache in HOME and disable strict encryption (App Service) ----
set "LOCALAPPDATA=%HOME%\data\azcopy"
if not exist "%LOCALAPPDATA%" mkdir "%LOCALAPPDATA%"
rem AzCopy expects these subfolders; pre-create them
set "AZCOPY_LOG_LOCATION=%LOCALAPPDATA%\logs"
if not exist "%AZCOPY_LOG_LOCATION%" mkdir "%AZCOPY_LOG_LOCATION%"
set "AZCOPY_JOB_PLAN_LOCATION=%LOCALAPPDATA%\plans"
if not exist "%AZCOPY_JOB_PLAN_LOCATION%" mkdir "%AZCOPY_JOB_PLAN_LOCATION%"
rem Allow plaintext token cache (safe on ephemeral App Service sandbox)
set "AZCOPY_DISABLE_STRICT_ENCRYPTION=true"

azcopy login --identity
if errorlevel 1 (
    echo [WARN] azcopy login failed while using Managed Identity. Upload skipped.
) else (
    if exist "%OUT%\*.csv" (
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

call :ReturnFinalExit

REM ---------------------------------------------------------------------------
REM  Subroutine that runs one scraper and logs the outcome
REM    %1 - friendly name
REM    %2 - Python script
REM    %3 - output directory
REM    %4 - log file path
REM ---------------------------------------------------------------------------
:RunScraper
setlocal EnableExtensions EnableDelayedExpansion
set "SCRAPER_NAME=%~1"
set "SCRIPT=%~2"
set "OUTDIR=%~3"
set "LOGFILE=%~4"

set "SCRAPER_START=%DATE% %TIME%"
set "SCRAPER_EXIT=0"
set "PYTHON_EXIT=0"
set "LATEST_CSV="
set "LATEST_SIZE=0"
set "FAIL_REASON="

if not exist "%OUTDIR%" mkdir "%OUTDIR%"
for %%L in ("%LOGFILE%") do if not exist "%%~dpL" mkdir "%%~dpL"

echo ===== !SCRAPER_START! START - !SCRAPER_NAME! =====>>"%LOGFILE%"
"%PY%" "%SCRIPT%" >>"%LOGFILE%" 2>&1
set "PYTHON_EXIT=%ERRORLEVEL%"
set "SCRAPER_EXIT=!PYTHON_EXIT!"

set "SCRAPER_END=%DATE% %TIME%"

if !PYTHON_EXIT! equ 0 (
    if exist "%OUTDIR%\*.csv" (
        pushd "%OUTDIR%" >nul 2>&1
        for /f "delims=" %%F in ('dir /b /a:-d /o:-d *.csv 2^>nul') do (
            if not defined LATEST_CSV (
                set "LATEST_CSV=%OUTDIR%\%%F"
                for %%S in ("%OUTDIR%\%%F") do set "LATEST_SIZE=%%~zS"
            )
        )
        popd >nul 2>&1
    )
    if not defined LATEST_CSV (
        set "SCRAPER_EXIT=2"
        set "FAIL_REASON=No CSV files were created in !OUTDIR!."
    ) else if !LATEST_SIZE! lss !MIN_CSV_SIZE! (
        set "SCRAPER_EXIT=3"
        set "FAIL_REASON=Latest CSV !LATEST_CSV! is only !LATEST_SIZE! bytes; requires at least !MIN_CSV_SIZE! bytes."
    )
) else (
    set "FAIL_REASON=Python exited with code !PYTHON_EXIT!; inspect the log for stack trace or network errors."
)

if !SCRAPER_EXIT! equ 0 (
    echo ===== !SCRAPER_END! DONE - !SCRAPER_NAME! =====>>"%LOGFILE%"
    echo [OK] !SCRAPER_NAME! completed successfully - CSV: !LATEST_CSV!, size !LATEST_SIZE! bytes
) else (
    echo ===== !SCRAPER_END! FAIL - !SCRAPER_NAME! exit !SCRAPER_EXIT! =====>>"%LOGFILE%"
    if not defined FAIL_REASON set "FAIL_REASON=Unknown failure; review the brand log for details."
    echo [ERROR] !SCRAPER_NAME! failed: !FAIL_REASON!
    if exist "%LOGFILE%" (
        echo [INFO] Review log for details: %LOGFILE%
        powershell -NoProfile -Command "Write-Output '----- Last 20 log lines for !SCRAPER_NAME! -----'; Get-Content -LiteralPath '%LOGFILE%' -Tail 20" 2>nul
    ) else (
        echo [WARN] Log file could not be created at %LOGFILE%.
    )
)

echo End: !SCRAPER_END! exit !SCRAPER_EXIT!>>"%LOGFILE%"
echo.>>"%LOGFILE%"

endlocal & exit /b %SCRAPER_EXIT%

REM ---------------------------------------------------------------------------
REM  Capture the first non-zero exit code so Task Scheduler can retry
REM ---------------------------------------------------------------------------
:UpdateFinalExit
if not "%~1"=="0" if "%FINAL_EXIT%"=="0" set "FINAL_EXIT=%~1"
exit /b 0

:ReturnFinalExit
endlocal & exit /b %FINAL_EXIT%
