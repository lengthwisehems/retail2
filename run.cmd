@echo off
setlocal EnableExtensions

REM ---------- Python from Site Extension ----------
set "PY=D:\home\python311x64\python.exe"
set "PATH=D:\home\python311x64;D:\home\python311x64\Scripts;%PATH%"

REM ---------- Folders ----------
set "ROOT=%~dp0"
set "OUT=%HOME%\data\outputs\"
set "LOGS=%HOME%\data\logs\"

if not exist "%OUT%"  mkdir "%OUT%"
if not exist "%LOGS%" mkdir "%LOGS%"

set "FINAL_EXIT=0"

REM ---------- Ensure required Python packages are installed ----------
echo Ensuring required Python packages...
"%PY%" -m pip install --disable-pip-version-check -q --upgrade pip
"%PY%" -m pip install --disable-pip-version-check -q requests openpyxl
echo Packages verified.


REM ---------- Example brand calls (edit ONLY the brand names below) ----------
call :RunScraper "ABrand" ^
                  "%ROOT%ABrand\abrand_inventory.py" ^
                  "%OUT%ABrand\Output" ^
                  "%LOGS%abrand_run.log"
call :UpdateFinalExit

call :RunScraper "AGJeans" ^
                  "%ROOT%AGJeans\agjeans_inventory.py" ^
                  "%OUT%AGJeans\Output" ^
                  "%LOGS%agjeans_run.log"
call :UpdateFinalExit


call :RunScraper "AMO" ^
                  "%ROOT%AMO\amo_inventory.py" ^
                  "%OUT%AMO\Output" ^
                  "%LOGS%AMO\Output\amo_run.log"
call :UpdateFinalExit

call :RunScraper "DL1961" ^
                  "%ROOT%DL1961\dl1961_inventory.py" ^
                  "%OUT%DL1961\Output" ^
                  "%LOGS%DL1961\dl1961_run.log"
call :UpdateFinalExit

call :RunScraper "Edyson" ^
                  "%ROOT%Edyson\edyson_products_all_to_layout.py" ^
                  "%OUT%Edyson\Output" ^
                  "%LOGS%Edyson\edyson_all_layout.log"
call :UpdateFinalExit

call :RunScraper "GoodAmerican" ^
                  "%ROOT%GoodAmerican\Goodamerican_inventory.py" ^
                  "%OUT%GoodAmerican\Output" ^
                  "%LOGS%GoodAmerican\ga_run.log"
call :UpdateFinalExit

call :RunScraper "Fidelity" ^
                  "%ROOT%Fidelity\fidelity_inventory.py" ^
                  "%OUT%Fidelity\Output" ^
                  "%LOGS%Fidelity\fidelity_run.log"
call :UpdateFinalExit

call :RunScraper "Frame" ^
                  "%ROOT%Frame\frame_inventory.py" ^
                  "%OUT%Frame\Output" ^
                  "%LOGS%Frame\frame_run.log"
call :UpdateFinalExit

call :RunScraper "Haikure" ^
                  "%ROOT%Haikure\haikure_inventory.py" ^
                  "%OUT%Haikure\Output" ^
                  "%LOGS%Haikure\haikure_inventory.log"
call :UpdateFinalExit

call :RunScraper "IconDenim" ^
                  "%ROOT%Icon\icon_inventory.py" ^
                  "%OUT%Icon\Output" ^
                  "%LOGS%Icon\icon_run.log"
call :UpdateFinalExit

call :RunScraper "LAgence" ^
                  "%ROOT%LAgence\lagence_inventory.py" ^
                  "%OUT%LAgence\Output" ^
                  "%LOGS%LAgence\lagence_run.log"
call :UpdateFinalExit

call :RunScraper "MotherDenim" ^
                  "%ROOT%MotherDenim\Motherdenim_inventory.py" ^
                  "%OUT%MotherDenim\Output" ^
                  "%LOGS%MotherDenim\Output\motherdenim_run.log"
call :UpdateFinalExit

call :RunScraper "Neuw" ^
                  "%ROOT%Neuw\neuw_inventory.py" ^
                  "%OUT%Neuw\Output" ^
                  "%LOGS%Neuw\neuw_run.log"
call :UpdateFinalExit

call :RunScraper "Paige" ^
                  "%ROOT%Paige\paige_inventory.py" ^
                  "%OUT%Paige\Output" ^
                  "%LOGS%Paige\paige_inventory.log"
call :UpdateFinalExit

call :RunScraper "Pistola" ^
                  "%ROOT%Pistola\pistola_inventory.py" ^
                  "%OUT%Pistola\Output" ^
                  "%LOGS%Pistola\pistola_inventory.log"
call :UpdateFinalExit

call :RunScraper "RamyBrook" ^
                  "%ROOT%RamyBrook\ramybrook_pants_inventory.py" ^
                  "%OUT%RamyBrook\Output" ^
                  "%LOGS%RamyBrook\Output\ramybrook_run.log"
call :UpdateFinalExit

call :RunScraper "ReDone" ^
                  "%ROOT%ReDone\redone_inventory.py" ^
                  "%OUT%ReDone\Output" ^
                  "%LOGS%ReDone\shopredone_inventory.log"
call :UpdateFinalExit

call :RunScraper "Rollas" ^
                  "%ROOT%Rollas\rollas_inventory.py" ^
                  "%OUT%Rollas\Output" ^
                  "%LOGS%Rollas\rollas_run.log"
call :UpdateFinalExit

call :RunScraper "Rudes" ^
                  "%ROOT%Rudes\rudes_inventory.py" ^
                  "%OUT%Rudes\Output" ^
                  "%LOGS%Rudes\Logs\rudes_run_+.log"
call :UpdateFinalExit

call :RunScraper "Selfcontrast" ^
                  "%ROOT%Selfcontrast\selfcontrast_inventory.py" ^
                  "%OUT%Selfcontrast\Output" ^
                  "%LOGS%Selfcontrast\selfcontrast_run.log"
call :UpdateFinalExit

call :RunScraper "Staud" ^
                  "%ROOT%Staud\staud_inventory.py" ^
                  "%OUT%Staud\Output" ^
                  "%LOGS%Staud\Output\staud_run.log"
call :UpdateFinalExit

call :RunScraper "Triarchy" ^
                  "%ROOT%Triarchy\triarchy_inline_inventory_to_layout.py" ^
                  "%OUT%Triarchy\Output" ^
                  "%LOGS%Triarchy\triarchy_inline_inventory.log"
call :UpdateFinalExit

call :RunScraper "Warpweft" ^
                  "%ROOT%Warpweft\warpweft_inventory.py" ^
                  "%OUT%Warpweft\Output" ^
                  "%LOGS%Warpweft\warpweft_run.log"
call :UpdateFinalExit

REM ===== After all scrapers have finished, copy CSVs out =====
echo Copying files to Azure Storage (Managed Identity)...
set "AZCOPY_LOG_LOCATION=%LOCALAPPDATA%\AzCopy"
if not exist "%AZCOPY_LOG_LOCATION%" mkdir "%AZCOPY_LOG_LOCATION%"

azcopy login --identity
azcopy copy "%OUT%" "https://lengthwisescraperstorage.blob.core.windows.net/scraperoutput" ^
  --recursive=true --include-pattern="*.csv"

if errorlevel 1 (
  echo [WARN] azcopy copy failed (non-fatal)
) else (
  echo AzCopy completed.
)


endlocal & exit /b %FINAL_EXIT%

REM ---------------------------------------------------------------------------
REM  Subroutine that runs one scraper and logs the outcome
REM    %1 - friendly name
REM    %2 - Python script
REM    %3 - output directory
REM    %4 - log file path
REM ---------------------------------------------------------------------------
:RunScraper
set "SCRAPER_NAME=%~1"
set "SCRIPT=%~2"
set "OUTDIR=%~3"
set "LOG=%~4"

set "SCRAPER_START=%date% %time%"

echo Running %SCRAPER_NAME%...

if not exist "%OUTDIR%" mkdir "%OUTDIR%"

echo ===== %SCRAPER_START% START (%SCRAPER_NAME%) =====>>"%LOG%"

REM Run the scraper
"%PY%" "%SCRIPT%" >>"%LOG%" 2>&1

set "SCRAPER_EXIT=%ERRORLEVEL%"
set "SCRAPER_END=%date% %time%"

if not "%SCRAPER_EXIT%"=="0" (
    echo ===== %SCRAPER_END% FAIL (%SCRAPER_NAME%, exit %SCRAPER_EXIT%) =====>>"%LOG%"
    echo %SCRAPER_NAME% failed with exit code %SCRAPER_EXIT%.
) else (
    echo ===== %SCRAPER_END% DONE (%SCRAPER_NAME%) =====>>"%LOG%"
    echo %SCRAPER_NAME% completed successfully.
)

exit /b %SCRAPER_EXIT%

REM ---------------------------------------------------------------------------
REM  Capture the first non-zero exit code so Task Scheduler can retry
REM ---------------------------------------------------------------------------
:UpdateFinalExit
if not "%ERRORLEVEL%"=="0" (
    if "%FINAL_EXIT%"=="0" (
        set "FINAL_EXIT=%ERRORLEVEL%"
    )
)
exit /b 0