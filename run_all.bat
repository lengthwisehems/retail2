@echo off
setlocal EnableExtensions

REM ---------------------------------------------------------------------------
REM  Python interpreter used for every scraper
REM ---------------------------------------------------------------------------
set "PY=C:\Users\carri\AppData\Local\Programs\Python\Python313\python.exe"

set "FINAL_EXIT=0"

REM ---------------------------------------------------------------------------
REM  Run scrapers in alphabetical order
REM    To add a new brand, copy one of the blocks below and adjust the
REM    friendly name plus the three paths (script, output folder, log file).
REM ---------------------------------------------------------------------------
call :RunScraper "ABrand" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\ABrand\abrand_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\ABrand\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\ABrand\abrand_run.log"
call :UpdateFinalExit

call :RunScraper "AGJeans" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\AGJeans\agjeans_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\AGJeans\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\AGJeans\agjeans_run.log"
call :UpdateFinalExit

call :RunScraper "AMO" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\AMO\amo_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\AMO\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\AMO\Output\amo_run.log"
call :UpdateFinalExit

call :RunScraper "AMO" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\AMO\amo_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\AMO\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\AMO\Output\amo_run.log"
call :UpdateFinalExit

call :RunScraper "Edyson" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Edyson\edyson_products_all_to_layout.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Edyson\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Edyson\edyson_all_layout.log"
call :UpdateFinalExit

call :RunScraper "Frame" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Frame\frame_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Frame\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Frame\frame_run.log"
call :UpdateFinalExit

call :RunScraper "GoodAmerican" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\GoodAmerican\Goodamerican_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\GoodAmerican\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\GoodAmerican\ga_run.log"
call :UpdateFinalExit

call :RunScraper "Haikure" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Haikure\haikure_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Haikure\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Haikure\haikure_inventory.log"
call :UpdateFinalExit

REM L'Agence denim scraper
call :RunScraper "LAgence" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\LAgence\lagence_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\LAgence\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\LAgence\lagence_run.log"
call :UpdateFinalExit

call :RunScraper "MotherDenim" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\MotherDenim\Motherdenim_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\MotherDenim\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\MotherDenim\Output\motherdenim_run.log"
call :UpdateFinalExit

call :RunScraper "Paige" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Paige\paige_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Paige\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Paige\paige_inventory.log"
call :UpdateFinalExit

call :RunScraper "Pistola" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Pistola\pistola_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Pistola\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Pistola\pistola_inventory.log"
call :UpdateFinalExit

call :RunScraper "RamyBrook" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\RamyBrook\ramybrook_pants_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\RamyBrook\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\RamyBrook\Output\ramybrook_run.log"
call :UpdateFinalExit

call :RunScraper "ReDone" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\ReDone\redone_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\ReDone\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\ReDone\shopredone_inventory.log"
call :UpdateFinalExit

call :RunScraper "Rollas" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Rollas\rollas_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Rollas\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Rollas\rollas_run.log"
call :UpdateFinalExit

call :RunScraper "Rudes" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Rudes\rudes_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Rudes\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Rudes\Logs\rudes_run_+.log"
call :UpdateFinalExit

call :RunScraper "Staud" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Staud\staud_inventory.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Staud\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Staud\Output\staud_run.log"
call :UpdateFinalExit

call :RunScraper "Triarchy" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Triarchy\triarchy_inline_inventory_to_layout.py" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Triarchy\Output" ^
                  "C:\Users\carri\OneDrive - Length Wise\data scraping\Triarchy\triarchy_inline_inventory.log"
call :UpdateFinalExit

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

REM Optional: make sure deps are present (quiet)
"%PY%" -m pip install --disable-pip-version-check -q requests openpyxl >>"%LOG%" 2>&1

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