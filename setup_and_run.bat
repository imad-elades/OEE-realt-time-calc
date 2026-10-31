@echo off
setlocal enabledelayedexpansion

:: ===========================================================================
::  OEE Analytics Pipeline - Setup & Launch Script
::  Version : 1.2.0
:: ===========================================================================

echo [OEE] Demarrage du script d'installation...
echo.

:: ===========================================================================
::  COULEURS ANSI (Windows 10+)
:: ===========================================================================

set "C_RST="
set "C_BLD="
set "C_DIM="
set "C_GRN="
set "C_YLW="
set "C_RED="
set "C_CYN="
set "C_MAG="
set "C_WHT="

set "ICON_OK=[OK]"
set "ICON_KO=[FAIL]"
set "ICON_WN=[WARN]"
set "ICON_IN=[INFO]"
set "ICON_ST=[STEP]"

for /f "tokens=4-5 delims=[.] " %%a in ('ver') do set "winver=%%a"
if not defined winver set "winver=0"
if !winver! geq 10 (
    for /f %%a in ('echo prompt $E ^| cmd') do set "ESC=%%a"
    if defined ESC (
        set "C_RST=!ESC![0m"
        set "C_BLD=!ESC![1m"
        set "C_DIM=!ESC![2m"
        set "C_GRN=!ESC![32m"
        set "C_YLW=!ESC![33m"
        set "C_RED=!ESC![31m"
        set "C_CYN=!ESC![36m"
        set "C_MAG=!ESC![35m"
        set "C_WHT=!ESC![97m"
        set "ICON_OK=!C_GRN! ok !C_RST!"
        set "ICON_KO=!C_RED! fail !C_RST!"
        set "ICON_WN=!C_YLW! warn !C_RST!"
        set "ICON_IN=!C_CYN! info !C_RST!"
        set "ICON_ST=!C_MAG!  >>  !C_RST!"
    )
)

:: ===========================================================================
::  VARIABLES PROJET
:: ===========================================================================

set "PRJ_ROOT=%~dp0"
set "REQ_FILE=%PRJ_ROOT%requirements.txt"
set "HADOOP_DIR=%PRJ_ROOT%hadoop"
set "WINUTILS=%HADOOP_DIR%\bin\winutils.exe"
set "GENERATOR=%PRJ_ROOT%data\generate_data.py"
set "PIPELINE=%PRJ_ROOT%spark\oee_pipeline.py"
set "DASHBOARD=%PRJ_ROOT%powerbi\create_dashboard.py"
set "LIVE_DASH=%PRJ_ROOT%streaming\live_dashboard.py"
set "PRODUCER=%PRJ_ROOT%streaming\producer.py"

set "CHECK_PASS=0"
set "CHECK_FAIL=0"
set "CHECK_WARN=0"

:: ===========================================================================
::  FICHIER JOURNAL
:: ===========================================================================

set "LOG_DIR=%PRJ_ROOT%logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" 2>nul

set "DT=%DATE:/=-%_%TIME::=-%"
set "DT=%DT: =0%"
set "LOG_FILE=%LOG_DIR%\setup_%DT%.log"
echo. > "%LOG_FILE%" 2>nul

:: ===========================================================================
::  FONCTIONS
:: ===========================================================================

:log
    echo [%TIME%] %*>> "%LOG_FILE%"
    goto :eof

:print_banner
    echo.
    echo ===========================================================================
    echo   OEE Analytics Pipeline - Automated Setup
    echo ===========================================================================
    echo.
    goto :eof

:print_section
    echo.
    echo ---------------------------------------------------------------------------
    echo  %~1
    echo ---------------------------------------------------------------------------
    echo.
    call :log "=== %~1 ==="
    goto :eof

:print_result
    if "%~1"=="PASS" (
        echo     %ICON_OK% %~2
        call :log "[PASS] %~2"
        set /a CHECK_PASS+=1
    ) else if "%~1"=="FAIL" (
        echo     %ICON_KO% %~2
        call :log "[FAIL] %~2"
        set /a CHECK_FAIL+=1
    ) else if "%~1"=="WARN" (
        echo     %ICON_WN% %~2
        call :log "[WARN] %~2"
        set /a CHECK_WARN+=1
    ) else if "%~1"=="INFO" (
        echo     %ICON_IN% %~2
        call :log "[INFO] %~2"
    ) else if "%~1"=="STEP" (
        echo     %ICON_ST% %~2
        call :log "[STEP] %~2"
    )
    goto :eof

:check_path
    if exist "%~1" (
        call :print_result "PASS" "%~2"
    ) else (
        call :print_result "FAIL" "%~2"
    )
    goto :eof

:check_command
    where "%~1" >nul 2>&1
    if !errorlevel! equ 0 (
        call :print_result "PASS" "%~2"
        exit /b 0
    ) else (
        call :print_result "FAIL" "%~2"
        exit /b 1
    )

:check_pip_pkg
    python -c "import %~1" 2>nul
    if !errorlevel! equ 0 (
        call :print_result "PASS" "%~2"
        exit /b 0
    ) else (
        call :print_result "FAIL" "%~2"
        exit /b 1
    )

:install_pkg
    call :print_result "STEP" "Installation de %~1..."
    call :log "pip install %~1"
    python -m pip install "%~1" >> "%LOG_FILE%" 2>&1
    if !errorlevel! equ 0 (
        call :print_result "PASS" "%~2 - installe"
        exit /b 0
    ) else (
        call :print_result "FAIL" "%~2 - ECHEC installation"
        call :print_result "INFO" "Log: %LOG_FILE%"
        exit /b 1
    )

:: ===========================================================================
::  DEMARRAGE
:: ===========================================================================

call :print_banner

echo     Demarrage : %DATE% %TIME%
echo     Journal   : %LOG_FILE%
echo.

call :log "============================================================"
call :log "OEE Analytics Setup - Demarrage"
call :log "Date: %DATE% %TIME%"
call :log "Repertoire: %PRJ_ROOT%"
call :log "Journal: %LOG_FILE%"
call :log "============================================================"

:: ===========================================================================
::  PHASE 1 - ENVIRONNEMENT
:: ===========================================================================

call :print_section "Phase 1/5 - Verification de l'environnement"

call :check_path "%REQ_FILE%" "Repertoire projet trouve"

:: Python
call :check_command "python" "Python installe"
if errorlevel 1 (
    echo.
    echo     Impossible de continuer sans Python.
    echo     Telechargez-le depuis : https://www.python.org/downloads/
    echo     Cochez "Add Python to PATH" lors de l'installation.
    echo.
    pause
    exit /b 1
)

:: Version Python
for /f "tokens=1-2 delims=. " %%a in ('python --version 2^>^&1') do (
    if not "%%c"=="" (
        set "PY_VER=%%b.%%c"
    ) else (
        set "PY_VER=%%b"
    )
)
for /f "tokens=1 delims=." %%m in ("!PY_VER!") do set "PY_MAJOR=%%m"
for /f "tokens=2 delims=." %%m in ("!PY_VER!") do set "PY_MINOR=%%m"
if !PY_MAJOR! geq 3 if !PY_MINOR! geq 8 (
    call :print_result "PASS" "Python !PY_VER! (requis: 3.8+)"
) else (
    call :print_result "FAIL" "Python !PY_VER! - version 3.8+ requise"
    pause
    exit /b 1
)

:: pip
python -m pip --version >nul 2>&1
if errorlevel 1 (
    call :print_result "FAIL" "pip non trouve, installation..."
    python -m ensurepip --upgrade >> "%LOG_FILE%" 2>&1
    if errorlevel 1 (
        echo     Impossible d'installer pip.
        pause
        exit /b 1
    )
) else (
    for /f "tokens=1 delims= " %%v in ('python -m pip --version 2^>^&1') do set "PIP_VER=%%v"
    call :print_result "PASS" "pip !PIP_VER!"
)

:: JAVA_HOME
if defined JAVA_HOME (
    if exist "!JAVA_HOME!\bin\java.exe" (
        call :print_result "PASS" "JAVA_HOME = !JAVA_HOME!"
    ) else (
        call :print_result "FAIL" "JAVA_HOME invalide: !JAVA_HOME!"
        set "JAVA_HOME="
    )
)
if not defined JAVA_HOME (
    call :print_result "FAIL" "JAVA_HOME non defini"
    echo.
    echo     Spark necessite Java JDK 17+
    echo     Telechargez: https://learn.microsoft.com/java/openjdk
    echo.
    set "AUTO_JAVA="
    if exist "C:\Program Files\Microsoft\jdk-17*" (
        for /d %%d in ("C:\Program Files\Microsoft\jdk-17*") do set "AUTO_JAVA=%%d"
    ) else if exist "C:\Program Files\Eclipse Adoptium\jdk-17*" (
        for /d %%d in ("C:\Program Files\Eclipse Adoptium\jdk-17*") do set "AUTO_JAVA=%%d"
    ) else if exist "C:\Program Files\Java\jdk-17*" (
        for /d %%d in ("C:\Program Files\Java\jdk-17*") do set "AUTO_JAVA=%%d"
    )
    if defined AUTO_JAVA (
        set "JAVA_HOME=!AUTO_JAVA!"
        call :print_result "WARN" "JAVA_HOME pointe sur !AUTO_JAVA!"
    )
)

:: HADOOP_HOME / winutils
if defined HADOOP_HOME (
    call :print_result "PASS" "HADOOP_HOME = !HADOOP_HOME!"
) else if exist "%WINUTILS%" (
    set "HADOOP_HOME=%PRJ_ROOT%hadoop"
    call :print_result "WARN" "HADOOP_HOME = !HADOOP_HOME! (auto-detection)"
) else (
    call :print_result "WARN" "HADOOP_HOME non defini"
)
call :check_path "%WINUTILS%" "winutils.exe (Hadoop Windows)"
if not exist "%WINUTILS%" (
    call :print_result "WARN" "winutils manquant - Spark local peut echouer"
)

:: Docker (optionnel)
where docker >nul 2>&1
if errorlevel 1 (
    call :print_result "WARN" "Docker non trouve (optionnel, requis mode cluster)"
) else (
    call :print_result "PASS" "Docker disponible"
)

:: ===========================================================================
::  PHASE 2 - ARBORESCENCE
:: ===========================================================================

call :print_section "Phase 2/5 - Verification de l'arborescence"

call :check_path "%GENERATOR%" "data\generate_data.py"
call :check_path "%PIPELINE%"  "spark\oee_pipeline.py"
call :check_path "%DASHBOARD%" "powerbi\create_dashboard.py"
call :check_path "%PRJ_ROOT%Makefile" "Makefile"

for %%d in (
    "data\raw" "data\processed" "data\streaming"
    "data\streaming\input" "data\streaming\output" "data\streaming\checkpoint"
) do (
    if not exist "%PRJ_ROOT%%%~d" (
        mkdir "%PRJ_ROOT%%%~d" 2>nul
        call :print_result "PASS" "Dossier cree: %%~d"
    )
)
call :check_path "%PRJ_ROOT%data\raw"       "data\raw\"
call :check_path "%PRJ_ROOT%data\processed" "data\processed\"

:: ===========================================================================
::  PHASE 3 - DEPENDANCES PYTHON
:: ===========================================================================

call :print_section "Phase 3/5 - Dependances Python"

echo     Mise a jour de pip...
python -m pip install --upgrade pip >> "%LOG_FILE%" 2>&1
echo       pip ok

if exist "%REQ_FILE%" (
    call :print_result "STEP" "Installation depuis requirements.txt..."
    python -m pip install -r "%REQ_FILE%" >> "%LOG_FILE%" 2>&1
    if errorlevel 1 (
        call :print_result "WARN" "Certains packages ont echoue - voir log"
    ) else (
        call :print_result "PASS" "requirements.txt installe"
    )
) else (
    call :print_result "FAIL" "requirements.txt introuvable"
)

call :check_pip_pkg "pandas"  "pandas"
if errorlevel 1 call :install_pkg "pandas>=2.0.0" "pandas"

call :check_pip_pkg "numpy"   "numpy"
if errorlevel 1 call :install_pkg "numpy>=1.24.0" "numpy"

call :check_pip_pkg "pyspark" "pyspark"
if errorlevel 1 call :install_pkg "pyspark>=3.5.0" "pyspark"

call :check_pip_pkg "pyarrow" "pyarrow (Parquet)"
if errorlevel 1 (
    call :print_result "INFO" "pyarrow est necessaire pour le format Parquet"
    call :install_pkg "pyarrow" "pyarrow"
)

call :check_pip_pkg "kafka"   "kafka-python (streaming)"
if errorlevel 1 (
    call :print_result "WARN" "kafka-python non installe (optionnel, mode streaming)"
)

:: ===========================================================================
::  PHASE 4 - DONNEES
:: ===========================================================================

call :print_section "Phase 4/5 - Verification des donnees"

if exist "%PRJ_ROOT%data\raw\production_events.csv" (
    for %%f in ("%PRJ_ROOT%data\raw\production_events.csv") do (
        set "CSV_SIZE=%%~zf"
        for /f %%c in ('type "%%f" ^| find /c /v ""') do set "CSV_ROWS=%%c"
    )
    set /a CSV_ROWS-=1
    call :print_result "PASS" "production_events.csv - !CSV_ROWS! lignes, !CSV_SIZE! octets"
) else (
    call :print_result "WARN" "Donnees non generees"
    set "NEED_GENERATE=1"
)

if exist "%PRJ_ROOT%data\processed\oee_results.csv" (
    call :print_result "PASS" "oee_results.csv - pipeline deja execute"
    set "PIPELINE_DONE=1"
) else (
    call :print_result "WARN" "Pipeline OEE pas encore execute"
)

:: ===========================================================================
::  RECAPITULATIF
:: ===========================================================================

call :print_section "Recapitulatif"

echo     Resultats : %CHECK_PASS% ok, %CHECK_FAIL% echec(s), %CHECK_WARN% avertissement(s)
echo.
call :log "=== RECAPITULATIF ==="
call :log "Passes: %CHECK_PASS% / Echecs: %CHECK_FAIL% / Avertissements: %CHECK_WARN%"

:: ===========================================================================
::  PHASE 5 - LANCEMENT
:: ===========================================================================

call :print_section "Phase 5/5 - Lancement"

echo  Choisissez le mode de demarrage :
echo.
echo    [1]  Pipeline complet local
echo    [2]  Pipeline Docker (cluster Hadoop + Spark)
echo    [3]  Streaming temps reel
echo    [4]  Generer les donnees uniquement
echo    [5]  Mode diagnostic (verifications seulement)
echo    [Q]  Quitter
echo.
set /p "MODE=  Votre choix [1] : "
if "%MODE%"=="" set "MODE=1"

echo.
call :log "Mode selectionne: %MODE%"

if "%MODE%"=="1" goto :run_local
if "%MODE%"=="2" goto :run_docker
if "%MODE%"=="3" goto :run_streaming
if "%MODE%"=="4" goto :run_generate
if "%MODE%"=="5" goto :finish
if /i "%MODE%"=="Q" goto :finish

echo  Choix invalide. Lancement du mode local par defaut.
timeout /t 2 >nul
goto :run_local

:: ===========================================================================
::  MODE LOCAL
:: ===========================================================================

:run_local
    call :print_section "Pipeline OEE - Mode local"

    if not defined JAVA_HOME (
        echo  JAVA_HOME n'est pas defini.
        echo  Le pipeline Spark ne peut pas continuer sans Java.
        echo.
        echo  Solutions :
        echo    - Telechargez Microsoft OpenJDK 17
        echo      https://learn.microsoft.com/java/openjdk
        echo    - Creez la variable systeme JAVA_HOME
        echo    - Relancez ce script
        echo.
        pause
        exit /b 1
    )

    set "HADOOP_HOME=%PRJ_ROOT%hadoop"
    set "PATH=%JAVA_HOME%\bin;%HADOOP_HOME%\bin;%PATH%"
    set "PYTHONPATH=%PRJ_ROOT%;%PYTHONPATH%"

    call :print_result "INFO" "JAVA_HOME  = !JAVA_HOME!"
    call :print_result "INFO" "HADOOP_HOME = !HADOOP_HOME!"

    echo.
    call :print_result "STEP" "Etape 1/3 : Generation des donnees..."
    python "%GENERATOR%"
    if errorlevel 1 (
        echo  Erreur lors de la generation des donnees.
        pause
        exit /b 1
    )
    call :print_result "PASS" "Donnees generees"

    echo.
    call :print_result "STEP" "Etape 2/3 : Pipeline PySpark (2-5 minutes)..."
    python "%PIPELINE%"
    if errorlevel 1 (
        echo.
        echo  Le pipeline Spark a rencontre une erreur.
        echo.
        echo  Causes possibles :
        echo    - Java JDK introuvable
        echo    - Memoire insuffisante (2 Go requis)
        echo    - winutils manquant
        echo.
        pause
        exit /b 1
    )
    call :print_result "PASS" "Pipeline termine"

    echo.
    call :print_result "STEP" "Etape 3/3 : Tableau de bord..."
    python "%DASHBOARD%"
    if errorlevel 1 (
        call :print_result "WARN" "Erreur lors de la generation du dashboard"
    ) else (
        call :print_result "PASS" "Tableau de bord genere"
        echo.
        echo ===========================================================================
        echo   PIPELINE TERMINE AVEC SUCCES
        echo ===========================================================================
        echo.
        echo  Fichiers generes :
        echo    - data\processed\oee_results.csv
        echo    - data\processed\oee_results.parquet
        echo    - data\processed\oee_monthly_summary.csv
        echo    - data\processed\oee_dashboard.html
        echo.
        echo  Prochaines etapes :
        echo    - Importez oee_results.csv dans Power BI Desktop
        echo    - Ouvrez oee_dashboard.html dans un navigateur
        echo.
    )
    goto :finish

:: ===========================================================================
::  MODE DOCKER
:: ===========================================================================

:run_docker
    call :print_section "Pipeline OEE - Mode Docker"

    where docker >nul 2>&1
    if errorlevel 1 (
        echo  Docker n'est pas installe.
        echo  Telechargez: https://www.docker.com/products/docker-desktop
        pause
        exit /b 1
    )

    call :print_result "STEP" "Demarrage du cluster..."
    docker-compose -f "%PRJ_ROOT%docker-compose.yml" up -d bigdata-node
    if errorlevel 1 (
        echo  Erreur au demarrage du cluster Docker.
        pause
        exit /b 1
    )

    echo.
    echo  Attente de HDFS (60 secondes)...
    echo  Interface: http://localhost:9870
    timeout /t 60 /nobreak >nul

    call :print_result "STEP" "Upload des donnees vers HDFS..."
    docker exec oee-bigdata hdfs dfs -mkdir -p /oee/raw/
    docker exec oee-bigdata hdfs dfs -put -f /data/raw/production_events.csv /oee/raw/

    call :print_result "STEP" "Execution du pipeline Spark..."
    docker-compose -f "%PRJ_ROOT%docker-compose.yml" run --rm --profile pipeline spark-submit

    call :print_result "STEP" "Generation du tableau de bord..."
    python "%DASHBOARD%"

    echo.
    echo ===========================================================================
    echo   PIPELINE DOCKER TERMINE AVEC SUCCES
    echo ===========================================================================
    echo.
    goto :finish

:: ===========================================================================
::  STREAMING
:: ===========================================================================

:run_streaming
    call :print_section "Streaming OEE - Temps reel"

    echo  Lancement du serveur de dashboard temps reel...
    echo.
    echo  Le serveur demarrera sur http://localhost:8050
    echo  Fermez la fenetre avec Ctrl+C pour arreter.
    echo.
    echo  Dans un autre terminal, lancez :
    echo    python streaming/producer.py --burst 2 --interval 2
    echo.
    pause
    start "OEE Live Dashboard" cmd /k "cd /d "%PRJ_ROOT%" && python streaming/live_dashboard.py"
    goto :finish

:: ===========================================================================
::  GENERATION UNIQUEMENT
:: ===========================================================================

:run_generate
    call :print_section "Generation des donnees"

    python "%GENERATOR%"
    if errorlevel 1 (
        call :print_result "FAIL" "Erreur lors de la generation"
    ) else (
        call :print_result "PASS" "Donnees generees dans data\raw\"
    )
    goto :finish

:: ===========================================================================
::  FIN
:: ===========================================================================

:finish
    echo.
    echo ---------------------------------------------------------------------------
    echo  Journal : %LOG_FILE%
    echo ---------------------------------------------------------------------------
    echo.
    call :log "=== SCRIPT TERMINE ==="
    echo.
    echo Appuyez sur une touche pour fermer.
    pause
    exit /b 0
