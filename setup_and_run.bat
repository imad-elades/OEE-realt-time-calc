@echo off
setlocal enabledelayedexpansion

:: ============================================================================
::  OEE Analytics Pipeline — Setup & Launch Script
::  Version : 1.0.0
::  Auteur  : Projet BIG Data
:: ============================================================================

:: ── Configuration des couleurs ANSI (Windows 10+ / Terminal moderne) ──────
for /f "tokens=2 delims=:." %%a in (
    'ver'
) do set "winver=%%a"
if !winver! geq 10 (
    set "ESC="
    set "C_RESET=!ESC![0m"
    set "C_BOLD=!ESC![1m"
    set "C_DIM=!ESC![2m"
    set "C_GREEN=!ESC![32m"
    set "C_YELLOW=!ESC![33m"
    set "C_RED=!ESC![31m"
    set "C_CYAN=!ESC![36m"
    set "C_MAGENTA=!ESC![35m"
    set "C_WHITE=!ESC![97m"
    set "C_BG_GREEN=!ESC![42m"
    set "C_BG_RED=!ESC![41m"
    set "C_BG_YELLOW=!ESC![43m"
    set "C_BG_BLUE=!ESC![44m"
    set "C_BG_CYAN=!ESC![46m"
    set "ICON_OK=!C_GREEN!^[✓!C_RESET!"
    set "ICON_FAIL=!C_RED!^[✗!C_RESET!"
    set "ICON_WARN=!C_YELLOW!^[⚠!C_RESET!"
    set "ICON_INFO=!C_CYAN!^[ℹ!C_RESET!"
    set "ICON_STEP=!C_MAGENTA!^[▶!C_RESET!"
) else (
    set "C_RESET="
    set "C_BOLD="
    set "C_DIM="
    set "C_GREEN="
    set "C_YELLOW="
    set "C_RED="
    set "C_CYAN="
    set "C_MAGENTA="
    set "C_WHITE="
    set "C_BG_GREEN="
    set "C_BG_RED="
    set "C_BG_YELLOW="
    set "C_BG_BLUE="
    set "C_BG_CYAN="
    set "ICON_OK=[OK]"
    set "ICON_FAIL=[FAIL]"
    set "ICON_WARN=[WARN]"
    set "ICON_INFO=[INFO]"
    set "ICON_STEP=[>>]"
)

:: ── Variables globales ─────────────────────────────────────────────────────
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
set "INSTALLED_PKGS="

:: ── Fichier journal ────────────────────────────────────────────────────────
set "LOG_DIR=%PRJ_ROOT%logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" 2>nul
for /f "tokens=2 delims==" %%I in (
    'wmic os get localdatetime /value 2^>nul'
) do set "DT=%%I"
if "%DT%"=="" set "DT=%DATE:/=-%_%TIME::=-%"
set "DT=%DT: =0%"
set "LOG_FILE=%LOG_DIR%\setup_%DT:~0,8%_%DT:~8,6%.log"
if "%DT%"=="0" set "LOG_FILE=%LOG_DIR%\setup_%DATE:/=-%_%TIME::=-%.log"

echo. >> "%LOG_FILE%" 2>nul

:: ═══════════════════════════════════════════════════════════════════════════
::  FONCTIONS
:: ═══════════════════════════════════════════════════════════════════════════

:log
    echo [%TIME%] %*>> "%LOG_FILE%"
    goto :eof

:print_banner
    cls
    echo !C_BG_BLUE!                                                                  !C_RESET!
    echo !C_BG_BLUE!   !C_BOLD!!C_WHITE!        OEE Analytics Pipeline                 !C_RESET!
    echo !C_BG_BLUE!   !C_DIM!  Automated Setup ^& Launcher v1.0.0                     !C_RESET!
    echo !C_BG_BLUE!                                                                  !C_RESET!
    echo.
    goto :eof

:print_section
    setlocal
    set "sectitle=%~1"
    echo.
    echo !C_BOLD!!C_CYAN!┌──────────────────────────────────────────────────────────────┐!C_RESET!
    echo !C_BOLD!!C_CYAN!│ !C_WHITE!!sectitle:~0,60!!C_RESET!!C_BOLD!!C_CYAN!│!C_RESET!
    echo !C_BOLD!!C_CYAN!└──────────────────────────────────────────────────────────────┘!C_RESET!
    echo.
    call :log "=== %~1 ==="
    endlocal
    goto :eof

:print_result
    setlocal
    set "pstatus=%~1"
    set "pmessage=%~2"
    if "!pstatus!"=="PASS" (
        echo     !ICON_OK! !pmessage!
        call :log "[PASS] %~2"
        set /a "p_count=CHECK_PASS+1"
    ) else if "!pstatus!"=="FAIL" (
        echo     !ICON_FAIL! !pmessage!
        call :log "[FAIL] %~2"
        set /a "p_count=CHECK_FAIL+1"
    ) else if "!pstatus!"=="WARN" (
        echo     !ICON_WARN! !C_YELLOW!%~2!C_RESET!
        call :log "[WARN] %~2"
        set /a "p_count=CHECK_WARN+1"
    ) else if "!pstatus!"=="INFO" (
        echo     !ICON_INFO! !C_CYAN!%~2!C_RESET!
        call :log "[INFO] %~2"
        set "p_count="
    ) else if "!pstatus!"=="STEP" (
        echo     !ICON_STEP! !C_MAGENTA!%~2!C_RESET!
        call :log "[STEP] %~2"
        set "p_count="
    )
    endlocal
    if defined p_count (
        if "%~1"=="PASS" set /a CHECK_PASS+=1
        if "%~1"=="FAIL" set /a CHECK_FAIL+=1
        if "%~1"=="WARN" set /a CHECK_WARN+=1
    )
    goto :eof

:print_status_header
    echo     !C_DIM!┌──────────────────────────────────────────────────────┐!C_RESET!
    exit /b 0

:check_path
    if exist "%~1" (
        call :print_result "PASS" "%~2"
    ) else (
        call :print_result "FAIL" "%~2"
    )
    goto :eof

:: ── Vérifier qu'une commande existe dans le PATH ───────────────────────────
:check_command
    setlocal
    set "cmd=%~1"
    set "label=%~2"
    where "%cmd%" >nul 2>&1
    if !errorlevel! equ 0 (
        call :print_result "PASS" "%label%"
        endlocal & call :log "[PASS] %label%"
        exit /b 0
    ) else (
        call :print_result "FAIL" "%label%"
        for /f "usebackq delims=" %%a in (`where "%cmd%" 2^>nul`) do set "cpath=%%a"
        endlocal & call :log "[FAIL] %label%"
        exit /b 1
    )

:: ── Vérifier un package pip ────────────────────────────────────────────────
:check_pip_pkg
    setlocal
    set "pkg=%~1"
    set "label=%~2"
    python -c "import %pkg%" 2>nul
    if !errorlevel! equ 0 (
        call :print_result "PASS" "%label%"
        endlocal & call :log "[PASS] %label%"
        exit /b 0
    ) else (
        call :print_result "FAIL" "%label%"
        endlocal & call :log "[FAIL] %label%"
        exit /b 1
    )

:: ── Installer un package pip ───────────────────────────────────────────────
:install_pkg
    setlocal
    set "pkg=%~1"
    set "label=%~2"
    call :print_result "STEP" "Installation de !pkg!..."
    python -m pip install "%pkg%" >> "%LOG_FILE%" 2>&1
    if !errorlevel! equ 0 (
        call :print_result "PASS" "!label! --- installé avec succès"
        endlocal & call :log "[INSTALL OK] %label%"
        exit /b 0
    ) else (
        call :print_result "FAIL" "!label! --- ÉCHEC de l'installation"
        endlocal & call :log "[INSTALL FAIL] %label%"
        exit /b 1
    )

:: ═══════════════════════════════════════════════════════════════════════════
::  DÉBUT DU SCRIPT
:: ═══════════════════════════════════════════════════════════════════════════

call :print_banner
echo     !C_DIM!Démarrage : %DATE% %TIME%!C_RESET!
echo     !C_DIM!Journal    : %LOG_FILE%!C_RESET!
echo.
call :log "============================================================"
call :log "OEE Analytics Setup - Demarrage"
call :log "Date: %DATE% %TIME%"
call :log "Repertoire: %PRJ_ROOT%"
call :log "Journal: %LOG_FILE%"
call :log "============================================================"

:: ═══════════════════════════════════════════════════════════════════════════
::  PHASE 1 — VÉRIFICATION DE L'ENVIRONNEMENT
:: ═══════════════════════════════════════════════════════════════════════════

call :print_section "Phase 1/5 — Verification de l'environnement"

:: ── Vérification du répertoire projet ─────────────────────────────────────
call :check_path "%PRJ_ROOT%requirements.txt" "Repertoire projet trouve"

:: ── Python ────────────────────────────────────────────────────────────────
call :check_command "python" "Python 3 installe"
if errorlevel 1 (
    echo.
    echo     !ICON_FAIL! !C_RED!!C_BOLD!Python n'est pas installe ou introuvable dans le PATH.!C_RESET!
    echo     !C_YELLOW!  Telechargez-le depuis : https://www.python.org/downloads/!C_RESET!
    echo     !C_YELLOW}!  Cochez "Add Python to PATH" lors de l'installation.!C_RESET!
    echo.
    pause
    exit /b 1
)

:: ── Version Python ─────────────────────────────────────────────────────────
for /f "tokens=2 delims= " %%V in ('python --version 2^>^&1') do set "PY_VER=%%V"
for /f "tokens=1 delims=." %%M in ("!PY_VER!") do set "PY_MAJOR=%%M"
for /f "tokens=2 delims=." %%m in ("!PY_VER!") do set "PY_MINOR=%%m"
if !PY_MAJOR! geq 3 (
    if !PY_MINOR! geq 8 (
        call :print_result "PASS" "Python !PY_VER! (requis : 3.8+)"
    ) else (
        call :print_result "FAIL" "Python !PY_VER! --- version 3.8+ requise"
        echo     !C_YELLOW!  Mettez a jour Python depuis https://www.python.org/downloads/!C_RESET!
        pause
        exit /b 1
    )
) else (
    call :print_result "FAIL" "Python !PY_VER! --- version 3.8+ requise"
    pause
    exit /b 1
)

:: ── pip ────────────────────────────────────────────────────────────────────
python -m pip --version >nul 2>&1
if errorlevel 1 (
    call :print_result "FAIL" "pip --- pas trouve"
    echo     !C_YELLOW!  Installation de pip...!C_RESET!
    python -m ensurepip --upgrade >> "%LOG_FILE%" 2>&1
    if errorlevel 1 (
        echo     !C_RED!  Impossible d'installer pip.!C_RESET!
        pause
        exit /b 1
    )
    call :print_result "PASS" "pip --- installe"
) else (
    for /f "tokens=1 delims= " %%V in ('python -m pip --version 2^>^&1') do set "PIP_VER=%%V"
    call :print_result "PASS" "pip !PIP_VER!"
)
call :log "Python %PY_VER% / pip %PIP_VER%"

:: ── Java (JAVA_HOME) ──────────────────────────────────────────────────────
if defined JAVA_HOME (
    if exist "!JAVA_HOME!\bin\java.exe" (
        for /f "tokens=3 delims= " %%J in ('"!JAVA_HOME!\bin\java" -version 2^>^&1 ^| findstr /i "version"') do set "JAVA_VER=%%J"
        set "JAVA_VER=!JAVA_VER:"=!
        call :print_result "PASS" "JAVA_HOME = !JAVA_HOME!  [Java !JAVA_VER!]"
    ) else (
        call :print_result "FAIL" "JAVA_HOME pointe vers un dossier invalide"
        call :print_result "INFO" "Telechargez JDK 17 : https://learn.microsoft.com/java/openjdk"
    )
) else (
    call :print_result "FAIL" "JAVA_HOME --- variable d'environnement non definie"
    call :print_result "INFO" "Spark necessite Java JDK 17+"
    call :print_result "INFO" "Telechargez : https://learn.microsoft.com/java/openjdk"
    call :print_result "INFO" "Puis creez la variable systeme JAVA_HOME"

    :: Tentative de détection automatique
    set "AUTO_JAVA="
    if exist "C:\Program Files\Microsoft\jdk-17*" (
        for /d %%D in ("C:\Program Files\Microsoft\jdk-17*") do set "AUTO_JAVA=%%D"
    ) else if exist "C:\Program Files\Eclipse Adoptium\jdk-17*" (
        for /d %%D in ("C:\Program Files\Eclipse Adoptium\jdk-17*") do set "AUTO_JAVA=%%D"
    ) else if exist "C:\Program Files\Java\jdk-17*" (
        for /d %%D in ("C:\Program Files\Java\jdk-17*") do set "AUTO_JAVA=%%D"
    )
    if defined AUTO_JAVA (
        set "JAVA_HOME=!AUTO_JAVA!"
        call :print_result "WARN" "JAVA_HOME defini automatiquement sur !AUTO_JAVA!"
    )
)

:: ── HADOOP_HOME & winutils ────────────────────────────────────────────────
if defined HADOOP_HOME (
    call :print_result "PASS" "HADOOP_HOME = !HADOOP_HOME!"
) else (
    call :print_result "WARN" "HADOOP_HOME --- variable non definie"
    if exist "%WINUTILS%" (
        set "HADOOP_HOME=%PRJ_ROOT%hadoop"
        call :print_result "WARN" "HADOOP_HOME defini automatiquement sur !HADOOP_HOME!"
    )
)

call :check_path "%WINUTILS%" "winutils.exe (Hadoop Windows)"
if not exist "%WINUTILS%" (
    call :print_result "WARN" "Spark local pourrait echouer sans winutils"
)

:: ── docker (optionnel) ────────────────────────────────────────────────────
where docker >nul 2>&1
if errorlevel 1 (
    call :print_result "WARN" "Docker --- non trouve (optionnel, requis pour mode cluster)"
) else (
    call :print_result "PASS" "Docker --- disponible"
)

:: ═══════════════════════════════════════════════════════════════════════════
::  PHASE 2 — VÉRIFICATION DE L'ARBORESCENCE
:: ═══════════════════════════════════════════════════════════════════════════

call :print_section "Phase 2/5 — Verification de l'arborescence projet"

call :check_path "%GENERATOR%"         "data\generate_data.py"
call :check_path "%PIPELINE%"          "spark\oee_pipeline.py"
call :check_path "%DASHBOARD%"         "powerbi\create_dashboard.py"
call :check_path "%PRJ_ROOT%Makefile"  "Makefile"

if not exist "%PRJ_ROOT%data\raw"       mkdir "%PRJ_ROOT%data\raw"
if not exist "%PRJ_ROOT%data\processed" mkdir "%PRJ_ROOT%data\processed"
if not exist "%PRJ_ROOT%data\streaming" mkdir "%PRJ_ROOT%data\streaming"
if not exist "%PRJ_ROOT%data\streaming\input"        mkdir "%PRJ_ROOT%data\streaming\input"
if not exist "%PRJ_ROOT%data\streaming\output"       mkdir "%PRJ_ROOT%data\streaming\output"
if not exist "%PRJ_ROOT%data\streaming\checkpoint"   mkdir "%PRJ_ROOT%data\streaming\checkpoint"

call :check_path "%PRJ_ROOT%data\raw"       "data\raw\"
call :check_path "%PRJ_ROOT%data\processed" "data\processed\"
call :check_path "%PRJ_ROOT%data\streaming" "data\streaming\"

:: ═══════════════════════════════════════════════════════════════════════════
::  PHASE 3 — VÉRIFICATION & INSTALLATION DES DÉPENDANCES PYTHON
:: ═══════════════════════════════════════════════════════════════════════════

call :print_section "Phase 3/5 — Verification des dependances Python"

:: ── Mise à jour de pip ────────────────────────────────────────────────────
call :print_result "INFO" "Mise a jour de pip..."
python -m pip install --upgrade pip >> "%LOG_FILE%" 2>&1
call :print_result "PASS" "pip --- mis a jour"

:: ── Installation des dépendances depuis requirements.txt ──────────────────
if exist "%REQ_FILE%" (
    call :print_result "STEP" "Installation des packages depuis requirements.txt..."
    python -m pip install -r "%REQ_FILE%" >> "%LOG_FILE%" 2>&1
    if !errorlevel! equ 0 (
        call :print_result "PASS" "requirements.txt --- tous les packages installes"
    ) else (
        call :print_result "WARN" "Certains packages ont echoue (voir logs)"
        echo     !C_DIM!     Consultez le journal : %LOG_FILE%!C_RESET!
    )
) else (
    call :print_result "FAIL" "requirements.txt --- fichier introuvable"
)

:: ── Vérification individuelle des packages ────────────────────────────────
call :check_pip_pkg "pandas"   "pandas"
if errorlevel 1 (
    call :install_pkg "pandas>=2.0.0" "pandas"
)

call :check_pip_pkg "numpy"    "numpy"
if errorlevel 1 (
    call :install_pkg "numpy>=1.24.0" "numpy"
)

call :check_pip_pkg "pyspark"  "pyspark"
if errorlevel 1 (
    call :install_pkg "pyspark>=3.5.0" "pyspark"
)

:: ── pyarrow (indispensable pour Parquet) ──────────────────────────────────
call :check_pip_pkg "pyarrow"  "pyarrow (Parquet)"
if errorlevel 1 (
    call :print_result "INFO" "pyarrow est necessaire pour l'ecriture Parquet"
    call :install_pkg "pyarrow" "pyarrow"
)

:: ── kafka-python (optionnel, pour le mode streaming) ──────────────────────
call :check_pip_pkg "kafka" "kafka-python (streaming)"
if errorlevel 1 (
    call :print_result "WARN" "kafka-python non installe (optionnel, requis pour mode streaming Kafka)"
)

:: ═══════════════════════════════════════════════════════════════════════════
::  PHASE 4 — VÉRIFICATION DES DONNÉES GÉNÉRÉES
:: ═══════════════════════════════════════════════════════════════════════════

call :print_section "Phase 4/5 — Verification des donnees"

if exist "%PRJ_ROOT%data\raw\production_events.csv" (
    for %%F in ("%PRJ_ROOT%data\raw\production_events.csv") do set "CSV_SIZE=%%~zF"
    for /f %%L in ('findstr /R /N "^" "%PRJ_ROOT%data\raw\production_events.csv" 2^>nul ^| find /C ":"') do set "CSV_LINES=%%L"
    set /a CSV_LINES-=1
    call :print_result "PASS" "production_events.csv --- !CSV_LINES! lignes, !CSV_SIZE! octets"
) else (
    call :print_result "WARN" "Aucune donnee generee --- phase de generation necessaire"
    set "NEED_GENERATE=1"
)

if exist "%PRJ_ROOT%data\processed\oee_results.csv" (
    call :print_result "PASS" "oee_results.csv --- pipeline deja execute"
    set "PIPELINE_DONE=1"
) else (
    call :print_result "WARN" "Pipeline OEE non encore execute"
    set "PIPELINE_DONE=0"
)

:: ═══════════════════════════════════════════════════════════════════════════
::  RÉCAPITULATIF
:: ═══════════════════════════════════════════════════════════════════════════

call :print_section "Recapitulatif des verifications"

echo     !C_DIM!┌──────────────────────────────────────────────────────────────┐!C_RESET!
echo     !C_DIM!│!C_RESET!  Resultats : !C_GREEN!%CHECK_PASS% passes!C_RESET!, !C_RED!%CHECK_FAIL% echoues!C_RESET!, !C_YELLOW!%CHECK_WARN% avertissements!C_RESET!  !C_DIM!│!C_RESET!
echo     !C_DIM!└──────────────────────────────────────────────────────────────┘!C_RESET!

echo.
echo     !C_BOLD!Legende :!C_RESET%
echo       !C_GREEN![✓]!C_RESET!  Verification reussie
echo       !C_RED![✗]!C_RESET!  Erreur bloquante
echo       !C_YELLOW![⚠]!C_RESET!  Avertissement (non bloquant)
echo       !C_CYAN![ℹ]!C_RESET!  Information
echo       !C_MAGENTA![▶]!C_RESET!  Action en cours
echo.

:: ── Journalisation du récapitulatif ───────────────────────────────────────
call :log "=== RECAPITULATIF ==="
call :log "Passes: %CHECK_PASS% / Echecs: %CHECK_FAIL% / Avertissements: %CHECK_WARN%"
call :log "Journal: %LOG_FILE%"

:: ═══════════════════════════════════════════════════════════════════════════
::  PHASE 5 — LANCEMENT DU PROJET
:: ═══════════════════════════════════════════════════════════════════════════

call :print_section "Phase 5/5 — Lancement du projet"

:: ── Menu de lancement ─────────────────────────────────────────────────────
echo     !C_BOLD!Choisissez le mode de demarrage :!C_RESET%
echo.
echo       [1]  Pipeline complet local
echo       [2]  Pipeline complet Docker (cluster Hadoop + Spark)
echo       [3]  Streaming temps reel (live dashboard + producer)
echo       [4]  Generer les donnees uniquement
echo       [5]  Mode diagnostic (verifications seulement)
echo       [Q]  Quitter
echo.
set /p "MODE=     !C_BOLD!Votre choix [1] : !C_RESET!"
if "!MODE!"=="" set "MODE=1"

echo.
call :log "Mode selectionne: %MODE%"

if "!MODE!"=="1" goto :run_local
if "!MODE!"=="2" goto :run_docker
if "!MODE!"=="3" goto :run_streaming
if "!MODE!"=="4" goto :run_generate
if "!MODE!"=="5" goto :finish
if /i "!MODE!"=="Q" goto :finish
if /i "!MODE!"=="q" goto :finish

echo     !C_RED!Choix invalide. Lancement du mode local par defaut.!C_RESET!
timeout /t 2 >nul
goto :run_local

:: ── Mode local ─────────────────────────────────────────────────────────────
:run_local
    call :print_section "Pipeline OEE — Mode local"

    if not defined JAVA_HOME (
        echo     !C_RED!!C_BOLD!JAVA_HOME n'est pas defini.!C_RESET%
        echo     !C_YELLOW!  Le pipeline Spark ne peut pas fonctionner sans Java.!C_RESET%
        echo.
        echo     !C_CYAN!  Solutions :!C_RESET%
        echo       - Telechargez Microsoft OpenJDK 17 :
        echo         https://learn.microsoft.com/java/openjdk
        echo       - Creez la variable systeme JAVA_HOME pointant vers le dossier JDK
        echo       - Redemarrez ce script
        echo.
        pause
        exit /b 1
    )

    :: Définition des variables d'environnement
    set "HADOOP_HOME=%PRJ_ROOT%hadoop"
    set "PATH=%JAVA_HOME%\bin;%HADOOP_HOME%\bin;%PATH%"

    call :print_result "INFO" "JAVA_HOME  = !JAVA_HOME!"
    call :print_result "INFO" "HADOOP_HOME = !HADOOP_HOME!"

    :: Ajout du dossier projet au PYTHONPATH
    set "PYTHONPATH=%PRJ_ROOT%;%PYTHONPATH%"

    :: ── Étape 1 : Génération des données ──────────────────────────────────
    echo.
    call :print_result "STEP" "Etape 1/3 : Generation des donnees..."
    python "%GENERATOR%"
    if !errorlevel! neq 0 (
        echo     !C_RED!  Erreur lors de la generation des donnees.!C_RESET!
        echo     !C_DIM!  Consultez le journal : %LOG_FILE%!C_RESET!
        echo.
        pause
        exit /b 1
    )
    call :print_result "PASS" "Donnees generees avec succes"
    call :log "Generation terminee"

    :: ── Étape 2 : Pipeline PySpark ────────────────────────────────────────
    echo.
    call :print_result "STEP" "Etape 2/3 : Execution du pipeline PySpark..."
    call :log "Lancement du pipeline Spark..."
    echo     !C_DIM!  Ceci peut prendre quelques minutes...!C_RESET!
    echo.
    python "%PIPELINE%"
    if !errorlevel! neq 0 (
        echo.
        echo     !C_YELLOW!  Le pipeline Spark a rencontre une erreur.!C_RESET%
        echo     !C_DIM!  Consultez le journal : %LOG_FILE%!C_RESET!
        echo.
        echo     !C_CYAN!  Causes possibles :!C_RESET%
        echo       - Java JDK introuvable ou mal configure
        echo       - Memoire insuffisante (2 Go requis)
        echo       - Chemin Hadoop mal defini
        echo.
        pause
        exit /b 1
    )
    call :print_result "PASS" "Pipeline PySpark termine avec succes"
    call :log "Pipeline termine"

    :: ── Étape 3 : Tableau de bord ─────────────────────────────────────────
    echo.
    call :print_result "STEP" "Etape 3/3 : Generation du tableau de bord..."
    python "%DASHBOARD%"
    if !errorlevel! equ 0 (
        call :print_result "PASS" "Tableau de bord genere avec succes"
        echo.
        echo     !C_BG_GREEN!                                                                  !C_RESET!
        echo     !C_BG_GREEN!  !C_BOLD!  Pipeline OEE termine avec succes !                       !C_RESET!
        echo     !C_BG_GREEN!                                                                  !C_RESET!
        echo.
        echo     !C_BOLD!Fichiers generes :!C_RESET%
        echo       - !C_CYAN!data\processed\oee_results.csv!C_RESET!
        echo       - !C_CYAN!data\processed\oee_results.parquet!C_RESET!
        echo       - !C_CYAN!data\processed\oee_monthly_summary.csv!C_RESET!
        echo       - !C_CYAN!data\processed\oee_dashboard.html!C_RESET!
        echo.
        echo     !C_BOLD!Prochaines etapes :!C_RESET%
        echo       - Importez oee_results.csv dans Power BI Desktop
        echo       - Ouvrez oee_dashboard.html dans un navigateur
        echo.
    ) else (
        call :print_result "WARN" "Erreur lors de la generation du dashboard (non bloquant)"
    )
    goto :finish

:: ── Mode Docker ────────────────────────────────────────────────────────────
:run_docker
    call :print_section "Pipeline OEE — Mode Docker (cluster)"

    where docker >nul 2>&1
    if errorlevel 1 (
        echo     !C_RED!!C_BOLD!Docker n'est pas installe.!C_RESET%
        echo     !C_YELLOW!  Telechargez Docker Desktop : https://www.docker.com/products/docker-desktop!C_RESET!
        pause
        exit /b 1
    )

    call :print_result "STEP" "Demarrage du cluster BigData..."
    call :log "Docker-compose up..."
    docker-compose -f "%PRJ_ROOT%docker-compose.yml" up -d bigdata-node
    if !errorlevel! neq 0 (
        echo     !C_RED!  Erreur au demarrage du cluster Docker.!C_RESET!
        pause
        exit /b 1
    )

    echo     !C_YELLOW!  Attente de HDFS (60 secondes)...!C_RESET!
    call :print_result "INFO" "Surveillez http://localhost:9870 (NameNode)"
    timeout /t 60 /nobreak >nul

    call :print_result "STEP" "Upload des donnees vers HDFS..."
    docker exec oee-bigdata hdfs dfs -mkdir -p /oee/raw/
    docker exec oee-bigdata hdfs dfs -put -f /data/raw/production_events.csv /oee/raw/

    call :print_result "STEP" "Execution du pipeline Spark dans le cluster..."
    docker-compose -f "%PRJ_ROOT%docker-compose.yml" run --rm --profile pipeline spark-submit

    call :print_result "STEP" "Generation du tableau de bord..."
    python "%DASHBOARD%"

    echo.
    echo     !C_BG_GREEN!                                                                  !C_RESET!
    echo     !C_BG_GREEN!  !C_BOLD!  Pipeline Docker termine avec succes !                    !C_RESET!
    echo     !C_BG_GREEN!                                                                  !C_RESET!
    echo.
    call :log "Pipeline Docker termine"
    goto :finish

:: ── Mode streaming ─────────────────────────────────────────────────────────
:run_streaming
    call :print_section "Streaming OEE — Temps reel"

    echo     !C_BOLD!Lancement du serveur de dashboard temps reel...!C_RESET%
    echo.
    echo     !C_CYAN!  Le serveur demarrera sur http://localhost:8050!C_RESET!
    echo     !C_YELLOW!  Appuyez sur Ctrl+C pour arreter le serveur.!C_RESET!
    echo.
    echo     !C_DIM!  Dans un autre terminal, lancez :!C_RESET%
    echo       python streaming/producer.py --burst 2 --interval 2
    echo.
    pause
    start "OEE Live Dashboard" cmd /k "cd /d "%PRJ_ROOT%" && python streaming/live_dashboard.py"
    goto :finish

:: ── Génération uniquement ──────────────────────────────────────────────────
:run_generate
    call :print_section "Generation des donnees uniquement"

    python "%GENERATOR%"
    if !errorlevel! equ 0 (
        call :print_result "PASS" "Donnees generees dans data\raw\"
        for %%F in ("%PRJ_ROOT%data\raw\production_events.csv") do set "size=%%~zF"
        echo     !C_DIM!  Taille du fichier : !size! octets!C_RESET!
    ) else (
        call :print_result "FAIL" "Erreur lors de la generation"
    )
    goto :finish

:: ═══════════════════════════════════════════════════════════════════════════
::  FIN
:: ═══════════════════════════════════════════════════════════════════════════

:finish
    echo.
    echo     !C_DIM!┌──────────────────────────────────────────────────────────────┐!C_RESET!
    echo     !C_DIM!│!C_RESET!  Journal d'execution : %LOG_FILE%       !C_DIM!│!C_RESET!
    echo     !C_DIM!└──────────────────────────────────────────────────────────────┘!C_RESET!
    echo.
    call :log "=== SCRIPT TERMINE ==="
    echo     !C_BOLD!Script termine.!C_RESET%  Appuyez sur une touche pour fermer.
    pause >nul
    exit /b 0
