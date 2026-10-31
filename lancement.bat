@echo off
title OEE Analytics - Dashboard & Power BI
cd /d "%~dp0"

:: ===========================================================================
::  OEE Analytics - Lancement du Dashboard Streaming + Power BI
:: ===========================================================================

cls
echo ===========================================================================
echo   OEE Analytics Pipeline - Lancement
echo ===========================================================================
echo.
echo  [1] Ouverture du dashboard temps reel...
start http://localhost:8050
echo      http://localhost:8050
echo.

:: Verifier si le serveur streaming tourne
powershell -Command "if ((Get-NetTCPConnection -LocalPort 8050 -ErrorAction SilentlyContinue).Count -eq 0) { Write-Host '     [ATTENTION] Serveur streaming non detecte sur le port 8050' -ForegroundColor Yellow; Write-Host '     Lancez : python streaming/live_dashboard.py' -ForegroundColor Yellow } else { Write-Host '     [OK] Serveur streaming actif' -ForegroundColor Green }"
echo.

echo  [2] Verification des donnees...
if exist "data\processed\oee_results.csv" (
    echo      [OK] Donnees trouvees
) else (
    echo      [ATTENTION] Donnees manquantes - lancement du pipeline...
    python data\generate_data.py
    if errorlevel 1 (
        echo      [ERREUR] Generation echouee
        pause
        exit /b 1
    )
    python spark\oee_pipeline.py
    if errorlevel 1 (
        echo      [ERREUR] Pipeline echoue
        pause
        exit /b 1
    )
    echo      [OK] Donnees pretes
)
echo.

echo  [3] Recherche de Power BI Desktop...
set "PBI_EXE="

:: Chercher dans le Microsoft Store
for /f "usebackq delims=" %%a in (`powershell -Command "(Get-AppxPackage *PowerBI*).InstallLocation" 2^>nul`) do (
    if exist "%%a\bin\PBIDesktop.exe" set "PBI_EXE=%%a\bin\PBIDesktop.exe"
)

:: Chercher dans Program Files
if not defined PBI_EXE if exist "C:\Program Files\Microsoft Power BI Desktop\bin\PBIDesktop.exe" (
    set "PBI_EXE=C:\Program Files\Microsoft Power BI Desktop\bin\PBIDesktop.exe"
)
if not defined PBI_EXE if exist "C:\Program Files (x86)\Microsoft Power BI Desktop\bin\PBIDesktop.exe" (
    set "PBI_EXE=C:\Program Files (x86)\Microsoft Power BI Desktop\bin\PBIDesktop.exe"
)

if defined PBI_EXE (
    echo      [OK] Power BI Desktop trouve
    echo.
    echo  [4] Demarrage de Power BI Desktop...
    start "" "%PBI_EXE%"
    echo      [OK] Power BI Desktop lance
    echo.
    echo  ============================================================
    echo   IMPORT DANS POWER BI :
    echo  ============================================================
    echo   1. Allez dans : Accueil  >  Obtenir des donnees  >  Texte/CSV
    echo.
    echo   2. Selectionnez le fichier :
    echo      %~dp0data\processed\oee_results.csv
    echo.
    echo   3. Cliquez sur "Charger"
    echo.
    echo   4. Creez vos graphiques (barres, courbes, cartes KPI)
    echo  ============================================================
    echo.
) else (
    echo      [ATTENTION] Power BI Desktop non trouve
    echo      Telechargez : https://powerbi.microsoft.com/desktop/
    echo.
)

echo ===========================================================================
echo  Termine !
echo.
echo  Dashboard temps reel : http://localhost:8050
if defined PBI_EXE echo  Power BI Desktop   : Lance (import manuel du CSV)
echo ===========================================================================
echo.
pause
