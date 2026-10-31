#!/usr/bin/env python3
"""
OEE Analytics - Import automatique CSV dans Power BI Desktop.
Usage :  python powerbi/auto_import.py
"""

import os
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
OEE_CSV: Path = PROJECT_ROOT / "data" / "processed" / "oee_results.csv"


def find_powerbi() -> str | None:
    paths = [
        r"C:\Program Files\Microsoft Power BI Desktop\bin\PBIDesktop.exe",
        r"C:\Program Files (x86)\Microsoft Power BI Desktop\bin\PBIDesktop.exe",
    ]
    for p in paths:
        if os.path.exists(p):
            return p
    try:
        r = subprocess.run(["powershell", "-Command",
                            "(Get-AppxPackage *PowerBI*).InstallLocation"],
                           capture_output=True, text=True, timeout=10)
        loc = r.stdout.strip()
        if loc:
            exe = os.path.join(loc, "bin", "PBIDesktop.exe")
            if os.path.exists(exe):
                return exe
    except Exception:
        pass
    return None


def auto_import():
    pbi = find_powerbi()
    if not pbi:
        print("[ERREUR] Power BI Desktop introuvable.")
        print("  Telechargez : https://powerbi.microsoft.com/desktop/")
        input("\nAppuyez sur Entree pour fermer...")
        return

    if not OEE_CSV.exists():
        print(f"[ERREUR] Fichier CSV introuvable : {OEE_CSV}")
        print("  Lancez d'abord : python spark/oee_pipeline.py")
        input("\nAppuyez sur Entree pour fermer...")
        return

    print("=" * 60)
    print("  Import automatique OEE -> Power BI Desktop")
    print("=" * 60)
    print()
    print(f"  Fichier : {OEE_CSV.name}")
    print()

    # Tenter l'automation
    success = False
    try:
        from pywinauto import Desktop, Application, mouse
        from pywinauto.keyboard import send_keys

        # 0. Fermer Power BI s'il est deja ouvert
        subprocess.run(["taskkill", "/f", "/im", "PBIDesktop.exe"],
                       capture_output=True, text=True)
        time.sleep(3)

        # 1. Lancer Power BI
        print(" [1/6] Demarrage de Power BI Desktop...")
        app = Application(backend="uia")
        app.start(pbi, timeout=45)
        time.sleep(5)

        # 2. Attendre la fenetre principale
        print(" [2/6] Attente de l'interface...")
        main_dlg = None
        for _ in range(30):
            try:
                for w in Desktop(backend="uia").windows():
                    txt = (w.window_text() or "").lower()
                    if "power bi" in txt:
                        main_dlg = w
                        break
            except Exception:
                pass
            if main_dlg:
                break
            time.sleep(1)

        if not main_dlg:
            raise RuntimeError("Fenetre Power BI non detectee")

        print(f"       Fenetre : {main_dlg.window_text()}")
        time.sleep(5)

        # 3. Declencher "Obtenir des donnees" via Alt+H (Home) puis navigation
        print(" [3/6] Activation du ruban Accueil...")
        send_keys("%h")  # Alt+H = Home tab
        time.sleep(2)

        # Tab 4-5 fois pour atteindre "Obtenir des donnees"
        for _ in range(6):
            send_keys("{TAB}")
            time.sleep(0.2)

        send_keys(" ")  # Espace pour activer
        time.sleep(2)

        # 4. Choisir "Texte/CSV" dans la liste
        print(" [4/6] Selection de Texte/CSV...")
        send_keys("csv")  # Tape "csv" pour filtrer
        time.sleep(1)
        send_keys("{ENTER}")  # Selectionne le premier resultat
        time.sleep(2)

        # 5. Fenetre "Ouvrir" -> entrer le chemin du fichier
        print(" [5/6] Navigation vers le fichier...")

        # Essayer differents patterns de boites de dialogue
        file_dlg = None
        for _ in range(10):
            for w in Desktop(backend="uia").windows():
                txt = (w.window_text() or "").lower()
                if "ouvrir" in txt or "open" in txt:
                    file_dlg = w
                    break
            if file_dlg:
                break
            time.sleep(1)

        if file_dlg:
            # Entrer le chemin dans le champ "Nom du fichier"
            try:
                file_path_box = file_dlg.descendants(control_type="Edit")
                for eb in file_path_box:
                    lbl = (eb.element_info.name or "").lower()
                    if "nom" in lbl or "file" in lbl or "name" in lbl:
                        eb.set_text(str(OEE_CSV))
                        time.sleep(1)
                        send_keys("{ENTER}")
                        break
                else:
                    # Fallback : Ctrl+V direct
                    subprocess.run(["powershell", "-Command",
                        "Set-Clipboard -Value '" + str(OEE_CSV) + "'"],
                        capture_output=True)  
                    send_keys("^v")
                    time.sleep(1)
                    send_keys("{ENTER}")
            except Exception:
                send_keys(str(OEE_CSV))
                time.sleep(1)
                send_keys("{ENTER}")
        else:
            # Fallback : send_keys directement
            time.sleep(1)
            send_keys(str(OEE_CSV))
            time.sleep(1)
            send_keys("{ENTER}")

        time.sleep(5)

        # 6. Cliquer sur "Charger"
        print(" [6/6] Import en cours...")
        load_found = False
        for _ in range(15):
            for dlg in Desktop(backend="uia").windows():
                try:
                    for ctrl in dlg.descendants():
                        txt = (ctrl.window_text() or "").lower()
                        if txt in ("charger", "load"):
                            ctrl.invoke()
                            load_found = True
                            break
                except Exception:
                    pass
                if load_found:
                    break
            if load_found:
                break
            time.sleep(1)

        if load_found:
            print("       Import lance avec succes !")
            success = True
        else:
            print("       Bouton 'Charger' non detecte")
            time.sleep(2)

    except ImportError:
        print(" [INFO] pywinauto non disponible, import manuel...")
    except Exception as e:
        print(f" [INFO] Automation non disponible : {e}")

    if not success:
        # Fallback : lancer Power BI et afficher les instructions
        try:
            subprocess.Popen([pbi], shell=True)
            print()
            print("  ===== IMPORT MANUEL ===== ")
            print(f"  1. Power BI Desktop est ouvert")
            print(f"  2. Allez dans : Accueil > Obtenir des donnees > Texte/CSV")
            print(f"  3. Selectionnez :")
            print(f"     {OEE_CSV}")
            print(f"  4. Cliquez sur 'Charger'")
            print("  ========================= ")
        except Exception as e:
            print(f"  [ERREUR] Impossible de lancer Power BI : {e}")

    print()
    print("=" * 60)
    print("  Termine !")
    print("=" * 60)
    print()
    input("Appuyez sur Entree pour fermer...")


if __name__ == "__main__":
    auto_import()
