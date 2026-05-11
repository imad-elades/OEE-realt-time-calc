#!/usr/bin/env python3
"""
OEE Analytics - Générateur de Tableau de Bord Power BI.

Crée un jeu de données prêt pour Power BI et ouvre Power BI Desktop automatiquement.
Peut également générer un tableau de bord HTML interactif comme solution de repli autonome.

Utilisation :
    python powerbi/create_dashboard.py          # Ouvrir Power BI avec les données
    python powerbi/create_dashboard.py --html    # Générer seulement le tableau de bord HTML
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
DATA_DIR: Path = PROJECT_ROOT / "data" / "processed"
OEE_CSV: Path = DATA_DIR / "oee_results.csv"
MONTHLY_CSV: Path = DATA_DIR / "oee_monthly_summary.csv"
POWERBI_EXE: str = (
    r"C:\Program Files\WindowsApps\Microsoft.MicrosoftPowerBIDesktop"
    r"_2.152.1279.0_x64__8wekyb3d8bbwe\bin\PBIDesktop.exe"
)


def verify_data_files() -> bool:
    """Vérifier que les fichiers de sortie du pipeline existent.

    Returns:
        True si tous les fichiers requis sont présents.
    """
    missing: list[str] = []
    for path in [OEE_CSV, MONTHLY_CSV]:
        if not path.exists():
            missing.append(str(path))

    if missing:
        print("[ERREUR] Fichiers de données manquants :")
        for m in missing:
            print(f"  - {m}")
        print("\nExécutez d'abord le pipeline : python spark/oee_pipeline.py")
        return False

    print(f"[OK] Résultats OEE : {OEE_CSV} ({OEE_CSV.stat().st_size / 1024:.1f} Ko)")
    print(f"[OK] Résumé mensuel : {MONTHLY_CSV}")
    return True


def find_powerbi() -> Optional[str]:
    """Trouver l'exécutable Power BI Desktop.

    Returns:
        Chemin vers PBIDesktop.exe ou None si introuvable.
    """
    # Check configured path
    if os.path.exists(POWERBI_EXE):
        return POWERBI_EXE

    # Recherche via AppxPackage
    try:
        result = subprocess.run(
            [
                "powershell", "-Command",
                "(Get-AppxPackage *PowerBI*).InstallLocation"
            ],
            capture_output=True, text=True, timeout=10
        )
        install_loc = result.stdout.strip()
        if install_loc:
            exe = os.path.join(install_loc, "bin", "PBIDesktop.exe")
            if os.path.exists(exe):
                return exe
    except Exception:
        pass

    # Emplacements d'installation standard
    standard_paths = [
        r"C:\Program Files\Microsoft Power BI Desktop\bin\PBIDesktop.exe",
        r"C:\Program Files (x86)\Microsoft Power BI Desktop\bin\PBIDesktop.exe",
    ]
    for p in standard_paths:
        if os.path.exists(p):
            return p

    return None


def open_powerbi() -> None:
    """Ouvrir Power BI Desktop.

    Lance Power BI Desktop pour que l'utilisateur puisse importer les données CSV.
    """
    pbi_path = find_powerbi()
    if not pbi_path:
        print("[ATTENTION] Power BI Desktop introuvable sur cette machine.")
        print("Téléchargement : https://powerbi.microsoft.com/desktop/")
        print(f"\nVous pouvez ouvrir manuellement : {OEE_CSV}")
        return

    print(f"[OK] Power BI trouvé : {pbi_path}")
    print("     Lancement de Power BI Desktop...")
    print(f"\n     === INSTRUCTIONS D'IMPORT ===")
    print(f"     1. Dans Power BI, cliquez : Accueil > Obtenir des données > Texte/CSV")
    print(f"     2. Naviguez vers : {OEE_CSV}")
    print(f"     3. Cliquez sur 'Charger' pour importer")
    print(f"     =============================\n")

    try:
        subprocess.Popen([pbi_path], shell=False)
        print("[OK] Power BI Desktop lancé !")
    except Exception as e:
        print(f"[ERREUR] Échec du lancement de Power BI : {e}")
        # Essayer via le protocole shell
        try:
            os.startfile(str(OEE_CSV))
            print("[OK] Fichier CSV ouvert avec l'application par défaut")
        except Exception:
            print(f"[INFO] Ouvrir manuellement : {OEE_CSV}")


def generate_html_dashboard() -> None:
    """Générer un tableau de bord HTML autonome avec des graphiques OEE.

    Crée un fichier HTML interactif utilisant Chart.js qui visualise
    les données OEE sans nécessiter Power BI.
    """
    import csv
    from collections import defaultdict

    # Lire les données OEE
    machines: dict[str, list[float]] = defaultdict(list)
    shifts: dict[str, list[float]] = defaultdict(list)
    dates: dict[str, list[float]] = defaultdict(list)
    all_oee: list[float] = []

    with open(OEE_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            oee = float(row["oee_pct"])
            machines[row["machine_id"]].append(oee)
            shifts[row["shift_id"]].append(oee)
            dates[row["date"]].append(oee)
            all_oee.append(oee)

    # Compute averages
    machine_avg = {k: round(sum(v) / len(v), 2) for k, v in sorted(machines.items())}
    shift_avg = {k: round(sum(v) / len(v), 2) for k, v in sorted(shifts.items())}
    date_avg = {k: round(sum(v) / len(v), 2) for k, v in sorted(dates.items())}
    overall_avg = round(sum(all_oee) / len(all_oee), 2)

    # Read monthly summary
    monthly_data: list[dict[str, str]] = []
    with open(MONTHLY_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            monthly_data.append(row)

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Tableau de Bord OEE Analytics</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        body {{
            font-family: 'Inter', sans-serif;
            background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
            color: #e0e0e0;
            min-height: 100vh;
            padding: 24px;
        }}

        .header {{
            text-align: center;
            padding: 32px 0;
            margin-bottom: 24px;
        }}

        .header h1 {{
            font-size: 2.5rem;
            font-weight: 700;
            background: linear-gradient(90deg, #00d2ff, #3a7bd5, #00d2ff);
            background-size: 200% auto;
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            animation: shimmer 3s linear infinite;
        }}

        @keyframes shimmer {{
            to {{ background-position: 200% center; }}
        }}

        .header p {{
            color: #8892b0;
            font-size: 1.1rem;
            margin-top: 8px;
        }}

        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 32px;
        }}

        .kpi-card {{
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 16px;
            padding: 24px;
            text-align: center;
            transition: transform 0.3s, box-shadow 0.3s;
        }}

        .kpi-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 8px 32px rgba(0, 210, 255, 0.15);
        }}

        .kpi-value {{
            font-size: 2.2rem;
            font-weight: 700;
            color: #00d2ff;
        }}

        .kpi-label {{
            font-size: 0.85rem;
            color: #8892b0;
            margin-top: 4px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 24px;
            margin-bottom: 32px;
        }}

        .chart-card {{
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 16px;
            padding: 24px;
        }}

        .chart-card h3 {{
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 16px;
            color: #ccd6f6;
        }}

        .chart-container {{
            position: relative;
            height: 300px;
        }}

        .table-card {{
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 16px;
            padding: 24px;
            overflow-x: auto;
        }}

        .table-card h3 {{
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 16px;
            color: #ccd6f6;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
        }}

        th, td {{
            padding: 12px 16px;
            text-align: center;
            border-bottom: 1px solid rgba(255, 255, 255, 0.06);
        }}

        th {{
            color: #00d2ff;
            font-weight: 600;
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        td {{ color: #ccd6f6; }}

        tr:hover td {{ background: rgba(0, 210, 255, 0.05); }}

        .oee-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 0.9rem;
        }}

        .oee-good {{ background: rgba(0, 200, 83, 0.2); color: #00c853; }}
        .oee-avg {{ background: rgba(255, 193, 7, 0.2); color: #ffc107; }}
        .oee-poor {{ background: rgba(255, 82, 82, 0.2); color: #ff5252; }}

        .footer {{
            text-align: center;
            padding: 24px;
            color: #5a6577;
            font-size: 0.85rem;
        }}
    </style>
</head>
<body>

<div class="header">
    <h1>Tableau de Bord OEE Analytics</h1>
    <p>Efficacité Globale des Équipements -- 5 Machines | 4 Équipes | 30 Jours | {len(all_oee):,} Événements</p>
</div>

<div class="kpi-grid">
    <div class="kpi-card">
        <div class="kpi-value">{overall_avg}%</div>
        <div class="kpi-label">OEE Global</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-value">{len(all_oee):,}</div>
        <div class="kpi-label">Événements de Production</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-value">{max(machine_avg.values())}%</div>
        <div class="kpi-label">Meilleure Machine OEE</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-value">{min(machine_avg.values())}%</div>
        <div class="kpi-label">Moins Bonne Machine OEE</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-value">{round(min(all_oee), 1)}%</div>
        <div class="kpi-label">Min d'un Événement</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-value">{round(max(all_oee), 1)}%</div>
        <div class="kpi-label">Max d'un Événement</div>
    </div>
</div>

<div class="charts-grid">
    <div class="chart-card">
        <h3>OEE Moyen par Machine</h3>
        <div class="chart-container">
            <canvas id="machineChart"></canvas>
        </div>
    </div>
    <div class="chart-card">
        <h3>OEE Moyen par Équipe</h3>
        <div class="chart-container">
            <canvas id="shiftChart"></canvas>
        </div>
    </div>
    <div class="chart-card" style="grid-column: 1 / -1;">
        <h3>Tendance OEE dans le Temps (Moyenne Quotidienne)</h3>
        <div class="chart-container">
            <canvas id="trendChart"></canvas>
        </div>
    </div>
</div>

<div class="table-card">
    <h3>Résumé OEE Mensuel par Machine</h3>
    <table>
        <thead>
            <tr>
                <th>Machine</th>
                <th>Dispo. Moy.</th>
                <th>Perf. Moy.</th>
                <th>Qual. Moy.</th>
                <th>OEE Moy.</th>
                <th>Événements</th>
                <th>OEE Min</th>
                <th>OEE Max</th>
            </tr>
        </thead>
        <tbody>
            {"".join(f'''<tr>
                <td><strong>{row.get("machine_id", "")}</strong></td>
                <td>{row.get("avg_availability_pct", "")}%</td>
                <td>{row.get("avg_performance_pct", "")}%</td>
                <td>{row.get("avg_quality_pct", "")}%</td>
                <td><span class="oee-badge {"oee-good" if float(row.get("avg_oee_pct", 0)) >= 65 else "oee-avg" if float(row.get("avg_oee_pct", 0)) >= 55 else "oee-poor"}">{row.get("avg_oee_pct", "")}%</span></td>
                <td>{row.get("event_count", "")}</td>
                <td>{row.get("min_oee_pct", "")}%</td>
                <td>{row.get("max_oee_pct", "")}%</td>
            </tr>''' for row in monthly_data)}
        </tbody>
    </table>
</div>

<div class="footer">
    Pipeline OEE Analytics -- PySpark + Power BI | Données : {OEE_CSV.name}
</div>

<script>
    Chart.defaults.color = '#8892b0';
    Chart.defaults.borderColor = 'rgba(255,255,255,0.06)';

    // Machine Chart
    new Chart(document.getElementById('machineChart'), {{
        type: 'bar',
        data: {{
            labels: {json.dumps(list(machine_avg.keys()))},
            datasets: [{{
                label: 'Moy. OEE %',
                data: {json.dumps(list(machine_avg.values()))},
                backgroundColor: ['#00d2ff', '#3a7bd5', '#00b4d8', '#0077b6', '#48cae4'],
                borderRadius: 8,
                borderSkipped: false,
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{
                y: {{ beginAtZero: true, max: 100, title: {{ display: true, text: 'OEE %' }} }}
            }}
        }}
    }});

    // Shift Chart
    new Chart(document.getElementById('shiftChart'), {{
        type: 'doughnut',
        data: {{
            labels: {json.dumps(list(shift_avg.keys()))},
            datasets: [{{
                data: {json.dumps(list(shift_avg.values()))},
                backgroundColor: ['#00d2ff', '#3a7bd5', '#7b2ff7', '#ff6b6b'],
                borderWidth: 0,
                spacing: 4,
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
                legend: {{ position: 'bottom', labels: {{ padding: 16 }} }}
            }}
        }}
    }});

    // Trend Chart
    new Chart(document.getElementById('trendChart'), {{
        type: 'line',
        data: {{
            labels: {json.dumps(list(date_avg.keys()))},
            datasets: [{{
                label: 'Moy. Quotidienne OEE %',
                data: {json.dumps(list(date_avg.values()))},
                borderColor: '#00d2ff',
                backgroundColor: 'rgba(0, 210, 255, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 4,
                pointHoverRadius: 6,
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{
                y: {{ beginAtZero: false, min: 50, max: 80, title: {{ display: true, text: 'OEE %' }} }},
                x: {{ title: {{ display: true, text: 'Date' }}, ticks: {{ maxRotation: 45 }} }}
            }}
        }}
    }});
</script>

</body>
</html>"""

    dashboard_path = PROJECT_ROOT / "data" / "processed" / "oee_dashboard.html"
    with open(dashboard_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] Tableau de bord généré : {dashboard_path}")
    return str(dashboard_path)


def main() -> None:
    """Point d'entrée : vérifier les données, générer le tableau de bord, ouvrir Power BI."""
    parser = argparse.ArgumentParser(description="Tableau de Bord Power BI OEE")
    parser.add_argument("--html", action="store_true", help="Générer seulement le tableau de bord HTML")
    parser.add_argument("--no-open", action="store_true", help="Ne pas ouvrir Power BI automatiquement")
    args = parser.parse_args()

    print("=" * 60)
    print("  OEE Analytics - Intégration Power BI")
    print("=" * 60)

    if not verify_data_files():
        sys.exit(1)

    # Toujours générer le tableau de bord HTML
    dashboard_path = generate_html_dashboard()

    if args.html:
        # Ouvrir le tableau de bord HTML dans le navigateur
        os.startfile(str(dashboard_path))
        print("[OK] Tableau de bord HTML ouvert dans le navigateur")
    elif not args.no_open:
        # Ouvrir Power BI Desktop
        open_powerbi()
        # Ouvrir également le tableau de bord HTML
        os.startfile(str(dashboard_path))
        print("[OK] Tableau de bord HTML également ouvert dans le navigateur")

    print("=" * 60)
    print("  Tableau de bord prêt !")
    print("=" * 60)


if __name__ == "__main__":
    main()
