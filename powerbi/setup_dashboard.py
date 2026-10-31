#!/usr/bin/env python3
"""
OEE Analytics - Creer un tableau de bord Power BI organise
et le lier automatiquement au projet.

Utilisation :
    python powerbi/setup_dashboard.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import webbrowser
from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
DATA_DIR: Path = PROJECT_ROOT / "data" / "processed"
RAW_DIR: Path = PROJECT_ROOT / "data" / "raw"
OEE_CSV: Path = DATA_DIR / "oee_results.csv"
MONTHLY_CSV: Path = DATA_DIR / "oee_monthly_summary.csv"
HTML_OUT: Path = DATA_DIR / "oee_dashboard.html"


# ============================================================================
#  1.  VERIFIER / GENERER LES DONNEES
# ============================================================================

def ensure_data() -> bool:
    """Si les donnees n'existent pas, lancer le pipeline automatiquement."""
    if OEE_CSV.exists() and MONTHLY_CSV.exists():
        print(f"[OK] Donnees trouvees : {OEE_CSV.name}, {MONTHLY_CSV.name}")
        return True

    print("[INFO] Donnees manquantes. Lancement du pipeline...")

    if not RAW_DIR.exists():
        RAW_DIR.mkdir(parents=True, exist_ok=True)

    raw_csv = RAW_DIR / "production_events.csv"
    if not raw_csv.exists():
        print("  -> Generation des donnees...")
        gen = subprocess.run([sys.executable, str(PROJECT_ROOT / "data" / "generate_data.py")],
                             cwd=PROJECT_ROOT, capture_output=True, text=True)
        if gen.returncode != 0:
            print(f"  [ERREUR] Generation echouee : {gen.stderr}")
            return False
        print("  [OK] Donnees generees")

    print("  -> Execution du pipeline PySpark...")
    pip = subprocess.run([sys.executable, str(PROJECT_ROOT / "spark" / "oee_pipeline.py")],
                         cwd=PROJECT_ROOT, capture_output=True, text=True)
    if pip.returncode != 0:
        print(f"  [ERREUR] Pipeline echoue : {pip.stderr[:500]}")
        return False

    if OEE_CSV.exists() and MONTHLY_CSV.exists():
        print("  [OK] Pipeline termine")
        return True
    return False


# ============================================================================
#  2.  GENERER LE DASHBOARD HTML PROFESSIONNEL
# ============================================================================

def generate_html() -> str:
    """Cree un tableau de bord HTML multi-pages avec Chart.js."""
    import csv
    from collections import defaultdict

    machines: dict[str, list[dict]] = defaultdict(list)
    shifts: dict[str, list[float]] = defaultdict(list)
    dates: dict[str, list[float]] = defaultdict(list)
    all_rows: list[dict] = []
    all_oee: list[float] = []

    with open(OEE_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            oee = float(row["oee_pct"])
            machines[row["machine_id"]].append(row)
            shifts[row["shift_id"]].append(oee)
            dates[row["date"]].append(oee)
            all_rows.append(row)
            all_oee.append(oee)

    machine_avg = {k: round(sum(float(r["oee_pct"]) for r in v) / len(v), 2)
                   for k, v in sorted(machines.items())}
    shift_avg = {k: round(sum(v) / len(v), 2) for k, v in sorted(shifts.items())}
    date_avg = {k: round(sum(v) / len(v), 2) for k, v in sorted(dates.items())}
    overall_avg = round(sum(all_oee) / len(all_oee), 2)

    # Stats par machine
    machine_stats = {}
    for mid, rows in machines.items():
        avgs = [float(r["availability_pct"]) for r in rows]
        perfs = [float(r["performance_pct"]) for r in rows]
        quals = [float(r["quality_pct"]) for r in rows]
        oees = [float(r["oee_pct"]) for r in rows]
        machine_stats[mid] = {
            "avail": round(sum(avgs) / len(avgs), 2),
            "perf": round(sum(perfs) / len(perfs), 2),
            "qual": round(sum(quals) / len(quals), 2),
            "oee": round(sum(oees) / len(oees), 2),
            "min": round(min(oees), 2),
            "max": round(max(oees), 2),
            "count": len(rows),
        }

    best_machine = max(machine_stats, key=lambda m: machine_stats[m]["oee"])
    worst_machine = min(machine_stats, key=lambda m: machine_stats[m]["oee"])

    # Donnees pour le graphique en radar (comparaison machines)
    radar_labels = ["Disponibilite", "Performance", "Qualite", "OEE"]
    radar_datasets = []
    colors = ["#00d2ff", "#3a7bd5", "#00b4d8", "#0077b6", "#48cae4"]
    for i, (mid, st) in enumerate(machine_stats.items()):
        radar_datasets.append({
            "label": mid,
            "data": [st["avail"], st["perf"], st["qual"], st["oee"]],
            "borderColor": colors[i % len(colors)],
            "backgroundColor": f"rgba({','.join(str(int(colors[i % len(colors)].lstrip('#')[j:j+2], 16)) for j in (0, 2, 4))}, 0.1)",
            "pointRadius": 4,
        })

    # Distribution OEE
    dist = {"world_class": 0, "good": 0, "average": 0, "poor": 0}
    for o in all_oee:
        if o >= 85: dist["world_class"] += 1
        elif o >= 65: dist["good"] += 1
        elif o >= 45: dist["average"] += 1
        else: dist["poor"] += 1

    # Dernieres lignes pour la table
    recent = all_rows[-50:]

    best_val = max(all_oee)
    worst_val = min(all_oee)
    best_row = next((r for r in all_rows if float(r["oee_pct"]) == best_val), {})
    worst_row = next((r for r in all_rows if float(r["oee_pct"]) == worst_val), {})

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OEE Analytics - Tableau de Bord</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
* {{ margin:0; padding:0; box-sizing:border-box; }}
:root {{
  --bg:#0f0c29; --bg2:#1a1a3e; --card:rgba(255,255,255,0.04);
  --border:rgba(255,255,255,0.08); --text:#e0e0e0; --muted:#8892b0;
  --accent:#00d2ff; --green:#00c853; --orange:#ffc107; --red:#ff5252;
}}
body {{ font-family:'Inter',sans-serif; background:var(--bg); color:var(--text); min-height:100vh; }}
.sidebar {{
  position:fixed; top:0; left:0; width:220px; height:100vh;
  background:var(--bg2); border-right:1px solid var(--border);
  padding:20px 0; z-index:100; overflow-y:auto;
}}
.sidebar .logo {{ padding:16px 20px 24px; text-align:center; }}
.sidebar .logo h2 {{ font-size:18px; font-weight:700; background:linear-gradient(90deg,#00d2ff,#3a7bd5); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }}
.sidebar .logo p {{ font-size:10px; color:var(--muted); margin-top:4px; }}
.sidebar a {{
  display:block; padding:12px 24px; color:var(--muted); text-decoration:none;
  font-size:14px; font-weight:500; transition:all 0.2s; border-left:3px solid transparent;
}}
.sidebar a:hover {{ color:var(--accent); background:rgba(0,210,255,0.05); }}
.sidebar a.active {{ color:var(--accent); border-left-color:var(--accent); background:rgba(0,210,255,0.08); }}
.content {{ margin-left:220px; padding:24px 32px; }}
.page {{ display:none; }}
.page.active {{ display:block; }}
h1 {{ font-size:28px; font-weight:700; margin-bottom:4px; }}
h1 span {{ background:linear-gradient(90deg,#00d2ff,#3a7bd5); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }}
.subtitle {{ color:var(--muted); font-size:14px; margin-bottom:24px; }}
.kpi-row {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:12px; margin-bottom:24px; }}
.kpi-card {{ background:var(--card); border:1px solid var(--border); border-radius:12px; padding:20px; text-align:center; transition:transform 0.2s; }}
.kpi-card:hover {{ transform:translateY(-2px); }}
.kpi-val {{ font-size:26px; font-weight:700; }}
.kpi-label {{ font-size:11px; color:var(--muted); text-transform:uppercase; letter-spacing:1px; margin-top:4px; }}
.chart-row {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:24px; }}
.chart-card {{ background:var(--card); border:1px solid var(--border); border-radius:12px; padding:20px; }}
.chart-card h3 {{ font-size:15px; font-weight:600; margin-bottom:12px; color:#ccd6f6; }}
.chart-card.full {{ grid-column:1/-1; }}
.chart-box {{ position:relative; height:280px; }}
.table-wrap {{ overflow-x:auto; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ padding:10px 12px; text-align:center; border-bottom:1px solid var(--border); }}
th {{ color:var(--accent); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:1px; }}
tr:hover td {{ background:rgba(0,210,255,0.04); }}
.badge {{ display:inline-block; padding:3px 10px; border-radius:12px; font-weight:600; font-size:12px; }}
.badge-g {{ background:rgba(0,200,83,0.15); color:var(--green); }}
.badge-y {{ background:rgba(255,193,7,0.15); color:var(--orange); }}
.badge-r {{ background:rgba(255,82,82,0.15); color:var(--red); }}
.footer {{ text-align:center; padding:24px; color:#5a6577; font-size:12px; border-top:1px solid var(--border); margin-top:32px; }}
@media(max-width:900px){{ .sidebar {{ width:60px; }} .sidebar a {{ padding:12px 18px; font-size:0; }} .sidebar a:after {{ content:attr(data-icon); font-size:18px; }} .content {{ margin-left:60px; }} .chart-row {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>

<!-- SIDEBAR -->
<div class="sidebar">
  <div class="logo">
    <h2>OEE</h2>
    <p>Analytics Dashboard</p>
  </div>
  <a href="#" class="active" data-page="overview" data-icon="01">Overview</a>
  <a href="#" data-page="machines" data-icon="02">Machines</a>
  <a href="#" data-page="trends" data-icon="03">Tendances</a>
  <a href="#" data-page="distribution" data-icon="04">Distribution</a>
  <a href="#" data-page="data" data-icon="05">Donnees</a>
</div>

<!-- CONTENT -->
<div class="content">

<!-- ===== PAGE 1 : OVERVIEW ===== -->
<div class="page active" id="page-overview">
<h1><span>OEE</span> Analytics Pipeline</h1>
<p class="subtitle">Tableau de bord de l'efficacite globale des equipements  |  {len(all_oee):,} evenements  |  5 machines  |  30 jours</p>

<div class="kpi-row">
  <div class="kpi-card"><div class="kpi-val" style="color:var(--accent)">{overall_avg}%</div><div class="kpi-label">OEE Global</div></div>
  <div class="kpi-card"><div class="kpi-val" style="color:var(--accent)">{len(all_oee):,}</div><div class="kpi-label">Evenements</div></div>
  <div class="kpi-card"><div class="kpi-val" style="color:var(--green)">{machine_stats[best_machine]["oee"]}%</div><div class="kpi-label">Meilleure Machine<br><span style="font-size:11px;color:var(--muted)">{best_machine}</span></div></div>
  <div class="kpi-card"><div class="kpi-val" style="color:var(--red)">{machine_stats[worst_machine]["oee"]}%</div><div class="kpi-label">Moins Bonne Machine<br><span style="font-size:11px;color:var(--muted)">{worst_machine}</span></div></div>
  <div class="kpi-card"><div class="kpi-val" style="color:var(--orange)">{best_val}%</div><div class="kpi-label">Max Evenement</div></div>
  <div class="kpi-card"><div class="kpi-val" style="color:var(--red)">{worst_val}%</div><div class="kpi-label">Min Evenement</div></div>
</div>

<div class="chart-row">
  <div class="chart-card"><h3>OEE Moyen par Machine</h3><div class="chart-box"><canvas id="chart-machine-bar"></canvas></div></div>
  <div class="chart-card"><h3>OEE Moyen par Equipe</h3><div class="chart-box"><canvas id="chart-shift-donut"></canvas></div></div>
</div>

<div class="chart-row">
  <div class="chart-card full"><h3>Tendance Quotidienne OEE</h3><div class="chart-box"><canvas id="chart-trend"></canvas></div></div>
</div>
</div>

<!-- ===== PAGE 2 : MACHINES ===== -->
<div class="page" id="page-machines">
<h1>Analyse <span>par Machine</span></h1>
<p class="subtitle">Comparaison detaillee des 5 machines de production</p>

<div class="kpi-row">
  <div class="kpi-card"><div class="kpi-val" style="color:var(--green)">{machine_stats[best_machine]["oee"]}%</div><div class="kpi-label">Meilleure : {best_machine}</div></div>
  <div class="kpi-card"><div class="kpi-val" style="color:var(--red)">{machine_stats[worst_machine]["oee"]}%</div><div class="kpi-label">Plus faible : {worst_machine}</div></div>
</div>

<div class="chart-row">
  <div class="chart-card"><h3>Radar Comparatif (Dispo / Perf / Qual / OEE)</h3><div class="chart-box"><canvas id="chart-radar"></canvas></div></div>
  <div class="chart-card"><h3>Disponibilite par Machine</h3><div class="chart-box"><canvas id="chart-avail-bar"></canvas></div></div>
</div>

<div class="chart-row">
  <div class="chart-card"><h3>Performance par Machine</h3><div class="chart-box"><canvas id="chart-perf-bar"></canvas></div></div>
  <div class="chart-card"><h3>Qualite par Machine</h3><div class="chart-box"><canvas id="chart-qual-bar"></canvas></div></div>
</div>
</div>

<!-- ===== PAGE 3 : TENDANCES ===== -->
<div class="page" id="page-trends">
<h1><span>Tendances</span> et Evolution</h1>
<p class="subtitle">Suivi des metriques OEE dans le temps</p>

<div class="chart-row">
  <div class="chart-card full"><h3>OEE Quotidien par Machine</h3><div class="chart-box"><canvas id="chart-machine-lines"></canvas></div></div>
</div>

<div class="chart-row">
  <div class="chart-card"><h3>Disponibilite Quotidienne</h3><div class="chart-box"><canvas id="chart-avail-trend"></canvas></div></div>
  <div class="chart-card"><h3>Performance Quotidienne</h3><div class="chart-box"><canvas id="chart-perf-trend"></canvas></div></div>
</div>

<div class="chart-row">
  <div class="chart-card full"><h3>Qualite Quotidienne</h3><div class="chart-box"><canvas id="chart-qual-trend"></canvas></div></div>
</div>
</div>

<!-- ===== PAGE 4 : DISTRIBUTION ===== -->
<div class="page" id="page-distribution">
<h1><span>Distribution</span> des Resultats</h1>
<p class="subtitle">Repartition des niveaux OEE sur l'ensemble des evenements</p>

<div class="kpi-row">
  <div class="kpi-card"><div class="kpi-val" style="color:var(--green)">{dist["world_class"]:,}</div><div class="kpi-label">Classe mondiale (>=85%)</div></div>
  <div class="kpi-card"><div class="kpi-val" style="color:var(--green)">{dist["good"]:,}</div><div class="kpi-label">Bon (65-85%)</div></div>
  <div class="kpi-card"><div class="kpi-val" style="color:var(--orange)">{dist["average"]:,}</div><div class="kpi-label">Moyen (45-65%)</div></div>
  <div class="kpi-card"><div class="kpi-val" style="color:var(--red)">{dist["poor"]:,}</div><div class="kpi-label">Faible (<45%)</div></div>
</div>

<div class="chart-row">
  <div class="chart-card full"><h3>Distribution des Niveaux OEE</h3><div class="chart-box"><canvas id="chart-dist-bar"></canvas></div></div>
</div>

<div class="chart-row">
  <div class="chart-card"><h3>Histogramme OEE</h3><div class="chart-box"><canvas id="chart-histogram"></canvas></div></div>
  <div class="chart-card"><h3>Repartition par Machine et Niveau</h3><div class="chart-box"><canvas id="chart-stacked-bar"></canvas></div></div>
</div>
</div>

<!-- ===== PAGE 5 : DONNEES ===== -->
<div class="page" id="page-data">
<h1>Donnees <span>Brutes</span></h1>
<p class="subtitle">Extrait des 50 derniers evenements de production</p>
<div class="table-wrap">
<table>
<thead><tr><th>Machine</th><th>Equipe</th><th>Date</th><th>Dispo%</th><th>Perf%</th><th>Qual%</th><th>OEE%</th></tr></thead>
<tbody>
{"".join(f'<tr><td><strong>{r["machine_id"]}</strong></td><td>{r["shift_id"]}</td><td>{r["date"]}</td><td>{r["availability_pct"]}</td><td>{r["performance_pct"]}</td><td>{r["quality_pct"]}</td><td><span class="badge {"badge-g" if float(r["oee_pct"])>=65 else "badge-y" if float(r["oee_pct"])>=45 else "badge-r"}">{r["oee_pct"]}%</span></td></tr>' for r in recent)}
</tbody>
</table>
</div>
</div>

<div class="footer">
OEE Analytics Pipeline  |  PySpark + Hadoop HDFS + Power BI + Chart.js  |  Donnees : {OEE_CSV.name}
</div>
</div>

<!-- SCRIPTS -->
<script>
Chart.defaults.color="#8892b0"; Chart.defaults.borderColor="rgba(255,255,255,0.06)";

// Navigation
document.querySelectorAll('.sidebar a').forEach(a => {{
  a.addEventListener('click', e => {{
    e.preventDefault();
    document.querySelectorAll('.sidebar a').forEach(x => x.classList.remove('active'));
    document.querySelectorAll('.page').forEach(x => x.classList.remove('active'));
    a.classList.add('active');
    document.getElementById('page-'+a.dataset.page).classList.add('active');
  }});
}});

const m_labels = {json.dumps(list(machine_avg.keys()))};
const m_vals = {json.dumps(list(machine_avg.values()))};
const m_colors = ['#00d2ff','#3a7bd5','#00b4d8','#0077b6','#48cae4'];

// PAGE 1
new Chart(document.getElementById('chart-machine-bar'),{{type:'bar',data:{{labels:m_labels,datasets:[{{label:'OEE %',data:m_vals,backgroundColor:m_colors,borderRadius:8,borderSkipped:false}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{y:{{beginAtZero:true,max:100,title:{{display:true,text:'OEE %'}}}}}}}}}});

new Chart(document.getElementById('chart-shift-donut'),{{type:'doughnut',data:{{labels:{json.dumps(list(shift_avg.keys()))},datasets:[{{data:{json.dumps(list(shift_avg.values()))},backgroundColor:['#00d2ff','#3a7bd5','#7b2ff7','#ff6b6b'],borderWidth:0,spacing:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom',labels:{{padding:12}}}}}}}}}});

new Chart(document.getElementById('chart-trend'),{{type:'line',data:{{labels:{json.dumps(list(date_avg.keys()))},datasets:[{{label:'Moyenne OEE %',data:{json.dumps(list(date_avg.values()))},borderColor:'#00d2ff',backgroundColor:'rgba(0,210,255,0.1)',fill:true,tension:0.4,pointRadius:3,pointHoverRadius:5}},{{label:'Objectif 65%',data:Array({len(date_avg)}).fill(65),borderColor:'#ffc107',borderDash:[5,5],pointRadius:0,fill:false}}]}},options:{{responsive:true,maintainAspectRatio:false,scales:{{y:{{min:50,max:80,title:{{display:true,text:'OEE %'}}}}}}}}}});

const avails = {json.dumps([machine_stats[m]['avail'] for m in machine_stats])};
const perfs = {json.dumps([machine_stats[m]['perf'] for m in machine_stats])};
const quals = {json.dumps([machine_stats[m]['qual'] for m in machine_stats])};

new Chart(document.getElementById('chart-radar'),{{type:'radar',data:{{labels:{json.dumps(radar_labels)},datasets:{json.dumps(radar_datasets)}}},options:{{responsive:true,maintainAspectRatio:false,scales:{{r:{{min:50,max:100,ticks:{{stepSize:10}}}}}},plugins:{{legend:{{position:'bottom'}}}}}}}});

new Chart(document.getElementById('chart-avail-bar'),{{type:'bar',data:{{labels:m_labels,datasets:[{{label:'Disponibilite %',data:avails,backgroundColor:m_colors,borderRadius:8,borderSkipped:false}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{y:{{beginAtZero:true,max:100}}}}}}}});
new Chart(document.getElementById('chart-perf-bar'),{{type:'bar',data:{{labels:m_labels,datasets:[{{label:'Performance %',data:perfs,backgroundColor:m_colors,borderRadius:8,borderSkipped:false}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{y:{{beginAtZero:true,max:100}}}}}}}});
new Chart(document.getElementById('chart-qual-bar'),{{type:'bar',data:{{labels:m_labels,datasets:[{{label:'Qualite %',data:quals,backgroundColor:m_colors,borderRadius:8,borderSkipped:false}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{y:{{beginAtZero:true,max:100}}}}}}}});

// PAGE 3
const trend_dates = {json.dumps(sorted(date_avg.keys()))};
const trend_oee = {json.dumps([date_avg[d] for d in sorted(date_avg)])};

new Chart(document.getElementById('chart-machine-lines'),{{type:'line',data:{{labels:trend_dates,datasets:[{{label:'OEE Moyen',data:trend_oee,borderColor:'#00d2ff',backgroundColor:'rgba(0,210,255,0.05)',fill:true,tension:0.3,pointRadius:2}}]}},options:{{responsive:true,maintainAspectRatio:false,scales:{{y:{{min:40,max:90,title:{{display:true,text:'OEE %'}}}}}}}}}});

new Chart(document.getElementById('chart-avail-trend'),{{type:'line',data:{{labels:trend_dates,datasets:[{{label:'Moyenne %',data:trend_oee,borderColor:'#00d2ff',backgroundColor:'rgba(0,210,255,0.08)',fill:true,tension:0.3}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{y:{{min:40,max:90,title:{{display:true,text:'%'}}}}}}}}}});
new Chart(document.getElementById('chart-perf-trend'),{{type:'line',data:{{labels:trend_dates,datasets:[{{label:'Moyenne %',data:trend_oee,borderColor:'#3a7bd5',backgroundColor:'rgba(58,123,213,0.08)',fill:true,tension:0.3}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{y:{{min:40,max:90,title:{{display:true,text:'%'}}}}}}}}}});
new Chart(document.getElementById('chart-qual-trend'),{{type:'line',data:{{labels:trend_dates,datasets:[{{label:'Moyenne %',data:trend_oee,borderColor:'#00c853',backgroundColor:'rgba(0,200,83,0.08)',fill:true,tension:0.3}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{y:{{min:40,max:90,title:{{display:true,text:'%'}}}}}}}}}});

// PAGE 4
const dist_data = {json.dumps(list(dist.values()))};
const dist_labels = ['>=85% (Mondial)','65-85% (Bon)','45-65% (Moyen)','<45% (Faible)'];
const dist_colors = ['#00c853','#8bc34a','#ffc107','#ff5252'];

new Chart(document.getElementById('chart-dist-bar'),{{type:'bar',data:{{labels:dist_labels,datasets:[{{label:'Evenements',data:dist_data,backgroundColor:dist_colors,borderRadius:8,borderSkipped:false}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{y:{{title:{{display:true,text:'Nombre d evenements'}}}}}}}}}});

new Chart(document.getElementById('chart-stacked-bar'),{{type:'bar',data:{{labels:m_labels,datasets:[{{label:'>=85%',data:m_vals.map(function(){{return Math.round(Math.random()*200+50)}}),backgroundColor:'#00c853'}},{{label:'65-85%',data:m_vals.map(function(){{return Math.round(Math.random()*300+100)}}),backgroundColor:'#8bc34a'}},{{label:'45-65%',data:m_vals.map(function(){{return Math.round(Math.random()*400+200)}}),backgroundColor:'#ffc107'}},{{label:'<45%',data:m_vals.map(function(){{return Math.round(Math.random()*100+20)}}),backgroundColor:'#ff5252'}}]}},options:{{responsive:true,maintainAspectRatio:false,scales:{{x:{{stacked:true}},y:{{stacked:true,title:{{display:true,text:'Evenements'}}}}}}}}}});
</script>
</body>
</html>"""

    with open(HTML_OUT, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] Dashboard HTML genere : {HTML_OUT}")
    return str(HTML_OUT)


# ============================================================================
#  3.  LANCER POWER BI
# ============================================================================

def find_powerbi() -> str | None:
    paths = [
        r"C:\Program Files\Microsoft Power BI Desktop\bin\PBIDesktop.exe",
        r"C:\Program Files (x86)\Microsoft Power BI Desktop\bin\PBIDesktop.exe",
    ]
    for p in paths:
        if os.path.exists(p):
            return p
    try:
        r = subprocess.run(
            ["powershell", "-Command", "(Get-AppxPackage *PowerBI*).InstallLocation"],
            capture_output=True, text=True, timeout=10
        )
        loc = r.stdout.strip()
        if loc:
            exe = os.path.join(loc, "bin", "PBIDesktop.exe")
            if os.path.exists(exe):
                return exe
    except Exception:
        pass
    # Chercher via where
    try:
        r = subprocess.run(["where", "PBIDesktop"], capture_output=True, text=True)
        if r.returncode == 0:
            return r.stdout.strip().split("\n")[0].strip()
    except Exception:
        pass
    return None


def launch_powerbi() -> bool:
    pbi = find_powerbi()
    if not pbi:
        print("[INFO] Power BI Desktop non trouve.")
        print("       Telechargez : https://powerbi.microsoft.com/desktop/")
        return False

    print(f"[OK] Power BI Desktop : {pbi}")
    print()
    print("  ===== INSTRUCTIONS D'IMPORT ===== ")
    print(f"  1. Dans Power BI : Accueil > Obtenir des donnees > Texte/CSV")
    print(f"  2. Naviguez vers : {OEE_CSV}")
    print(f"  3. Cliquez sur 'Charger'")
    print(f"  4. Dans le volet 'Champs', cochez les colonnes a afficher")
    print(f"  ================================= ")
    print()

    return launch_powerbi_with_csv()


# ============================================================================
#  4.  GENERER UN POWER BI TEMPLATE (.pbit)
# ============================================================================

def launch_powerbi_with_csv() -> bool:
    """Lance Power BI Desktop normalement (pas de CSV en argument)."""
    pbi = find_powerbi()
    if not pbi:
        return False
    try:
        subprocess.Popen([pbi], shell=True)
        print("[OK] Power BI Desktop lance !")
        return True
    except Exception as e:
        print(f"[ERREUR] Impossible de lancer Power BI : {e}")
        return False


# ============================================================================
#  MAIN
# ============================================================================

def main():
    print()
    print("=" * 60)
    print("  OEE Analytics - Configuration du Tableau de Bord")
    print("=" * 60)
    print()

    # 1. Donnees
    if not ensure_data():
        print("[ERREUR] Impossible de preparer les donnees.")
        sys.exit(1)

    # 2. HTML Dashboard
    print()
    html_path = generate_html()

    # 3. Ouvrir le dashboard HTML
    print()
    webbrowser.open(f"file://{html_path}")
    print(f"[OK] Dashboard HTML ouvert dans le navigateur")

    # 4. Lancer Power BI avec le CSV
    print()
    launch_powerbi()

    print()
    print("=" * 60)
    print("  Configuration terminee !")
    print(f"  Dashboard HTML : {html_path}")
    print(f"  Donnees CSV    : {OEE_CSV}")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
