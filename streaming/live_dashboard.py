#!/usr/bin/env python3
"""
OEE Analytics - Serveur de Tableau de Bord en Direct (sans WebSocket).

Sert un tableau de bord d'une seule page qui utilise le polling fetch() pour
mettre à jour les graphiques et les données sans rechargement complet de la page.

Points de terminaison :
    GET /           -> HTML du tableau de bord (chargé une fois)
    GET /api/data   -> JSON avec les dernières données de streaming (interrogé toutes les 2s)

Utilisation :
    python streaming/live_dashboard.py              # Démarrer sur le port 8050
    python streaming/live_dashboard.py --port 8050  # Port personnalisé
"""

from __future__ import annotations

import argparse
import glob
import json
import os
from collections import defaultdict
from datetime import datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
STREAMING_INPUT: Path = PROJECT_ROOT / "data" / "streaming" / "input"


def build_api_data(max_events: int = 200) -> dict[str, Any]:
    """Construire la charge utile JSON à partir des derniers événements de streaming.

    Args:
        max_events: Nombre maximum d'événements à lire.

    Returns:
        Dictionnaire avec toutes les données du tableau de bord.
    """
    events: list[dict[str, Any]] = []
    pattern = str(STREAMING_INPUT / "event_*.json")
    files = sorted(glob.glob(pattern), reverse=True)[:max_events]

    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                events.append(json.load(f))
        except (json.JSONDecodeError, IOError):
            continue

    if not events:
        return {"empty": True, "total_events": 0}

    all_oee = [e.get("oee_pct", 0) for e in events]
    machine_data: dict[str, list[float]] = defaultdict(list)
    for e in events:
        machine_data[e.get("machine_id", "?")].append(e.get("oee_pct", 0))

    machine_avg = {k: round(sum(v) / len(v), 2) for k, v in sorted(machine_data.items())}
    timeline = [{"id": e.get("event_id", 0), "oee": e.get("oee_pct", 0)} for e in events[:60]]
    timeline.reverse()

    feed = []
    for e in events[:15]:
        feed.append({
            "id": e.get("event_id", 0),
            "ts": e.get("timestamp", "")[:19],
            "machine": e.get("machine_id", ""),
            "shift": e.get("shift_id", ""),
            "avail": e.get("availability_pct", 0),
            "perf": e.get("performance_pct", 0),
            "qual": e.get("quality_pct", 0),
            "oee": e.get("oee_pct", 0),
        })

    latest = events[0]
    return {
        "empty": False,
        "total_events": len(events),
        "overall_oee": round(sum(all_oee) / len(all_oee), 2),
        "best_oee": round(max(all_oee), 1),
        "worst_oee": round(min(all_oee), 1),
        "last_machine": latest.get("machine_id", "-"),
        "last_oee": latest.get("oee_pct", 0),
        "machine_labels": list(machine_avg.keys()),
        "machine_values": list(machine_avg.values()),
        "timeline_labels": [t["id"] for t in timeline],
        "timeline_values": [t["oee"] for t in timeline],
        "feed": feed,
        "updated_at": datetime.now().strftime("%H:%M:%S"),
    }


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tableau de Bord OEE en Direct</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;900&display=swap');
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Inter',sans-serif;background:linear-gradient(135deg,#0a0a1a 0%,#1a1a3e 40%,#0d0d2b 100%);color:#e0e0e0;min-height:100vh;padding:20px}
.header{display:flex;justify-content:space-between;align-items:center;padding:16px 24px;margin-bottom:20px;background:rgba(255,255,255,0.03);border-radius:16px;border:1px solid rgba(255,255,255,0.06)}
.header h1{font-size:1.8rem;font-weight:700;background:linear-gradient(90deg,#00ff87,#60efff);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.live-badge{display:flex;align-items:center;gap:8px;background:rgba(0,255,135,0.1);padding:8px 16px;border-radius:20px;border:1px solid rgba(0,255,135,0.3)}
.live-dot{width:10px;height:10px;background:#00ff87;border-radius:50%;animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(1.3)}}
.status-text{color:#6a7a8a;font-size:.85rem}
.kpi-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:20px}
.kpi{background:rgba(255,255,255,0.04);backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:16px;text-align:center;transition:all .3s}
.kpi:hover{transform:translateY(-2px);box-shadow:0 4px 20px rgba(0,255,135,0.1)}
.kpi-value{font-size:1.8rem;font-weight:900;transition:color .5s}
.green{color:#00ff87}.blue{color:#60efff}.yellow{color:#ffd700}.red{color:#ff5252}
.kpi-label{font-size:.7rem;color:#6a7a8a;margin-top:4px;text-transform:uppercase;letter-spacing:1.5px}
.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px}
.card{background:rgba(255,255,255,0.04);backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:20px}
.card h3{font-size:.95rem;font-weight:600;color:#8892b0;margin-bottom:12px;text-transform:uppercase;letter-spacing:1px}
.chart-box{height:240px;position:relative}
.feed-card{grid-column:1/-1}
table{width:100%;border-collapse:collapse;font-size:.85rem}
th,td{padding:8px 12px;text-align:center;border-bottom:1px solid rgba(255,255,255,0.04)}
th{color:#60efff;font-weight:600;font-size:.75rem;text-transform:uppercase;letter-spacing:1px}
tr{transition:background .3s}
tr:hover td{background:rgba(0,255,135,0.03)}
.oee-badge{display:inline-block;padding:3px 10px;border-radius:12px;font-weight:700;font-size:.85rem;transition:all .3s}
.oee-good{background:rgba(0,255,135,0.15);color:#00ff87}
.oee-avg{background:rgba(255,215,0,0.15);color:#ffd700}
.oee-poor{background:rgba(255,82,82,0.15);color:#ff5252}
.footer{text-align:center;padding:16px;color:#3a4a5a;font-size:.75rem}
.fade-in{animation:fadeIn .5s ease}
@keyframes fadeIn{from{opacity:.3}to{opacity:1}}
.waiting{display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:60vh;text-align:center}
.waiting h2{font-size:1.5rem;color:#60efff;margin-bottom:16px}
.spinner{width:50px;height:50px;border:4px solid rgba(96,239,255,0.2);border-top-color:#60efff;border-radius:50%;animation:spin 1s linear infinite;margin:24px 0}
@keyframes spin{to{transform:rotate(360deg)}}
code{background:rgba(255,255,255,0.1);padding:4px 12px;border-radius:6px;font-size:.9rem;color:#00ff87}
</style>
</head>
<body>

<div class="header">
    <h1>Tableau de Bord OEE en Direct</h1>
    <div class="status-text" id="statusText">Connexion...</div>
    <div class="live-badge">
        <div class="live-dot" id="liveDot"></div>
        <span style="color:#00ff87;font-weight:600;font-size:.85rem" id="liveLabel">EN DIRECT</span>
    </div>
</div>

<div id="waitingScreen" class="waiting" style="display:none">
    <h2>En attente d'événements...</h2>
    <div class="spinner"></div>
    <p style="color:#6a7a8a">Lancez le producteur :</p>
    <p style="margin-top:8px"><code>python streaming/producer.py</code></p>
</div>

<div id="dashboard" style="display:none">
    <div class="kpi-grid">
        <div class="kpi"><div class="kpi-value green" id="kpiOee">-</div><div class="kpi-label">OEE Global</div></div>
        <div class="kpi"><div class="kpi-value blue" id="kpiEvents">0</div><div class="kpi-label">Total Événements</div></div>
        <div class="kpi"><div class="kpi-value green" id="kpiLastMachine">-</div><div class="kpi-label">Dernière Machine</div></div>
        <div class="kpi"><div class="kpi-value" id="kpiLastOee">-</div><div class="kpi-label">Dernier OEE</div></div>
        <div class="kpi"><div class="kpi-value green" id="kpiBest">-</div><div class="kpi-label">Meilleur Événement</div></div>
        <div class="kpi"><div class="kpi-value red" id="kpiWorst">-</div><div class="kpi-label">Pire Événement</div></div>
    </div>

    <div class="grid-2">
        <div class="card">
            <h3>OEE par Machine (Moyenne en Direct)</h3>
            <div class="chart-box"><canvas id="machineChart"></canvas></div>
        </div>
        <div class="card">
            <h3>Chronologie OEE Récente</h3>
            <div class="chart-box"><canvas id="timelineChart"></canvas></div>
        </div>
    </div>

    <div class="grid-2">
        <div class="card feed-card">
            <h3>Flux d'Événements en Direct</h3>
            <table>
                <thead><tr><th>ID</th><th>Horodatage</th><th>Machine</th><th>Équipe</th><th>Dispo</th><th>Perf</th><th>Qualité</th><th>OEE</th></tr></thead>
                <tbody id="feedBody"></tbody>
            </table>
        </div>
    </div>
</div>

<div class="footer">OEE Analytics - Streaming en Temps Réel | Mises à jour via AJAX</div>

<script>
Chart.defaults.color = '#6a7a8a';
Chart.defaults.borderColor = 'rgba(255,255,255,0.04)';

let machineChart = null;
let timelineChart = null;
let prevEventCount = 0;

function initCharts() {
    machineChart = new Chart(document.getElementById('machineChart'), {
        type: 'bar',
        data: { labels: [], datasets: [{ label: 'Moy. OEE %', data: [],
            backgroundColor: ['#00ff87','#60efff','#7b61ff','#ffd700','#ff6b6b'],
            borderRadius: 6, borderSkipped: false }] },
        options: { responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true, max: 100 } },
            animation: { duration: 600, easing: 'easeOutQuart' } }
    });

    timelineChart = new Chart(document.getElementById('timelineChart'), {
        type: 'line',
        data: { labels: [], datasets: [
            { label: 'OEE %', data: [], borderColor: '#00ff87',
              backgroundColor: 'rgba(0,255,135,0.08)', fill: true,
              tension: 0.3, pointRadius: 2, pointHoverRadius: 5, borderWidth: 2 },
            { label: 'Objectif 65%', data: [], borderColor: 'rgba(255,215,0,0.4)',
              borderDash: [5,5], borderWidth: 1, pointRadius: 0, fill: false }
        ] },
        options: { responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: true, position: 'top', labels: { boxWidth: 12 } } },
            scales: { y: { min: 20, max: 100 }, x: { title: { display: true, text: 'Event #' } } },
            animation: { duration: 400 } }
    });
}

function oeeClass(val) {
    if (val >= 70) return 'green';
    if (val >= 50) return 'yellow';
    return 'red';
}

function badgeClass(val) {
    if (val >= 70) return 'oee-good';
    if (val >= 50) return 'oee-avg';
    return 'oee-poor';
}

function updateDashboard(data) {
    if (data.empty) {
        document.getElementById('waitingScreen').style.display = 'flex';
        document.getElementById('dashboard').style.display = 'none';
        return;
    }

    document.getElementById('waitingScreen').style.display = 'none';
    document.getElementById('dashboard').style.display = 'block';
    document.getElementById('statusText').textContent =
        'Last update: ' + data.updated_at + ' | ' + data.total_events + ' events';

    // KPIs — smooth text updates
    document.getElementById('kpiOee').textContent = data.overall_oee + '%';
    document.getElementById('kpiEvents').textContent = data.total_events;
    document.getElementById('kpiLastMachine').textContent = data.last_machine;
    const lastOeeEl = document.getElementById('kpiLastOee');
    lastOeeEl.textContent = data.last_oee + '%';
    lastOeeEl.className = 'kpi-value ' + oeeClass(data.last_oee);
    document.getElementById('kpiBest').textContent = data.best_oee + '%';
    document.getElementById('kpiWorst').textContent = data.worst_oee + '%';

    // Flash animation on new events
    if (data.total_events !== prevEventCount) {
        document.getElementById('kpiEvents').classList.remove('fade-in');
        void document.getElementById('kpiEvents').offsetWidth;
        document.getElementById('kpiEvents').classList.add('fade-in');
        prevEventCount = data.total_events;
    }

    // Machine chart — update data in place
    machineChart.data.labels = data.machine_labels;
    machineChart.data.datasets[0].data = data.machine_values;
    machineChart.update('none');

    // Timeline chart
    timelineChart.data.labels = data.timeline_labels;
    timelineChart.data.datasets[0].data = data.timeline_values;
    timelineChart.data.datasets[1].data = new Array(data.timeline_labels.length).fill(65);
    timelineChart.update('none');

    // Feed table — rebuild rows smoothly
    const tbody = document.getElementById('feedBody');
    let html = '';
    for (const e of data.feed) {
        html += '<tr><td>#' + e.id + '</td><td>' + e.ts + '</td>'
            + '<td><strong>' + e.machine + '</strong></td><td>' + e.shift + '</td>'
            + '<td>' + e.avail + '%</td><td>' + e.perf + '%</td>'
            + '<td>' + e.qual + '%</td>'
            + '<td><span class="oee-badge ' + badgeClass(e.oee) + '">' + e.oee + '%</span></td></tr>';
    }
    tbody.innerHTML = html;
}

async function poll() {
    try {
        const resp = await fetch('/api/data');
        const data = await resp.json();
        updateDashboard(data);
    } catch (err) {
        document.getElementById('statusText').textContent = 'Connection lost...';
    }
}

// Initialize
initCharts();
poll();
setInterval(poll, 2000);
</script>
</body>
</html>"""


class DashboardHandler(SimpleHTTPRequestHandler):
    """Gestionnaire HTTP pour le tableau de bord en direct."""

    def do_GET(self) -> None:
        """Router les requêtes vers le tableau de bord ou l'API."""
        if self.path == "/" or self.path == "/index.html":
            self._send_html(DASHBOARD_HTML)
        elif self.path == "/api/data":
            self._send_json(build_api_data())
        else:
            self.send_error(404)

    def _send_html(self, html: str) -> None:
        """Envoyer une réponse HTML."""
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def _send_json(self, data: dict) -> None:
        """Envoyer une réponse JSON avec en-têtes CORS."""
        body = json.dumps(data)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format: str, *args: Any) -> None:
        """Supprimer les journaux d'accès."""
        pass


def main() -> None:
    """Démarrer le serveur de tableau de bord en direct."""
    parser = argparse.ArgumentParser(description="Tableau de Bord OEE en Direct")
    parser.add_argument("--port", type=int, default=8050, help="Port (défaut : 8050)")
    args = parser.parse_args()

    print("=" * 60)
    print("  OEE Analytics - Serveur Tableau de Bord en Direct")
    print("=" * 60)
    print(f"  URL :    http://localhost:{args.port}")
    print(f"  API :    http://localhost:{args.port}/api/data")
    print(f"  Entrée :  {STREAMING_INPUT}")
    print(f"  Mise à jour : Toutes les 2 secondes (AJAX, sans rechargement)")
    print("  Appuyez sur Ctrl+C pour arrêter")
    print("=" * 60)

    server = HTTPServer(("0.0.0.0", args.port), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  [ARRÊT] Serveur de tableau de bord arrêté")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
