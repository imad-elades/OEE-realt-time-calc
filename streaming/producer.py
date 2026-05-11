#!/usr/bin/env python3
"""
OEE Analytics - Producteur de Données en Temps Réel.

Simule des capteurs de machine générant des événements de production en temps réel.
Chaque événement représente un cycle de production d'une machine.

Modes :
    - file :  Écrit des fichiers JSON dans data/streaming/input/ (par défaut)
    - kafka : Envoie les événements vers le topic Kafka 'oee-events'

Utilisation :
    python streaming/producer.py                    # Mode fichier, intervalle 2s
    python streaming/producer.py --interval 1       # 1 événement par seconde
    python streaming/producer.py --mode kafka        # Mode Kafka
    python streaming/producer.py --burst 5           # 5 événements par intervalle
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MACHINES: list[str] = ["M001", "M002", "M003", "M004", "M005"]
SHIFTS: list[str] = ["morning", "afternoon", "night", "weekend"]
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
OUTPUT_DIR: str = str(PROJECT_ROOT / "data" / "streaming" / "input")
KAFKA_TOPIC: str = "oee-events"
KAFKA_BROKER: str = "localhost:9092"

DOWNTIME_REASONS: list[str] = [
    "mechanical_failure", "electrical_fault", "material_shortage",
    "tool_change", "operator_break", "planned_maintenance",
    "quality_inspection", "setup_changeover",
]

MACHINE_PROFILES: dict[str, dict[str, float]] = {
    "M001": {"efficiency": 0.92, "ideal_cycle": 12.0, "name": "CNC Lathe"},
    "M002": {"efficiency": 0.85, "ideal_cycle": 18.0, "name": "Milling Center"},
    "M003": {"efficiency": 0.88, "ideal_cycle": 8.0,  "name": "Press Machine"},
    "M004": {"efficiency": 0.78, "ideal_cycle": 25.0, "name": "Assembly Robot"},
    "M005": {"efficiency": 0.95, "ideal_cycle": 15.0, "name": "Packaging Line"},
}

# Drapeau global pour un arrêt en douceur
running: bool = True


def signal_handler(sig: int, frame: Any) -> None:
    """Gérer Ctrl+C pour un arrêt en douceur."""
    global running
    print("\n[ARRÊT] Arrêt du producteur...")
    running = False


def get_current_shift() -> str:
    """Déterminer l'équipe actuelle en fonction de l'heure de la journée.

    Returns:
        Nom de l'équipe : morning (6-14), afternoon (14-22), night (22-6),
        ou weekend si samedi/dimanche.
    """
    now = datetime.now()
    if now.weekday() >= 5:
        return "weekend"
    hour = now.hour
    if 6 <= hour < 14:
        return "morning"
    elif 14 <= hour < 22:
        return "afternoon"
    else:
        return "night"


def generate_event(event_id: int) -> dict[str, Any]:
    """Générer un seul événement de production simulant des données de capteurs réelles.

    Args:
        event_id: Identifiant séquentiel unique de l'événement.

    Returns:
        Dictionnaire avec les données de l'événement de production.
    """
    machine_id = random.choice(MACHINES)
    profile = MACHINE_PROFILES[machine_id]
    shift_id = get_current_shift()
    now = datetime.now()

    # Temps prévu pour ce cycle (fenêtre de 5-15 min)
    planned_time_min = round(random.uniform(5.0, 15.0), 1)

    # Disponibilité : spécifique à la machine avec un peu d'aléatoire
    avail_factor = min(1.0, max(0.3, random.gauss(profile["efficiency"], 0.08)))
    actual_runtime_min = round(planned_time_min * avail_factor, 1)

    # Temps de cycle
    ideal_cycle_time_sec = profile["ideal_cycle"]
    perf_degradation = max(1.0, random.gauss(1.12, 0.06))
    actual_cycle_time_sec = round(ideal_cycle_time_sec * perf_degradation, 2)

    # Pièces produites
    max_parts = max(1, int((actual_runtime_min * 60) / actual_cycle_time_sec))
    total_parts = max(1, int(max_parts * random.uniform(0.85, 1.0)))
    quality_rate = min(1.0, max(0.80, random.gauss(0.96, 0.025)))
    good_parts = max(0, min(total_parts, int(total_parts * quality_rate)))

    # Événements d'arrêt
    downtime_budget = planned_time_min - actual_runtime_min
    downtime_events = []
    max_dt = min(3, int(downtime_budget / 2))
    if downtime_budget > 1.0 and max_dt >= 1 and random.random() < 0.4:
        num_dt = random.randint(1, max_dt)
        remaining = downtime_budget
        for _ in range(num_dt):
            if remaining < 0.5:
                break
            dur = round(random.uniform(0.5, min(remaining, 5.0)), 1)
            downtime_events.append({
                "reason": random.choice(DOWNTIME_REASONS),
                "duration_min": dur,
            })
            remaining -= dur

    # Calculer l'OEE en ligne pour le tableau de bord en direct
    availability = actual_runtime_min / planned_time_min if planned_time_min > 0 else 0
    performance = (ideal_cycle_time_sec * total_parts) / (actual_runtime_min * 60) if actual_runtime_min > 0 else 0
    quality = good_parts / total_parts if total_parts > 0 else 0
    oee = min(1.0, availability) * min(1.0, performance) * min(1.0, quality)

    return {
        "event_id": event_id,
        "timestamp": now.isoformat(),
        "machine_id": machine_id,
        "machine_name": profile["name"],
        "shift_id": shift_id,
        "date": now.strftime("%Y-%m-%d"),
        "planned_time_min": planned_time_min,
        "actual_runtime_min": actual_runtime_min,
        "ideal_cycle_time_sec": ideal_cycle_time_sec,
        "actual_cycle_time_sec": actual_cycle_time_sec,
        "total_parts": total_parts,
        "good_parts": good_parts,
        "defective_parts": total_parts - good_parts,
        "downtime_events": downtime_events,
        "availability_pct": round(min(1.0, availability) * 100, 2),
        "performance_pct": round(min(1.0, performance) * 100, 2),
        "quality_pct": round(min(1.0, quality) * 100, 2),
        "oee_pct": round(min(1.0, oee) * 100, 2),
    }


def write_to_file(event: dict[str, Any], output_dir: str) -> str:
    """Écrire l'événement dans un fichier JSON dans le répertoire d'entrée du streaming.

    Args:
        event: Dictionnaire de l'événement de production.
        output_dir: Répertoire où écrire le fichier JSON.

    Returns:
        Chemin vers le fichier écrit.
    """
    os.makedirs(output_dir, exist_ok=True)
    filename = f"event_{event['event_id']:06d}_{event['machine_id']}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(event, f, indent=2)

    return filepath


def write_to_kafka(event: dict[str, Any], producer: Any) -> None:
    """Envoyer l'événement vers le topic Kafka.

    Args:
        event: Dictionnaire de l'événement de production.
        producer: Instance du producteur Kafka.
    """
    producer.send(
        KAFKA_TOPIC,
        key=event["machine_id"].encode(),
        value=json.dumps(event).encode(),
    )
    producer.flush()


def print_event_summary(event: dict[str, Any], filepath: str = "") -> None:
    """Afficher un résumé compact de l'événement dans la console.

    Args:
        event: Dictionnaire de l'événement de production.
        filepath: Chemin de fichier optionnel pour le mode fichier.
    """
    oee = event["oee_pct"]
    if oee >= 75:
        indicator = "[OK]"
    elif oee >= 55:
        indicator = "[--]"
    else:
        indicator = "[!!]"

    print(
        f"  {indicator} #{event['event_id']:04d} | "
        f"{event['machine_id']} ({event['machine_name']:<15s}) | "
        f"Shift: {event['shift_id']:<10s} | "
        f"A={event['availability_pct']:5.1f}% "
        f"P={event['performance_pct']:5.1f}% "
        f"Q={event['quality_pct']:5.1f}% | "
        f"OEE={oee:5.1f}%"
    )


def run_producer(
    mode: str = "file",
    interval: float = 2.0,
    burst: int = 1,
    max_events: int = 0,
) -> None:
    """Exécuter le producteur d'événements en temps réel.

    Args:
        mode: Mode de sortie ('file' ou 'kafka').
        interval: Secondes entre les lots d'événements.
        burst: Nombre d'événements par intervalle.
        max_events: Nombre maximum d'événements à produire (0 = illimité).
    """
    global running
    signal.signal(signal.SIGINT, signal_handler)

    print("=" * 76)
    print("  OEE Analytics - Producteur Temps Réel")
    print("=" * 76)
    print(f"  Mode :     {mode}")
    print(f"  Intervalle : {interval}s")
    print(f"  Rafale :    {burst} événement(s) par intervalle")
    print(f"  Machines : {', '.join(MACHINES)}")
    print(f"  Équipe :    {get_current_shift()}")
    if mode == "file":
        print(f"  Sortie :   {OUTPUT_DIR}/")
    else:
        print(f"  Kafka :    {KAFKA_BROKER} / {KAFKA_TOPIC}")
    print("=" * 76)
    print("  Appuyez sur Ctrl+C pour arrêter\n")

    kafka_producer = None
    if mode == "kafka":
        try:
            from kafka import KafkaProducer
            kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
            print(f"  [OK] Connecté à Kafka : {KAFKA_BROKER}")
        except Exception as e:
            print(f"  [ERREUR] Échec de connexion Kafka : {e}")
            print(f"  Repli vers le mode fichier...")
            mode = "file"

    event_id = 0
    total_oee = 0.0

    while running:
        for _ in range(burst):
            event_id += 1
            event = generate_event(event_id)
            total_oee += event["oee_pct"]

            if mode == "file":
                filepath = write_to_file(event, OUTPUT_DIR)
                print_event_summary(event, filepath)
            elif mode == "kafka" and kafka_producer:
                write_to_kafka(event, kafka_producer)
                print_event_summary(event)

            if max_events > 0 and event_id >= max_events:
                running = False
                break

        if running:
            time.sleep(interval)

    # Résumé et nettoyage
    avg_oee = total_oee / event_id if event_id > 0 else 0
    print(f"\n{'=' * 76}")
    print(f"  Producteur arrêté. {event_id} événements générés.")
    print(f"  OEE moyen : {avg_oee:.2f}%")

    # Nettoyage automatique : supprimer tous les fichiers de streaming générés
    if mode == "file":
        import glob
        pattern = os.path.join(OUTPUT_DIR, "event_*.json")
        files = glob.glob(pattern)
        for f in files:
            try:
                os.remove(f)
            except OSError:
                pass
        print(f"  [NETTOYAGE] {len(files)} fichiers d'événements supprimés de {OUTPUT_DIR}/")

    print(f"{'=' * 76}")

    if kafka_producer:
        kafka_producer.close()


def main() -> None:
    """Point d'entrée avec analyse des arguments."""
    parser = argparse.ArgumentParser(description="Producteur d'Événements Temps Réel OEE")
    parser.add_argument("--mode", choices=["file", "kafka"], default="file",
                        help="Output mode (default: file)")
    parser.add_argument("--interval", type=float, default=2.0,
                        help="Seconds between events (default: 2.0)")
    parser.add_argument("--burst", type=int, default=1,
                        help="Events per interval (default: 1)")
    parser.add_argument("--max-events", type=int, default=0,
                        help="Max events to produce, 0=unlimited (default: 0)")
    args = parser.parse_args()

    run_producer(
        mode=args.mode,
        interval=args.interval,
        burst=args.burst,
        max_events=args.max_events,
    )


if __name__ == "__main__":
    main()
