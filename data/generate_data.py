#!/usr/bin/env python3
"""
OEE Analytics — Générateur de données d'événements de production.

Simule 10 000 événements de production sur 5 machines, 4 équipes et 30 jours
avec des distributions réalistes pour les métriques de disponibilité, performance et qualité.

Utilisation :
    python data/generate_data.py

Sortie :
    data/raw/production_events.csv
    data/raw/production_events.json
"""

from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

MACHINES: list[str] = [f"M{str(i).zfill(3)}" for i in range(1, 6)]
SHIFTS: list[str] = ["morning", "afternoon", "night", "weekend"]
NUM_DAYS: int = 30
TARGET_EVENTS: int = 10_000
SEED: int = 42

DOWNTIME_REASONS: list[str] = [
    "mechanical_failure",
    "electrical_fault",
    "material_shortage",
    "tool_change",
    "operator_break",
    "planned_maintenance",
    "quality_inspection",
    "setup_changeover",
    "software_error",
    "power_outage",
]


def _generate_downtime_events(
    max_downtime_min: float,
    rng: random.Random,
) -> list[dict[str, Any]]:
    """Générer une liste réaliste d'événements d'arrêt.

    Args:
        max_downtime_min: Budget total d'arrêt maximum en minutes.
        rng: Instance aléatoire ensemencée pour la reproductibilité.

    Returns:
        Liste de dictionnaires avec les clés ``reason`` et ``duration_min``.
    """
    if max_downtime_min <= 0:
        return []

    num_events: int = rng.randint(0, min(5, int(max_downtime_min / 5) + 1))
    events: list[dict[str, Any]] = []
    remaining: float = max_downtime_min

    for _ in range(num_events):
        if remaining <= 1.0:
            break
        duration: float = round(rng.uniform(1.0, min(remaining, 45.0)), 1)
        events.append(
            {
                "reason": rng.choice(DOWNTIME_REASONS),
                "duration_min": duration,
            }
        )
        remaining -= duration

    return events


def generate_production_events(seed: int = SEED) -> pd.DataFrame:
    """Générer des données d'événements de production synthétiques.

    Crée un DataFrame d'événements de production avec des corrélations réalistes
    entre le temps prévu, le temps d'exécution réel, les temps de cycle et les quantités de pièces.

    Args:
        seed: Graine aléatoire pour la reproductibilité.

    Returns:
        DataFrame avec les colonnes : machine_id, shift_id, date, planned_time_min,
        actual_runtime_min, ideal_cycle_time_sec, actual_cycle_time_sec,
        total_parts, good_parts, downtime_events.
    """
    rng = random.Random(seed)
    np.random.seed(seed)

    # Pré-calculer toutes les combinaisons machine/équipe/date (5×4×30 = 600)
    combos: list[tuple[str, str, str]] = []
    base_date = datetime(2025, 1, 1)
    for machine in MACHINES:
        for shift in SHIFTS:
            for day_offset in range(NUM_DAYS):
                date_str = (base_date + timedelta(days=day_offset)).strftime("%Y-%m-%d")
                combos.append((machine, shift, date_str))

    # Nous avons 600 combinaisons ; pour atteindre 10 000 événements, nous échantillonnons
    # les combinaisons avec remplacement pour atteindre TARGET_EVENTS
    sampled_combos = [rng.choice(combos) for _ in range(TARGET_EVENTS)]

    records: list[dict[str, Any]] = []

    # Profils spécifiques aux machines pour plus de réalisme
    machine_profiles: dict[str, dict[str, float]] = {
        "M001": {"efficiency": 0.92, "ideal_cycle": 12.0},
        "M002": {"efficiency": 0.85, "ideal_cycle": 18.0},
        "M003": {"efficiency": 0.88, "ideal_cycle": 8.0},
        "M004": {"efficiency": 0.78, "ideal_cycle": 25.0},
        "M005": {"efficiency": 0.95, "ideal_cycle": 15.0},
    }

    shift_multipliers: dict[str, float] = {
        "morning": 1.0,
        "afternoon": 0.95,
        "night": 0.88,
        "weekend": 0.80,
    }

    for machine_id, shift_id, date_str in sampled_combos:
        profile = machine_profiles[machine_id]
        shift_mult = shift_multipliers[shift_id]

        # Temps prévu : 360–480 min (6–8 heures) selon l'équipe
        planned_time_min: float = round(
            rng.uniform(360.0, 480.0) * shift_mult, 1
        )

        # Facteur de disponibilité : distribution normale autour de l'efficacité de la machine
        avail_factor: float = min(
            1.0,
            max(0.3, float(np.random.normal(profile["efficiency"] * shift_mult, 0.08))),
        )
        actual_runtime_min: float = round(planned_time_min * avail_factor, 1)

        # Arrêt = prévu - réel (utilisé pour la génération d'événements d'arrêt)
        downtime_budget: float = planned_time_min - actual_runtime_min
        downtime_events = _generate_downtime_events(downtime_budget, rng)

        # Temps de cycle
        ideal_cycle_time_sec: float = profile["ideal_cycle"]
        # Le temps de cycle réel est légèrement pire que l'idéal
        perf_degradation: float = float(np.random.normal(1.12, 0.06))
        perf_degradation = max(1.0, perf_degradation)
        actual_cycle_time_sec: float = round(
            ideal_cycle_time_sec * perf_degradation, 2
        )

        # Pièces produites basées sur le temps d'exécution réel et le temps de cycle réel
        max_possible_parts: int = max(
            1, int((actual_runtime_min * 60) / actual_cycle_time_sec)
        )
        total_parts: int = max(1, int(max_possible_parts * rng.uniform(0.85, 1.0)))

        # Qualité : la plupart des pièces sont bonnes (90–99,5 %)
        quality_rate: float = min(1.0, max(0.80, float(np.random.normal(0.96, 0.025))))
        good_parts: int = max(0, min(total_parts, int(total_parts * quality_rate)))

        records.append(
            {
                "machine_id": machine_id,
                "shift_id": shift_id,
                "date": date_str,
                "planned_time_min": planned_time_min,
                "actual_runtime_min": actual_runtime_min,
                "ideal_cycle_time_sec": ideal_cycle_time_sec,
                "actual_cycle_time_sec": actual_cycle_time_sec,
                "total_parts": total_parts,
                "good_parts": good_parts,
                "downtime_events": json.dumps(downtime_events),
            }
        )

    return pd.DataFrame(records)


def save_data(df: pd.DataFrame, output_dir: str = "data/raw") -> None:
    """Sauvegarder le DataFrame en fichiers CSV et JSON.

    Args:
        df: DataFrame des événements de production.
        output_dir: Répertoire de sauvegarde des fichiers de sortie.
    """
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, "production_events.csv")
    json_path = os.path.join(output_dir, "production_events.json")

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", indent=2)

    print(f"[OK] {len(df):,} événements de production générés")
    print(f"   > CSV :  {csv_path}  ({os.path.getsize(csv_path) / 1024:.1f} Ko)")
    print(f"   > JSON : {json_path} ({os.path.getsize(json_path) / 1024:.1f} Ko)")
    print(f"   > Machines : {sorted(df['machine_id'].unique())}")
    print(f"   > Équipes :  {sorted(df['shift_id'].unique())}")
    print(f"   > Période : {df['date'].min()} -> {df['date'].max()}")


def main() -> None:
    """Point d'entrée : générer et sauvegarder les données d'événements de production."""
    print("=" * 60)
    print("  OEE Analytics — Générateur de Données")
    print("=" * 60)
    df = generate_production_events()
    save_data(df)
    print("=" * 60)


if __name__ == "__main__":
    main()
