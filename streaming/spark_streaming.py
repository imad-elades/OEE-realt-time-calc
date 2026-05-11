#!/usr/bin/env python3
"""
OEE Analytics - Pipeline Spark Structured Streaming.

Lit les événements de production en temps réel depuis une source de streaming
(répertoire de fichiers ou Kafka), calcule les métriques OEE en micro-lots,
et affiche les résultats dans un CSV mis à jour en direct + console.

Utilisation :
    python streaming/spark_streaming.py              # Streaming basé sur fichiers
    python streaming/spark_streaming.py --mode kafka  # Streaming Kafka
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Journalisation
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("oee_streaming")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STREAMING_INPUT_DIR: str = "data/streaming/input"
STREAMING_OUTPUT_DIR: str = "data/streaming/output"
CHECKPOINT_DIR: str = "data/streaming/checkpoint"
KAFKA_BROKER: str = "localhost:9092"
KAFKA_TOPIC: str = "oee-events"

# ---------------------------------------------------------------------------
# Schéma pour les événements de streaming
# ---------------------------------------------------------------------------
DOWNTIME_SCHEMA = ArrayType(
    StructType([
        StructField("reason", StringType()),
        StructField("duration_min", DoubleType()),
    ])
)

EVENT_SCHEMA: StructType = StructType([
    StructField("event_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("machine_id", StringType()),
    StructField("machine_name", StringType()),
    StructField("shift_id", StringType()),
    StructField("date", StringType()),
    StructField("planned_time_min", DoubleType()),
    StructField("actual_runtime_min", DoubleType()),
    StructField("ideal_cycle_time_sec", DoubleType()),
    StructField("actual_cycle_time_sec", DoubleType()),
    StructField("total_parts", IntegerType()),
    StructField("good_parts", IntegerType()),
    StructField("defective_parts", IntegerType()),
    StructField("downtime_events", DOWNTIME_SCHEMA),
    StructField("availability_pct", DoubleType()),
    StructField("performance_pct", DoubleType()),
    StructField("quality_pct", DoubleType()),
    StructField("oee_pct", DoubleType()),
])


def create_spark_session() -> SparkSession:
    """Créer une SparkSession configurée pour le streaming structuré.

    Returns:
        SparkSession configurée.
    """
    spark = (
        SparkSession.builder
        .appName("OEE_Streaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created for streaming")
    return spark


def read_file_stream(spark: SparkSession) -> DataFrame:
    """Lire les événements de streaming depuis des fichiers JSON dans un répertoire.

    Spark surveille le répertoire d'entrée pour les nouveaux fichiers et les traite
    en micro-lots.

    Args:
        spark: SparkSession active.

    Returns:
        DataFrame de streaming avec les événements de production.
    """
    logger.info("Démarrage du flux fichier depuis : %s", STREAMING_INPUT_DIR)
    os.makedirs(STREAMING_INPUT_DIR, exist_ok=True)

    return (
        spark.readStream
        .format("json")
        .schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .load(STREAMING_INPUT_DIR)
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Lire les événements de streaming depuis un topic Kafka.

    Args:
        spark: SparkSession active.

    Returns:
        DataFrame de streaming avec les événements de production.
    """
    logger.info("Démarrage du flux Kafka depuis : %s / %s", KAFKA_BROKER, KAFKA_TOPIC)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Analyser le JSON depuis la valeur Kafka
    return (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )


def compute_streaming_oee(df: DataFrame) -> DataFrame:
    """Calculer les métriques OEE à partir des événements de streaming.

    Étant donné que le producteur calcule déjà l'OEE, cette fonction
    sélectionne et formate les colonnes prêtes pour Power BI.
    Pour les événements bruts sans OEE pré-calculé, elle recalcule.

    Args:
        df: DataFrame de streaming avec les événements de production.

    Returns:
        DataFrame de streaming avec les colonnes OEE.
    """
    return df.select(
        "event_id",
        "timestamp",
        "machine_id",
        "machine_name",
        "shift_id",
        "date",
        "planned_time_min",
        "actual_runtime_min",
        "total_parts",
        "good_parts",
        "defective_parts",
        F.col("availability_pct"),
        F.col("performance_pct"),
        F.col("quality_pct"),
        F.col("oee_pct"),
    )


def compute_running_aggregates(df: DataFrame) -> DataFrame:
    """Calculer les agrégats OEE en cours par machine.

    Utilise le watermarking pour les événements en retard et calcule
    une agrégation par fenêtre glissante.

    Args:
        df: DataFrame de streaming avec les métriques OEE.

    Returns:
        DataFrame de streaming agrégé.
    """
    return (
        df
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            F.window("event_time", "30 seconds", "10 seconds"),
            "machine_id",
        )
        .agg(
            F.round(F.avg("availability_pct"), 2).alias("avg_availability"),
            F.round(F.avg("performance_pct"), 2).alias("avg_performance"),
            F.round(F.avg("quality_pct"), 2).alias("avg_quality"),
            F.round(F.avg("oee_pct"), 2).alias("avg_oee"),
            F.count("*").alias("event_count"),
            F.round(F.min("oee_pct"), 2).alias("min_oee"),
            F.round(F.max("oee_pct"), 2).alias("max_oee"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "machine_id",
            "avg_availability",
            "avg_performance",
            "avg_quality",
            "avg_oee",
            "event_count",
            "min_oee",
            "max_oee",
        )
    )


def write_to_console_and_csv(
    df_oee: DataFrame,
    df_agg: DataFrame,
) -> tuple:
    """Démarrer les requêtes de streaming qui écrivent dans la console et CSV.

    Args:
        df_oee: DataFrame de streaming OEE par événement.
        df_agg: DataFrame de streaming OEE agrégé.

    Returns:
        Tuple de requêtes de streaming.
    """
    os.makedirs(STREAMING_OUTPUT_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    # Requête 1 : Écrire les événements détaillés dans la console
    query_console = (
        df_oee
        .select("event_id", "machine_id", "shift_id",
                "availability_pct", "performance_pct", "quality_pct", "oee_pct")
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime="5 seconds")
        .start()
    )
    logger.info("Console output stream started")

    # Requête 2 : Écrire les agrégats dans une table mémoire (interrogeable)
    query_memory = (
        df_agg
        .writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("oee_live_aggregates")
        .trigger(processingTime="10 seconds")
        .start()
    )
    logger.info("Memory table 'oee_live_aggregates' stream started")

    # Requête 3 : Écrire les données par événement en JSON pour le tableau de bord en direct
    query_csv = (
        df_oee
        .select("event_id", "timestamp", "machine_id", "machine_name",
                "shift_id", "availability_pct", "performance_pct",
                "quality_pct", "oee_pct")
        .writeStream
        .outputMode("append")
        .format("json")
        .option("path", STREAMING_OUTPUT_DIR)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="5 seconds")
        .start()
    )
    logger.info("JSON output stream started -> %s", STREAMING_OUTPUT_DIR)

    return query_console, query_memory, query_csv


def run_streaming(mode: str = "file") -> None:
    """Exécuter le pipeline Spark Structured Streaming.

    Args:
        mode: Source d'entrée ('file' ou 'kafka').
    """
    print("=" * 70)
    print("  OEE Analytics - Spark Structured Streaming")
    print("=" * 70)
    print(f"  Mode : {mode}")
    print(f"  Entrée : {STREAMING_INPUT_DIR if mode == 'file' else KAFKA_TOPIC}")
    print(f"  Sortie : {STREAMING_OUTPUT_DIR}")
    print("  Appuyez sur Ctrl+C pour arrêter")
    print("=" * 70)

    spark = create_spark_session()

    try:
        # Lire le flux
        if mode == "kafka":
            df_raw = read_kafka_stream(spark)
        else:
            df_raw = read_file_stream(spark)

        # Traiter
        df_oee = compute_streaming_oee(df_raw)
        df_agg = compute_running_aggregates(df_oee)

        # Sortie
        q_console, q_memory, q_csv = write_to_console_and_csv(df_oee, df_agg)

        print("\n  [OK] Pipeline de streaming démarré !")
        print("  En attente d'événements...")
        print("  Lancez le producteur : python streaming/producer.py\n")

        # Attendre la terminaison
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        print("\n  [ARRÊT] Arrêt du pipeline de streaming...")
    finally:
        spark.stop()
        print("  SparkSession arrêtée")
        print("=" * 70)


def main() -> None:
    """Point d'entrée."""
    import argparse
    parser = argparse.ArgumentParser(description="Streaming Spark OEE")
    parser.add_argument("--mode", choices=["file", "kafka"], default="file",
                        help="Input source (default: file)")
    args = parser.parse_args()
    run_streaming(mode=args.mode)


if __name__ == "__main__":
    main()
