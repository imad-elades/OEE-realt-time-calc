#!/usr/bin/env python3
"""
OEE Analytics - Pipeline ETL PySpark.

Charge les données brutes d'événements de production depuis HDFS (ou en local),
calcule les métriques OEE (Disponibilité, Performance, Qualité), et écrit
les résultats en Parquet et CSV pour une consommation dans Power BI.

Utilisation :
    spark-submit spark/oee_pipeline.py
    python spark/oee_pipeline.py  # mode local de repli

Étapes du pipeline :
    1. Ingestion  -> Lire le CSV depuis HDFS ou chemin local
    2. Nettoyage  -> Supprimer les nuls, appliquer les types, filtrer les lignes invalides
    3. Calcul     -> Calculer OEE = Disponibilité x Performance x Qualité
    4. Export     -> Écrire Parquet + CSV (prêt Power BI, échelle 0-100)
    5. Agrégation -> Résumé OEE mensuel par machine
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
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
logger = logging.getLogger("oee_pipeline")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HDFS_INPUT_PATH: str = "hdfs://namenode:9000/oee/raw/production_events.csv"
LOCAL_INPUT_PATH: str = "data/raw/production_events.csv"

PARQUET_OUTPUT: str = "data/processed/oee_results.parquet"
CSV_OUTPUT: str = "data/processed/oee_results.csv"
MONTHLY_PARQUET_OUTPUT: str = "data/processed/oee_monthly_summary.parquet"
MONTHLY_CSV_OUTPUT: str = "data/processed/oee_monthly_summary.csv"

# ---------------------------------------------------------------------------
# Schéma
# ---------------------------------------------------------------------------
PRODUCTION_SCHEMA: StructType = StructType(
    [
        StructField("machine_id", StringType(), nullable=False),
        StructField("shift_id", StringType(), nullable=False),
        StructField("date", StringType(), nullable=False),
        StructField("planned_time_min", DoubleType(), nullable=False),
        StructField("actual_runtime_min", DoubleType(), nullable=False),
        StructField("ideal_cycle_time_sec", DoubleType(), nullable=False),
        StructField("actual_cycle_time_sec", DoubleType(), nullable=False),
        StructField("total_parts", IntegerType(), nullable=False),
        StructField("good_parts", IntegerType(), nullable=False),
        StructField("downtime_events", StringType(), nullable=True),
    ]
)


def create_spark_session(app_name: str = "OEE_Analytics") -> SparkSession:
    """Créer et configurer une SparkSession.

    Args:
        app_name: Nom de l'application Spark.

    Returns:
        Instance SparkSession configurée.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created: %s", app_name)
    return spark


def load_data(spark: SparkSession) -> DataFrame:
    """Charger le CSV des événements de production depuis HDFS ou en local.

    Tente d'abord le chemin HDFS ; si indisponible, utilise le système de fichiers local.

    Args:
        spark: SparkSession active.

    Returns:
        DataFrame brut avec les événements de production.

    Raises:
        FileNotFoundError: Si ni HDFS ni le fichier local ne sont accessibles.
    """
    # Essayer HDFS d'abord
    try:
        logger.info("Tentative de chargement HDFS : %s", HDFS_INPUT_PATH)
        df = (
            spark.read
            .option("header", "true")
            .schema(PRODUCTION_SCHEMA)
            .csv(HDFS_INPUT_PATH)
        )
        count = df.count()
        logger.info("[OK] %d lignes chargées depuis HDFS", count)
        return df
    except Exception as e:
        logger.warning("HDFS indisponible (%s), repli sur le chemin local", e)

    # Repli local
    local_path = LOCAL_INPUT_PATH
    if not os.path.exists(local_path):
        raise FileNotFoundError(
            f"Aucune donnée trouvée dans HDFS ({HDFS_INPUT_PATH}) ou en local ({local_path}). "
            "Exécutez 'make generate' d'abord."
        )

    logger.info("Chargement depuis le chemin local : %s", local_path)
    df = (
        spark.read
        .option("header", "true")
        .schema(PRODUCTION_SCHEMA)
        .csv(local_path)
    )
    count = df.count()
    logger.info("[OK] %d lignes chargées depuis le CSV local", count)
    return df


def clean_data(df: DataFrame) -> DataFrame:
    """Nettoyer et valider les données d'événements de production.

    Opérations :
        - Supprimer les lignes avec des valeurs nulles dans les colonnes critiques
        - Filtrer les lignes où planned_time_min <= 0
        - Filtrer les lignes où total_parts <= 0
        - Caster la chaîne de date en type date approprié

    Args:
        df: DataFrame brut des événements de production.

    Returns:
        DataFrame nettoyé.
    """
    initial_count: int = df.count()

    critical_cols = [
        "machine_id", "shift_id", "date",
        "planned_time_min", "actual_runtime_min",
        "ideal_cycle_time_sec", "actual_cycle_time_sec",
        "total_parts", "good_parts",
    ]

    df_clean = (
        df
        .dropna(subset=critical_cols)
        .filter(F.col("planned_time_min") > 0)
        .filter(F.col("total_parts") > 0)
        .filter(F.col("actual_runtime_min") >= 0)
        .filter(F.col("ideal_cycle_time_sec") > 0)
        .withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
    )

    final_count: int = df_clean.count()
    dropped: int = initial_count - final_count
    logger.info(
        "Nettoyage : %d -> %d lignes (%d lignes invalides supprimées)",
        initial_count, final_count, dropped,
    )

    return df_clean


def compute_oee(df: DataFrame) -> DataFrame:
    """Calculer les métriques OEE par machine/équipe/date.

    Formules :
        Disponibilité = actual_runtime_min / planned_time_min
        Performance   = (ideal_cycle_time_sec x total_parts) / (actual_runtime_min x 60)
        Qualité       = good_parts / total_parts
        OEE           = Disponibilité x Performance x Qualité

    Toutes les métriques sont limitées à [0.0, 1.0] puis converties en pourcentage (0-100).

    Args:
        df: DataFrame nettoyé des événements de production.

    Returns:
        DataFrame avec les métriques OEE en colonnes de pourcentage (échelle 0-100).
    """
    df_oee = df.withColumn(
        "availability_raw",
        F.col("actual_runtime_min") / F.col("planned_time_min"),
    ).withColumn(
        "performance_raw",
        (F.col("ideal_cycle_time_sec") * F.col("total_parts"))
        / (F.col("actual_runtime_min") * F.lit(60)),
    ).withColumn(
        "quality_raw",
        F.col("good_parts") / F.col("total_parts"),
    )

    # Limiter toutes les métriques brutes à [0.0, 1.0]
    for metric in ["availability_raw", "performance_raw", "quality_raw"]:
        df_oee = df_oee.withColumn(
            metric,
            F.when(F.col(metric) < 0.0, F.lit(0.0))
            .when(F.col(metric) > 1.0, F.lit(1.0))
            .otherwise(F.col(metric)),
        )

    # Calculer l'OEE et convertir en pourcentage (échelle 0-100, 2 décimales)
    df_oee = (
        df_oee
        .withColumn(
            "oee_raw",
            F.col("availability_raw") * F.col("performance_raw") * F.col("quality_raw"),
        )
        .withColumn("availability_pct", F.round(F.col("availability_raw") * 100, 2))
        .withColumn("performance_pct", F.round(F.col("performance_raw") * 100, 2))
        .withColumn("quality_pct", F.round(F.col("quality_raw") * 100, 2))
        .withColumn("oee_pct", F.round(F.col("oee_raw") * 100, 2))
    )

    # Sélectionner les colonnes finales prêtes pour Power BI
    df_result = df_oee.select(
        "machine_id",
        "shift_id",
        "date",
        "availability_pct",
        "performance_pct",
        "quality_pct",
        "oee_pct",
    )

    return df_result


def compute_monthly_summary(df_oee: DataFrame) -> DataFrame:
    """Agréger les métriques OEE en un résumé mensuel par machine.

    Args:
        df_oee: DataFrame avec les pourcentages OEE par événement.

    Returns:
        DataFrame avec les métriques OEE moyennes mensuelles par machine.
    """
    df_monthly = (
        df_oee
        .withColumn("year_month", F.date_format(F.col("date"), "yyyy-MM"))
        .groupBy("machine_id", "year_month")
        .agg(
            F.round(F.avg("availability_pct"), 2).alias("avg_availability_pct"),
            F.round(F.avg("performance_pct"), 2).alias("avg_performance_pct"),
            F.round(F.avg("quality_pct"), 2).alias("avg_quality_pct"),
            F.round(F.avg("oee_pct"), 2).alias("avg_oee_pct"),
            F.count("*").alias("event_count"),
            F.round(F.min("oee_pct"), 2).alias("min_oee_pct"),
            F.round(F.max("oee_pct"), 2).alias("max_oee_pct"),
        )
        .orderBy("machine_id", "year_month")
    )

    return df_monthly


def write_results(
    df: DataFrame,
    parquet_path: str,
    csv_path: str,
    label: str = "résultats",
) -> None:
    """Écrire le DataFrame en Parquet et CSV.

    Utilise l'écriture Spark sur Linux/Docker. Utilize le repli pandas sur Windows
    quand l'E/S native Hadoop est indisponible.

    Args:
        df: DataFrame à écrire.
        parquet_path: Chemin de sortie pour les fichiers Parquet.
        csv_path: Chemin de sortie pour le fichier CSV.
        label: Étiquette lisible pour la journalisation.
    """
    try:
        # Essayer l'écriture native Spark (fonctionne dans Docker / Linux)
        logger.info("Écriture %s -> Parquet : %s", label, parquet_path)
        (
            df.write
            .mode("overwrite")
            .parquet(parquet_path)
        )

        logger.info("Écriture %s -> CSV : %s", label, csv_path)
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(csv_path)
        )

        # Renommer le fichier part généré par Spark en un nom propre
        _rename_spark_csv(csv_path)

    except Exception as e:
        # Repli : convertir en pandas et écrire directement (compatibilité Windows)
        logger.warning(
            "L'écriture Spark a échoué (%s). Utilisation du repli pandas.", type(e).__name__
        )
        _write_with_pandas(df, parquet_path, csv_path, label)

    logger.info("[OK] %s écrit avec succès", label)


def _write_with_pandas(
    df: DataFrame,
    parquet_path: str,
    csv_path: str,
    label: str,
) -> None:
    """Écrivain de repli utilisant pandas pour la compatibilité Windows.

    Évite df.toPandas() qui échoue sur Python 3.12+ (distutils supprimé).
    Au lieu de cela, collecte les lignes manuellement et construit un DataFrame pandas.

    Args:
        df: DataFrame Spark à écrire.
        parquet_path: Chemin de sortie pour le fichier Parquet.
        csv_path: Chemin de sortie pour le fichier CSV.
        label: Étiquette lisible pour la journalisation.
    """
    import csv
    import shutil

    import pandas as pd

    # Collecter les données manuellement (évite le problème distutils de toPandas)
    columns = df.columns
    rows = df.collect()
    data = [row.asDict() for row in rows]
    pdf = pd.DataFrame(data, columns=columns)

    # Convertir les colonnes de date en chaînes pour la compatibilité CSV
    for col in pdf.columns:
        if hasattr(pdf[col].dtype, 'name') and 'datetime' in pdf[col].dtype.name:
            pdf[col] = pdf[col].astype(str)

    # S'assurer que les répertoires de sortie existent ; supprimer l'ancienne sortie si présente
    for path in [parquet_path, csv_path]:
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)

    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    # Écrire Parquet (fichier unique)
    parquet_file = parquet_path if parquet_path.endswith(".parquet") else parquet_path + ".parquet"
    if os.path.isdir(parquet_path):
        shutil.rmtree(parquet_path)
    pdf.to_parquet(parquet_file, index=False, engine="pyarrow")
    logger.info("Pandas a écrit %s -> %s (%d lignes)", label, parquet_file, len(pdf))

    # Écrire CSV (fichier plat unique pour Power BI)
    csv_file = csv_path if csv_path.endswith(".csv") else csv_path + ".csv"
    if os.path.isdir(csv_path):
        shutil.rmtree(csv_path)
    pdf.to_csv(csv_file, index=False)
    logger.info("Pandas a écrit %s -> %s (%d lignes)", label, csv_file, len(pdf))


def _rename_spark_csv(output_dir: str) -> None:
    """Renommer la sortie part-*.csv de Spark en un nom de fichier propre.

    Spark écrit le CSV dans un répertoire avec des fichiers part. Cette fonction
    trouve le fichier part et le renomme en nom_de_répertoire + .csv.

    Args:
        output_dir: Le répertoire de sortie CSV Spark.
    """
    if not os.path.isdir(output_dir):
        return

    for fname in os.listdir(output_dir):
        if fname.startswith("part-") and fname.endswith(".csv"):
            src = os.path.join(output_dir, fname)
            parent = os.path.dirname(output_dir)
            base = os.path.basename(output_dir)
            clean_name = base if base.endswith(".csv") else f"{base}.csv"
            dst = os.path.join(parent, f"_{clean_name}")

            import shutil
            shutil.copy2(src, dst)
            logger.info("Renamed %s -> %s", fname, dst)
            break


def log_execution_plan(df: DataFrame, label: str = "Pipeline OEE") -> None:
    """Journaliser le plan d'exécution DAG Spark.

    Args:
        df: DataFrame à expliquer.
        label: Étiquette pour la sortie du plan.
    """
    logger.info("=" * 60)
    logger.info("Plan d'exécution DAG - %s", label)
    logger.info("=" * 60)
    df.explain(extended=True)


def run_pipeline(spark: Optional[SparkSession] = None) -> None:
    """Exécuter le pipeline complet d'analyse OEE.

    Étapes :
        1. Créer une SparkSession (ou utiliser celle fournie)
        2. Charger les données brutes depuis HDFS ou CSV local
        3. Nettoyer et valider les données
        4. Calculer les métriques OEE par événement
        5. Journaliser le plan d'exécution DAG
        6. Écrire les résultats détaillés en Parquet + CSV
        7. Calculer et écrire le résumé mensuel
        8. Afficher un échantillon des résultats

    Args:
        spark: SparkSession préexistante optionnelle.
    """
    print("=" * 60)
    print("  OEE Analytics - Pipeline PySpark")
    print("=" * 60)

    # Étape 1 : SparkSession
    if spark is None:
        spark = create_spark_session()

    try:
        # Étape 2 : Chargement
        logger.info("ÉTAPE 1/5 - Chargement des données...")
        df_raw = load_data(spark)

        # Étape 3 : Nettoyage
        logger.info("ÉTAPE 2/5 - Nettoyage des données...")
        df_clean = clean_data(df_raw)

        # Étape 4 : Calcul OEE
        logger.info("ÉTAPE 3/5 - Calcul des métriques OEE...")
        df_oee = compute_oee(df_clean)

        # Étape 5 : Journalisation DAG
        logger.info("ÉTAPE 4/5 - Journalisation du plan d'exécution...")
        log_execution_plan(df_oee, "Calcul OEE")

        # Étape 6 : Écriture des résultats
        logger.info("ÉTAPE 5/5 - Écriture des résultats...")
        write_results(df_oee, PARQUET_OUTPUT, CSV_OUTPUT, "Détail OEE")

        # Étape 7 : Résumé mensuel
        logger.info("Calcul du résumé mensuel OEE...")
        df_monthly = compute_monthly_summary(df_oee)
        write_results(
            df_monthly,
            MONTHLY_PARQUET_OUTPUT,
            MONTHLY_CSV_OUTPUT,
            "Résumé Mensuel",
        )

        # Afficher un échantillon des résultats
        logger.info("Échantillon des résultats OEE :")
        df_oee.show(20, truncate=False)

        logger.info("Échantillon du résumé mensuel :")
        df_monthly.show(10, truncate=False)

        row_count = df_oee.count()
        logger.info("[OK] Pipeline terminé - %d enregistrements OEE produits", row_count)

        # Statistiques récapitulatives
        logger.info("Distribution OEE :")
        df_oee.select("oee_pct").summary("min", "25%", "50%", "75%", "max", "mean").show()

    finally:
        spark.stop()
        logger.info("SparkSession arrêtée")

    print("=" * 60)
    print("  Pipeline terminé avec succès !")
    print("=" * 60)


def main() -> None:
    """Point d'entrée pour le pipeline OEE."""
    run_pipeline()


if __name__ == "__main__":
    main()
