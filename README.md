# 🏭 OEE Analytics — Pipeline PySpark + Power BI

> Un pipeline Big Data de qualité production qui simule des données de fabrication, calcule
> les métriques OEE (Efficacité Globale des Équipements) avec PySpark, et exporte
> des jeux de données prêts pour Power BI — le tout orchestré avec Docker et HDFS.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Pipeline OEE Analytics                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │              │    │              │    │                      │  │
│  │  Génération  │───▶│  HDFS        │───▶│  ETL PySpark         │  │
│  │  Données     │    │  (Hadoop 3)  │    │  (Spark 3.5)         │  │
│  │  (Python)    │    │              │    │                      │  │
│  │              │    │  /oee/raw/   │    │  ┌────────────────┐  │  │
│  │  10K events  │    │  Stockage    │    │  │ 1. Charger CSV │  │  │
│  │  5 machines  │    │  CSV         │    │  │ 2. Nettoyer     │  │  │
│  │  4 équipes   │    │              │    │  │ 3. Calculer OEE │  │  │
│  │  30 jours    │    │  Répliqué    │    │  │ 4. Agréger      │  │  │
│  │              │    │  & tolérant  │    │  │ 5. Exporter     │  │  │
│  │  CSV + JSON  │    │  aux pannes  │    │  │  └────────────────┘  │  │
│  └──────────────┘    └──────────────┘    └──────────┬───────────┘  │
│                                                     │              │
│                                          ┌──────────▼───────────┐  │
│                                          │                      │  │
│                                          │  Couche de Sortie    │  │
│                                          │                      │  │
│                                          │  ├── Parquet (rapide)│  │
│                                          │  └── CSV (Power BI)  │  │
│                                          │                      │  │
│                                          └──────────┬───────────┘  │
│                                                     │              │
│                                          ┌──────────▼───────────┐  │
│                                          │                      │  │
│                                          │  📊 Power BI Desktop │  │
│                                          │                      │  │
│                                          │  Tableaux de bord,   │  │
│                                          │  KPI, Analyses       │  │
│                                          │  de tendances        │  │
│                                          │                      │  │
│                                          └──────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

Services Docker Compose :
┌────────────┐  ┌────────────┐  ┌──────────────┐  ┌──────────────┐
│  namenode   │  │  datanode   │  │ spark-master │  │spark-worker  │
│  :9870 UI   │  │  Données    │  │  :8080 UI    │  │  2G / 2 coeurs│
└────────────┘  └────────────┘  └──────────────┘  └──────────────┘
```

---

## 🚀 Démarrage Rapide (3 commandes)

```bash
# 1. Cloner le dépôt
git clone <repo-url> oee-analytics && cd oee-analytics

# 2. Exécuter le pipeline complet (générer données → upload HDFS → lancer Spark)
make all

# 3. Ouvrir les résultats dans Power BI Desktop
#    Fichier : data/processed/oee_results.csv
#    Voir : README_powerbi.md pour le guide d'import étape par étape
```

### Mode Local (sans Docker)

```bash
pip install -r requirements.txt
make generate
make run-local
```

---

## 📐 Formule OEE

**OEE = Disponibilité × Performance × Qualité**

### Définition des Composants

| Métrique        | Formule                                                           | Ce qu'elle mesure                     |
|-----------------|-------------------------------------------------------------------|---------------------------------------|
| **Disponibilité** | `actual_runtime_min / planned_time_min`                          | Temps de fonctionnement vs. prévu     |
| **Performance**   | `(ideal_cycle_time_sec × total_parts) / (actual_runtime_min × 60)` | Vitesse vs. maximum théorique         |
| **Qualité**       | `good_parts / total_parts`                                       | Taux de rendement des pièces bonnes   |
| **OEE**           | `Disponibilité × Performance × Qualité`                         | Efficacité globale des équipements    |

### Exemple Détaillé

```
Machine : M001 | Équipe : morning | Date : 2025-01-15

Entrées :
  planned_time_min      = 480 min (8 heures)
  actual_runtime_min    = 432 min (48 min d'arrêt)
  ideal_cycle_time_sec  = 12 sec/pièce
  actual_cycle_time_sec = 13,5 sec/pièce
  total_parts           = 1 920
  good_parts            = 1 843

Calculs :
  Disponibilité = 432 / 480            = 0,90  (90,00%)
  Performance   = (12 × 1920) / (432 × 60) = 23040 / 25920 = 0,8889 (88,89%)
  Qualité       = 1843 / 1920          = 0,9599 (95,99%)

  OEE = 0,90 × 0,8889 × 0,9599        = 0,7680 (76,80%)

Ligne de sortie :
  machine_id=M001, shift_id=morning, date=2025-01-15,
  availability_pct=90,00, performance_pct=88,89,
  quality_pct=95,99, oee_pct=76,80
```

### Références OEE

| Plage OEE   | Niveau         | Interprétation                              |
|-------------|----------------|---------------------------------------------|
| ≥ 85%       | 🟢 Classe mondiale | Fabrication de premier ordre               |
| 60–85%      | 🟡 Bon          | Possibilités d'amélioration                 |
| 40–60%      | 🟠 Moyen        | Pertes significatives présentes             |
| < 40%       | 🔟 Faible       | Interventions majeures nécessaires          |

---

## 📁 Structure du Projet

```
projet/
├── data/
│   ├── generate_data.py           # Phase 1 : Générateur de données synthétiques
│   ├── raw/                       # CSV/JSON générés (ignorés par git)
│   │   ├── production_events.csv
│   │   └── production_events.json
│   └── processed/                 # Sortie du pipeline (ignorée par git)
│       ├── oee_results.parquet/
│       ├── oee_results.csv/
│       ├── oee_monthly_summary.parquet/
│       └── oee_monthly_summary.csv/
├── spark/
│   ├── __init__.py
│   └── oee_pipeline.py           # Phase 2 : Pipeline ETL PySpark
├── docker-compose.yml             # Phase 3 : Cluster Hadoop + Spark
├── hadoop.env                     # Configuration HDFS
├── Makefile                       # Automatisation de construction
├── requirements.txt               # Dépendances Python
├── .gitignore
├── README.md                      # ← Vous êtes ici
└── README_powerbi.md              # Phase 4 : Guide de connexion Power BI
```

---

## 🐳 Services Docker

| Service         | Image                  | Ports          | Rôle                              |
|-----------------|------------------------|----------------|-----------------------------------|
| `namenode`      | apache/hadoop:3.3.6    | 9870, 9000     | NameNode HDFS + Interface Web     |
| `datanode`      | apache/hadoop:3.3.6    | —              | DataNode HDFS (stockage)          |
| `spark-master`  | bitnami/spark:3.5      | 8080, 7077     | Master Spark + Interface Web      |
| `spark-worker`  | bitnami/spark:3.5      | —              | Worker Spark (2G RAM, 2 cœurs)    |
| `spark-submit`  | bitnami/spark:3.5      | —              | Exécuteur unique du pipeline      |

---

## 🧠 Concepts Big Data

### RDD vs DataFrame

| Aspect            | RDD (Resilient Distributed Dataset)          | DataFrame (API structurée)                |
|-------------------|----------------------------------------------|-------------------------------------------|
| **Abstraction**   | Collection distribuée immuable de bas niveau | Tableau haut niveau avec schéma           |
| **Optimisation**  | Aucune optimisation automatique              | Optimiseur Catalyst + Moteur Tungsten     |
| **Sécurité types**| Vérification de type à la compilation        | Validation de schéma à l'exécution        |
| **Style API**     | Fonctionnel (map, filter, reduce)            | Déclaratif (DSL similaire à SQL)          |
| **Cas d'usage**   | Données non structurées, contrôle fin        | ETL structuré/semi-structuré              |
| **Performance**   | Plus lent (pas d'optimisation de requêtes)   | Plus rapide (plans d'exécution optimisés) |

> **Ce projet utilise DataFrames** pour toutes les transformations car :
> - L'application du schéma détecte les problèmes de qualité des données tôt
> - L'optimiseur Catalyst génère des plans d'exécution physique efficaces
> - Support natif de Parquet pour le stockage columnar
> - L'API compatible SQL simplifie l'expression de la formule OEE

### DAG (Graphe Orienté Acyclique)

Spark construit un **DAG de transformations** avant d'exécuter tout calcul :

```
Lire CSV → Filtrer Nuls → Caster Types → Calculer Disponibilité
                                         → Calculer Performance
                                         → Calculer Qualité
                                               ↓
                                         Multiplier (OEE)
                                               ↓
                                      ┌────────┴────────┐
                                      ▼                  ▼
                                 Écrire Parquet     Écrire CSV
```

- **Évaluation paresseuse** : les transformations sont enregistrées mais pas exécutées jusqu'à ce qu'une **action** (`.count()`, `.write()`, `.show()`) déclenche l'exécution
- **Limites d'étapes** : se produisent lors des **shuffles** (groupBy, join) — Spark divise le DAG en étapes
- **Inspection du pipeline** : utiliser `df.explain(extended=True)` pour voir les plans logiques et physiques optimisés

### ETL (Extract, Transform, Load)

Ce pipeline suit le modèle ETL classique :

1. **Extract** : Lire le CSV brut depuis HDFS ou le système de fichiers local
2. **Transform** : Nettoyer les nuls, caster les types, calculer les métriques OEE, agréger les résumés mensuels
3. **Load** : Écrire les résultats en Parquet (analytique) et CSV (Power BI)

### Calcul Distribué

```
┌─────────────┐     ┌─────────────────────────────────────┐
│ Pilote Spark │────▶│ Gestionnaire de cluster (ou YARN)  │
│ (plan DAG)   │     └──────────┬──────────────────────────┘
└─────────────┘                │
                    ┌──────────▼──────────┐
                    │ Ordonnanceur de     │
                    │ tâches              │
                    └──┬───────┬───────┬──┘
                       │       │       │
                  ┌────▼─┐ ┌──▼──┐ ┌──▼────┐
                  │Tâche1│ │Tâche2│ │Tâche 3 │  ← Parallélisme par partition
                  │Part-0│ │Pt-1 │ │Part-2 │
                  └──────┘ └─────┘ └───────┘
```

- **Partitionnement des données** : le CSV d'entrée est divisé en partitions traitées en parallèle
- **`local[*]`** : utilise tous les cœurs CPU disponibles sur une seule machine
- **Passage à l'échelle** : le même code fonctionne sur un cluster multi-nœuds en changeant l'URL du maître
- **Tolérance aux pannes** : les partitions perdues sont recalculées à partir de la lignée du DAG

---

## 📋 Cibles Makefile

```bash
make all           # Pipeline complet : générer → upload → exécuter
make generate      # Générer 10K événements de production synthétiques
make upload-data   # Démarrer Docker + uploader les données vers HDFS
make run-pipeline  # Exécuter le pipeline PySpark via Docker
make run-local     # Exécuter le pipeline PySpark en local (sans Docker)
make docker-up     # Démarrer le cluster Hadoop + Spark
make docker-down   # Arrêter et supprimer tous les conteneurs
make clean         # Supprimer toutes les données générées
make help          # Afficher les cibles disponibles
```

---

## ⚙️ Configuration

### Variables d'Environnement

| Variable               | Valeur par défaut                          | Description                    |
|------------------------|--------------------------------------------|--------------------------------|
| `HDFS_INPUT_PATH`      | `hdfs://namenode:9000/oee/raw/...`       | Chemin source HDFS             |
| `LOCAL_INPUT_PATH`     | `data/raw/production_events.csv`         | Chemin de repli local          |
| `SPARK_DRIVER_MEMORY`  | `2g`                                     | Mémoire du pilote Spark        |
| `SPARK_SHUFFLE_PARTS`  | `8`                                      | Nombre de partitions de shuffle|

### Paramètres de Génération des Données

| Paramètre       | Valeur     | Description                             |
|-----------------|------------|-----------------------------------------|
| `MACHINES`      | 5          | M001–M005                               |
| `SHIFTS`        | 4          | morning, afternoon, night, weekend      |
| `NUM_DAYS`      | 30         | 1–30 janvier 2025                       |
| `TARGET_EVENTS` | 10 000     | Nombre total d'événements de production |
| `SEED`          | 42         | Graine aléatoire reproductible          |

---

## 📚 Références

- [Documentation Apache Spark](https://spark.apache.org/docs/latest/)
- [Guide d'architecture HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Fondation OEE](https://www.oee.com/)
- [Power BI Desktop](https://powerbi.microsoft.com/desktop/)
- [Spécification du format Parquet](https://parquet.apache.org/documentation/latest/)

---

## 📄 Licence

Projet académique — Tous droits réservés.
