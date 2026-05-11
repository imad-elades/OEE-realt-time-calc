# ============================================================================
# OEE Analytics - Makefile
# ============================================================================
# Cibles :
#   make generate      - Générer des données de production synthétiques
#   make upload-data   - Uploader les données brutes vers HDFS
#   make run-pipeline  - Exécuter le pipeline PySpark OEE via Docker
#   make run-local     - Exécuter le pipeline PySpark OEE en local (sans Docker)
#   make all           - Pipeline complet : générer + upload + exécuter
#   make clean         - Supprimer les données générées
#   make docker-up     - Démarrer le cluster BigData
#   make docker-down   - Arrêter et supprimer tous les conteneurs
# ============================================================================

.PHONY: all generate upload-data run-pipeline run-local clean docker-up docker-down dashboard help

# Cible par défaut
all: generate upload-data run-pipeline dashboard
	@echo ""
	@echo "=============================================="
	@echo "  [OK] Pipeline OEE Analytics terminé !"
	@echo "  Résultats : data/processed/oee_results.csv"
	@echo "  Tableau de bord : data/processed/oee_dashboard.html"
	@echo "  Power BI Desktop lancé"
	@echo "=============================================="

# --------------------------------------------------------------------------
# Phase 1 : Générer les données synthétiques
# --------------------------------------------------------------------------
generate:
	@echo "Génération des données d'événements de production..."
	python data/generate_data.py
	@echo "[OK] Génération des données terminée"

# --------------------------------------------------------------------------
# Phase 2 : Démarrer le cluster Docker et uploader vers HDFS
# --------------------------------------------------------------------------
docker-up:
	@echo "Démarrage du cluster BigData..."
	docker-compose up -d bigdata-node
	@echo "Attente que HDFS soit prêt..."
	@sleep 60
	@echo "[OK] Le cluster est en cours d'exécution"
	@echo "   Interface NameNode : http://localhost:9870"
	@echo "   Interface Spark :    http://localhost:8080"

upload-data: docker-up
	@echo "Création du répertoire HDFS..."
	docker exec oee-bigdata hdfs dfs -mkdir -p /oee/raw/
	@echo "Upload des données vers HDFS..."
	docker exec oee-bigdata hdfs dfs -put -f /data/raw/production_events.csv /oee/raw/
	@echo "[OK] Données uploadées vers hdfs://namenode:9000/oee/raw/"
	docker exec oee-bigdata hdfs dfs -ls /oee/raw/

# --------------------------------------------------------------------------
# Phase 3 : Exécuter le pipeline PySpark
# --------------------------------------------------------------------------
run-pipeline:
	@echo "Exécution du pipeline OEE via Spark..."
	docker-compose run --rm --profile pipeline spark-submit
	@echo "[OK] Exécution du pipeline terminée"

run-local:
	@echo "Exécution du pipeline OEE en local..."
	python spark/oee_pipeline.py
	@echo "[OK] Exécution locale du pipeline terminée"

# --------------------------------------------------------------------------
# Phase 4 : Tableau de bord Power BI
# --------------------------------------------------------------------------
dashboard:
	@echo "Génération du tableau de bord et lancement de Power BI..."
	python powerbi/create_dashboard.py
	@echo "[OK] Tableau de bord prêt"

# --------------------------------------------------------------------------
# Utilitaires
# --------------------------------------------------------------------------
clean:
	@echo "Nettoyage des données générées..."
	rm -rf data/raw/*.csv data/raw/*.json
	rm -rf data/processed/*
	@echo "[OK] Nettoyage terminé"

docker-down:
	@echo "Arrêt des conteneurs Docker..."
	docker-compose --profile pipeline down -v
	@echo "[OK] Tous les conteneurs arrêtés et supprimés"

help:
	@echo "OEE Analytics - Cibles disponibles :"
	@echo "  make all           Pipeline complet (générer + upload + exécuter + tableau de bord)"
	@echo "  make generate      Générer des données de production synthétiques"
	@echo "  make upload-data   Uploader les données brutes vers HDFS"
	@echo "  make run-pipeline  Exécuter le pipeline PySpark (Docker)"
	@echo "  make run-local     Exécuter le pipeline PySpark (local, sans Docker)"
	@echo "  make dashboard     Générer le tableau de bord HTML + ouvrir Power BI"
	@echo "  make docker-up     Démarrer le cluster BigData"
	@echo "  make docker-down   Arrêter tous les conteneurs"
	@echo "  make clean         Supprimer les données générées"

