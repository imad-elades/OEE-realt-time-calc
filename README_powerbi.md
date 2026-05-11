# 📊 Power BI — Guide de Connexion

## Connexion de Power BI Desktop aux Résultats OEE

Ce guide vous accompagne dans l'importation des résultats d'analyse OEE dans Power BI Desktop pour la visualisation et la création de tableaux de bord.

---

## Prérequis

- **Power BI Desktop** (dernière version) — [Télécharger](https://powerbi.microsoft.com/desktop/)
- Le pipeline a été exécuté (`make all` ou `make run-local`)
- Le fichier existe : `data/processed/oee_results.csv/`

---

## Étape 1 : Localiser le Fichier de Sortie

Après avoir exécuté le pipeline, les résultats OEE sont exportés vers :

```
data/processed/oee_results.csv/      ← Répertoire de sortie Spark
data/processed/_oee_results.csv.csv  ← Copie propre d'un seul fichier
```

> **Astuce :** Utilisez le fichier `_oee_results.csv.csv` pour Power BI — c'est un fichier CSV plat unique.

---

## Étape 2 : Importer le CSV dans Power BI

1. Ouvrez **Power BI Desktop**
2. Cliquez sur **Accueil → Obtenir des données → Texte/CSV**
3. Naviguez vers : `data/processed/_oee_results.csv.csv`
4. Dans la fenêtre d'aperçu, vérifiez :
   - **Délimiteur** : Virgule
   - **Encodage** : UTF-8
   - **Détection du type de données** : Basé sur les 200 premières lignes

5. Cliquez sur **Transformer les données** (recommandé) ou **Charger**

---

## Étape 3 : Vérifier les Types de Données des Colonnes

Dans l'éditeur Power Query, assurez-vous que ces types de données sont définis :

| Colonne             | Type attendu      | Description                          |
|---------------------|-------------------|--------------------------------------|
| `machine_id`        | Texte             | Identifiant machine (M001–M005)      |
| `shift_id`          | Texte             | Nom de l'équipe (morning, etc.)      |
| `date`              | Date              | Date de production (AAAA-MM-JJ)      |
| `availability_pct`  | Nombre décimal    | Taux de disponibilité % (0–100)      |
| `performance_pct`   | Nombre décimal    | Taux de performance % (0–100)        |
| `quality_pct`       | Nombre décimal    | Taux de qualité % (0–100)            |
| `oee_pct`           | Nombre décimal    | OEE global % (0–100)                 |

Pour changer un type de colonne :
- Cliquez sur l'icône **ABC/123** dans l'en-tête de la colonne
- Sélectionnez le type correct dans la liste déroulante

---

## Étape 4 : Créer des Visualisations

### Graphiques Recommandés

1. **OEE par Machine** (Graphique à barres groupées)
   - Axe : `machine_id`
   - Valeurs : Moyenne de `oee_pct`

2. **Tendance OEE dans le Temps** (Graphique en courbes)
   - Axe : `date`
   - Valeurs : Moyenne de `oee_pct`
   - Légende : `machine_id`

3. **Répartition Disponibilité / Performance / Qualité** (Barres empilées)
   - Axe : `machine_id`
   - Valeurs : Moyenne de `availability_pct`, `performance_pct`, `quality_pct`

4. **Comparaison des Équipes** (Matrice / Carte thermique)
   - Lignes : `machine_id`
   - Colonnes : `shift_id`
   - Valeurs : Moyenne de `oee_pct`

5. **Cartes KPI**
   - Moyenne globale OEE
   - Meilleure machine
   - Pire équipe

---

## Étape 5 : Optionnel — Connexion à Parquet

Pour de meilleures performances avec de grands ensembles de données, utilisez le fichier Parquet :

1. **Accueil → Obtenir des données → Plus → Parquet**
2. Naviguez vers : `data/processed/oee_results.parquet/`
3. Sélectionnez tous les fichiers `.parquet` dans le dossier

> **Remarque :** Parquet nécessite Power BI Desktop version 2.108 ou ultérieure.

---

## Référence des Colonnes

Toutes les valeurs en pourcentage sont sur une **échelle de 0–100** (pas 0–1), arrondies à 2 décimales.

```
machine_id        → Identifiant machine (M001, M002, M003, M004, M005)
shift_id          → Type d'équipe (morning, afternoon, night, weekend)
date              → Date de production
availability_pct  → Taux de disponibilité (0–100%)
performance_pct   → Taux de performance (0–100%)
quality_pct       → Taux de qualité (0–100%)
oee_pct           → Efficacité Globale des Équipements (0–100%)
```

---

## Dépannage

| Problème                        | Solution                                                |
|----------------------------------|---------------------------------------------------------|
| Fichier introuvable              | Exécutez `make all` ou `make run-local` d'abord         |
| Mauvais délimiteur               | Assurez-vous que la virgule (`,`) est sélectionnée      |
| Date non reconnue                | Définir manuellement le type de colonne sur Date        |
| Valeurs affichées 0–1 au lieu de 0–100 | Relancez le pipeline — valeurs pré-multipliées ×100 |
| Plusieurs fichiers CSV dans le dossier | Utilisez `_oee_results.csv.csv` (fichier unique fusionné) |
