# Airflow Basics (pipeline de données météo)

## Description
Ce projet implémente un pipeline de données météorologiques complet en utilisant Apache Airflow. Il collecte, traite et analyse des données météorologiques en temps réel pour plusieurs villes européennes, puis génère des rapports détaillés avec visualisations.

## Fonctionnalités
- **Collecte de données**: Récupération des données météorologiques actuelles via l'API OpenWeatherMap
- **Traitement des données**: Nettoyage, transformation et préparation des données pour analyse
- **Analyse avancée**: Calcul de statistiques, détection d'anomalies, et reconnaissance de tendances
- **Génération de rapports**: Création de rapports HTML interactifs avec visualisations graphiques
- **Orchestration automatique**: Exécution quotidienne via Airflow avec gestion des dépendances

## Architecture
Le DAG Airflow (`meteo_pipeline`) est constitué de 4 tâches principales:
1. `fetch_weather_data`: Collecte des données depuis l'API
2. `process_data`: Nettoyage et préparation des données
3. `analyze_data`: Analyse statistique des données
4. `create_report`: Génération du rapport final avec visualisations

## Technologies utilisées
- **Apache Airflow**: Orchestration du pipeline
- **Python**: Langage principal avec bibliothèques d'analyse de données
- **pandas & NumPy**: Manipulation et analyse de données
- **Matplotlib & Seaborn**: Visualisation de données
- **Jinja2**: Création de template HTML pour les rapports
- **Docker**: Conteneurisation de l'environnement

## Installation
```bash
# Cloner le dépôt
git clone https://github.com/Germain-D/aiflow_basics.git

# Lancer l'environnement Airflow avec Docker
cd meteo-pipeline
docker-compose up -d

# Accéder à l'interface Airflow
# Ouvrir http://localhost:8080 dans votre navigateur
# Login: airflow / Password: airflow
```

## Configuration
- Obtenez une clé API gratuite sur [OpenWeatherMap](https://openweathermap.org/api)
- Modifiez la variable `api_key` dans le fichier `dags/meteo.py`
- Personnalisez la liste des villes dans la variable `cities`

## Résultats
Le pipeline génère quotidiennement:
- Données brutes au format JSON
- Données traitées au format CSV
- Résultats d'analyse au format JSON
- Rapport HTML complet avec visualisations
- Les fichiers sont stockés dans le répertoire `./dags/reports/`
