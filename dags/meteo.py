import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'Germain',
    'depends_on_past': False,
    'start_date': dt.datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG(
    'meteo_pipeline',
    default_args=default_args,
    description='Un DAG simple pour traiter des données météo',
    schedule_interval=dt.timedelta(days=1),
)

def fetch_weather_data(**kwargs):
    """
    Récupère les données météorologiques actuelles pour une liste de villes
    et les stocke dans un fichier JSON pour traitement ultérieur.
    
    Arguments:
        **kwargs: Arguments contextuels fournis par Airflow
    
    Returns:
        str: Chemin vers le fichier JSON contenant les données météo
    """
    import requests
    import json
    import os
    from datetime import datetime
    
    # Récupérer le contexte d'exécution Airflow
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Paramètres API
    api_key = os.getenv("API_KEY")
    cities = ["Paris", "London", "Berlin", "Madrid", "Rome"]  # Liste des villes à surveiller
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    
    # Créer le répertoire de données si nécessaire
    data_dir = "./tmp/weather_data"
    os.makedirs(data_dir, exist_ok=True)
    
    # Fichier de sortie
    output_file = f"{data_dir}/weather_data_{date_str}.json"
    
    # Récupérer les données pour chaque ville
    weather_data = {}
    
    for city in cities:
        try:
            # Paramètres de la requête
            params = {
                "q": city,
                "appid": api_key,
                "units": "metric"  # Pour obtenir les températures en Celsius
            }
            
            # Effectuer la requête API
            response = requests.get(base_url, params=params)
            
            # Vérifier le statut de la réponse
            if response.status_code == 200:
                city_data = response.json()
                weather_data[city] = {
                    "temperature": city_data["main"]["temp"],
                    "humidity": city_data["main"]["humidity"],
                    "pressure": city_data["main"]["pressure"],
                    "weather_condition": city_data["weather"][0]["main"],
                    "weather_description": city_data["weather"][0]["description"],
                    "wind_speed": city_data["wind"]["speed"],
                    "timestamp": datetime.now().isoformat()
                }
                print(f"Données météo récupérées pour {city}")
            else:
                print(f"Erreur lors de la récupération des données pour {city}: {response.status_code}")
                weather_data[city] = {"error": f"HTTP Error {response.status_code}"}
                
        except Exception as e:
            print(f"Exception lors de la récupération des données pour {city}: {str(e)}")
            weather_data[city] = {"error": str(e)}
    
    # Enregistrer les données récupérées dans un fichier JSON
    with open(output_file, 'w') as f:
        json.dump(weather_data, f, indent=4)
    
    print(f"Données météo enregistrées dans {output_file}")
    
    # Passer le chemin du fichier à la tâche suivante via XCom
    ti.xcom_push(key='weather_data_file', value=output_file)
    
    return output_file


def process_data(**kwargs):
    """
    Traite les données météorologiques brutes:
    - Nettoie les données
    - Calcule des statistiques de base
    - Transforme en format exploitable pour l'analyse
    - Exporte vers un fichier CSV
    
    Arguments:
        **kwargs: Arguments contextuels fournis par Airflow
    
    Returns:
        str: Chemin vers le fichier CSV traité
    """
    import json
    import pandas as pd
    import os
    from datetime import datetime
    
    # Récupérer le contexte d'exécution Airflow
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Récupérer le chemin du fichier de données via XCom
    weather_data_file = ti.xcom_pull(task_ids='fetch_weather_data', key='weather_data_file')
    
    if not weather_data_file or not os.path.exists(weather_data_file):
        raise FileNotFoundError(f"Le fichier de données météo n'a pas été trouvé: {weather_data_file}")
    
    print(f"Traitement du fichier de données: {weather_data_file}")
    
    # Lire les données JSON
    with open(weather_data_file, 'r') as f:
        weather_data = json.load(f)
    
    # Créer une liste pour stocker les données nettoyées
    processed_data = []
    
    # Traiter les données de chaque ville
    for city, data in weather_data.items():
        # Vérifier si nous avons des données valides ou une erreur
        if 'error' in data:
            print(f"Données ignorées pour {city} en raison d'une erreur précédente")
            continue
        
        try:
            # Extraire les informations pertinentes
            processed_entry = {
                'city': city,
                'date': date_str,
                'temperature': data.get('temperature'),
                'humidity': data.get('humidity'),
                'pressure': data.get('pressure'),
                'weather_condition': data.get('weather_condition'),
                'weather_description': data.get('weather_description'),
                'wind_speed': data.get('wind_speed'),
                # Calculs dérivés
                'feels_like_temp': calculate_feels_like(data.get('temperature', 0), 
                                                       data.get('humidity', 0), 
                                                       data.get('wind_speed', 0)),
                'is_rainy': 'Rain' in data.get('weather_condition', ''),
                'temperature_category': categorize_temperature(data.get('temperature', 0))
            }
            processed_data.append(processed_entry)
            print(f"Données traitées pour {city}")
            
        except Exception as e:
            print(f"Erreur lors du traitement des données pour {city}: {str(e)}")
    
    # Convertir en DataFrame pandas
    if not processed_data:
        raise ValueError("Aucune donnée valide à traiter")
    
    df = pd.DataFrame(processed_data)
    
    # Effectuer des transformations sur le DataFrame
    # 1. Tri des données
    df = df.sort_values(['date', 'city'])
    
    # 2. Vérifier et gérer les valeurs manquantes
    df = df.fillna({
        'temperature': df['temperature'].mean(),
        'humidity': df['humidity'].mean(),
        'pressure': df['pressure'].mean(),
        'wind_speed': df['wind_speed'].mean()
    })
    
    # 3. Arrondir les valeurs numériques
    numeric_columns = ['temperature', 'humidity', 'pressure', 'wind_speed', 'feels_like_temp']
    df[numeric_columns] = df[numeric_columns].round(2)
    
    # Créer le répertoire de sortie si nécessaire
    output_dir = "./tmp/weather_processed"
    os.makedirs(output_dir, exist_ok=True)
    
    # Enregistrer dans un fichier CSV
    output_file = f"{output_dir}/weather_processed_{date_str}.csv"
    df.to_csv(output_file, index=False)
    
    print(f"Données traitées enregistrées dans {output_file}")
    
    # Passer le chemin du fichier et quelques statistiques à la tâche suivante
    ti.xcom_push(key='processed_data_file', value=output_file)
    
    # Calculer quelques statistiques de base
    stats = {
        'avg_temperature': df['temperature'].mean(),
        'min_temperature': df['temperature'].min(),
        'max_temperature': df['temperature'].max(),
        'avg_humidity': df['humidity'].mean(),
        'cities_count': len(df['city'].unique()),
        'rainy_cities': df[df['is_rainy']]['city'].tolist()
    }
    ti.xcom_push(key='weather_stats', value=stats)
    
    return output_file

# Fonctions utilitaires pour les calculs dérivés
def calculate_feels_like(temp, humidity, wind_speed):
    """
    Calcul simplifié de la température ressentie basé sur la température, l'humidité et la vitesse du vent
    """
    # Formule simplifiée pour démonstration
    feels_like = temp
    
    # Ajustement pour l'humidité (augmente la sensation de chaleur)
    if temp > 20:  # Si température > 20°C
        feels_like += (humidity - 50) / 5 if humidity > 50 else 0
    
    # Ajustement pour le vent (effet refroidissant)
    feels_like -= min(wind_speed * 0.5, 5)  # Limiter l'effet à 5°C max
    
    return round(feels_like, 1)

def categorize_temperature(temp):
    """
    Catégorisation de la température
    """
    if temp < 0:
        return "Très froid"
    elif temp < 10:
        return "Froid"
    elif temp < 20:
        return "Frais"
    elif temp < 27:
        return "Tempéré"
    elif temp < 32:
        return "Chaud"
    else:
        return "Très chaud"

def analyze_data(**kwargs):
    """
    Analyse les données météorologiques traitées:
    - Identifie les tendances et anomalies
    - Calcule des statistiques avancées
    - Prépare les données pour la visualisation
    
    Arguments:
        **kwargs: Arguments contextuels fournis par Airflow
    
    Returns:
        dict: Résultats de l'analyse
    """
    import pandas as pd
    import numpy as np
    import json
    import os
    from datetime import datetime, timedelta
    
    # Récupérer le contexte d'exécution Airflow
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Récupérer le chemin du fichier de données traitées via XCom
    processed_data_file = ti.xcom_pull(task_ids='process_data', key='processed_data_file')
    
    if not processed_data_file or not os.path.exists(processed_data_file):
        raise FileNotFoundError(f"Le fichier de données traitées n'a pas été trouvé: {processed_data_file}")
    
    print(f"Analyse du fichier de données traitées: {processed_data_file}")
    
    # Charger les données traitées
    df = pd.read_csv(processed_data_file)
    
    # Récupérer les statistiques précédemment calculées
    weather_stats = ti.xcom_pull(task_ids='process_data', key='weather_stats')
    
    # --- ANALYSE AVANCÉE ---
    
    # 1. Analyse comparative entre les villes
    city_comparison = df.groupby('city').agg({
        'temperature': ['mean', 'min', 'max', 'std'],
        'humidity': ['mean', 'min', 'max'],
        'pressure': ['mean'],
        'wind_speed': ['mean', 'max']
    }).reset_index()
    
    # Renommer les colonnes pour plus de clarté
    city_comparison.columns = ['_'.join(col).strip('_') for col in city_comparison.columns.values]
    
    # 2. Détection des valeurs aberrantes (méthode IQR)
    q1 = df['temperature'].quantile(0.25)
    q3 = df['temperature'].quantile(0.75)
    iqr = q3 - q1
    outliers = df[(df['temperature'] < (q1 - 1.5 * iqr)) | (df['temperature'] > (q3 + 1.5 * iqr))]
    
    # 3. Analyse des corrélations
    correlation_matrix = df[['temperature', 'humidity', 'pressure', 'wind_speed']].corr()
    
    # 4. Catégorisation avancée des conditions climatiques
    def categorize_weather(row):
        if row['is_rainy']:
            if row['wind_speed'] > 10:
                return "Tempête"
            else:
                return "Pluvieux"
        elif row['temperature'] > 28:
            if row['humidity'] > 70:
                return "Chaud et humide"
            else:
                return "Chaud et sec"
        elif row['temperature'] < 5:
            return "Froid"
        else:
            if row['wind_speed'] > 15:
                return "Venteux"
            else:
                return "Modéré"
    
    df['weather_category'] = df.apply(categorize_weather, axis=1)
    weather_categories = df.groupby(['city', 'weather_category']).size().reset_index(name='count')
    
    # 5. Comparaison avec la veille (simulée pour cet exemple)
    # Dans un système réel, nous récupérerions les données historiques depuis une base de données
    simulated_previous_day = df.copy()
    simulated_previous_day['temperature'] = simulated_previous_day['temperature'] * (1 + np.random.normal(0, 0.05, len(df)))
    temperature_change = (df['temperature'].mean() - simulated_previous_day['temperature'].mean())
    
    # --- PRÉPARATION DES RÉSULTATS ---
    
    # Créer le répertoire de sortie si nécessaire
    output_dir = "./tmp/weather_analysis"
    os.makedirs(output_dir, exist_ok=True)
    
    # Enregistrer les résultats dans un fichier JSON
    analysis_results = {
        'date': date_str,
        'city_comparison': city_comparison.to_dict(orient='records'),
        'outliers': outliers[['city', 'temperature', 'weather_condition']].to_dict(orient='records'),
        'correlation': {
            'temp_humidity': correlation_matrix.loc['temperature', 'humidity'],
            'temp_pressure': correlation_matrix.loc['temperature', 'pressure'],
            'temp_wind': correlation_matrix.loc['temperature', 'wind_speed'],
        },
        'weather_categories': weather_categories.to_dict(orient='records'),
        'temperature_change': float(temperature_change),
        'insights': [
            f"La température moyenne est de {weather_stats['avg_temperature']:.1f}°C",
            f"La ville la plus chaude est {df.loc[df['temperature'].idxmax()]['city']} avec {df['temperature'].max():.1f}°C",
            f"La ville la plus froide est {df.loc[df['temperature'].idxmin()]['city']} avec {df['temperature'].min():.1f}°C",
            f"L'humidité moyenne est de {weather_stats['avg_humidity']:.1f}%",
            f"{len(weather_stats['rainy_cities'])} villes ont signalé des précipitations: {', '.join(weather_stats['rainy_cities'])}" if weather_stats['rainy_cities'] else "Aucune ville n'a signalé de précipitations",
            f"La température a {'augmenté' if temperature_change > 0 else 'diminué'} de {abs(temperature_change):.1f}°C par rapport à la veille"
        ]
    }
    
    # Enregistrer les résultats dans un fichier JSON
    analysis_file = f"{output_dir}/weather_analysis_{date_str}.json"
    with open(analysis_file, 'w') as f:
        json.dump(analysis_results, f, indent=4)
    
    print(f"Résultats de l'analyse enregistrés dans {analysis_file}")
    
    # Passer les résultats à la tâche suivante
    ti.xcom_push(key='analysis_file', value=analysis_file)
    ti.xcom_push(key='analysis_results', value=analysis_results)
    
    return analysis_results


def create_report(**kwargs):
    """
    Génère un rapport détaillé basé sur l'analyse des données météorologiques:
    - Crée des visualisations
    - Formate les informations clés
    - Génère un rapport PDF ou HTML
    
    Arguments:
        **kwargs: Arguments contextuels fournis par Airflow
    
    Returns:
        str: Chemin vers le rapport généré
    """
    import pandas as pd
    import json
    import os
    import matplotlib.pyplot as plt
    import seaborn as sns
    from datetime import datetime
    import jinja2
    import numpy as np
    
    # Récupérer le contexte d'exécution Airflow
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Récupérer les résultats de l'analyse et le fichier de données traitées
    analysis_results = ti.xcom_pull(task_ids='analyze_data', key='analysis_results')
    processed_data_file = ti.xcom_pull(task_ids='process_data', key='processed_data_file')
    
    if not processed_data_file or not os.path.exists(processed_data_file):
        raise FileNotFoundError(f"Le fichier de données traitées n'a pas été trouvé: {processed_data_file}")
    
    if not analysis_results:
        raise ValueError("Les résultats d'analyse n'ont pas été trouvés")
    
    print(f"Création du rapport à partir des données: {processed_data_file}")
    
    # Charger les données traitées
    df = pd.read_csv(processed_data_file)

        # Ajouter la catégorisation des conditions climatiques
    def categorize_weather(row):
        if row.get('is_rainy') == True:
            if row['wind_speed'] > 10:
                return "Tempête"
            else:
                return "Pluvieux"
        elif row['temperature'] > 28:
            if row['humidity'] > 70:
                return "Chaud et humide"
            else:
                return "Chaud et sec"
        elif row['temperature'] < 5:
            return "Froid"
        else:
            if row['wind_speed'] > 15:
                return "Venteux"
            else:
                return "Modéré"
    
    # Ajouter la colonne manquante
    df['weather_category'] = df.apply(categorize_weather, axis=1)
    
    # Créer le répertoire pour les visualisations et le rapport
    report_dir = "/opt/airflow/dags/reports"
    os.makedirs(report_dir, exist_ok=True)
    viz_dir = f"{report_dir}/visualizations"
    os.makedirs(viz_dir, exist_ok=True)
    
    # --- GÉNÉRER LES VISUALISATIONS ---
    
    # 1. Graphique des températures par ville
    plt.figure(figsize=(10, 6))
    sns.barplot(x='city', y='temperature', data=df, palette='viridis')
    plt.title('Températures par ville')
    plt.xlabel('Ville')
    plt.ylabel('Température (°C)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    temp_chart_path = f"{viz_dir}/temperature_chart.png"
    plt.savefig(temp_chart_path)
    plt.close()
    
    # 2. Comparaison humidité/température
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='temperature', y='humidity', hue='city', data=df, s=100)
    plt.title('Relation entre température et humidité')
    plt.xlabel('Température (°C)')
    plt.ylabel('Humidité (%)')
    plt.tight_layout()
    humidity_chart_path = f"{viz_dir}/humidity_temp_chart.png"
    plt.savefig(humidity_chart_path)
    plt.close()
    
    # 3. Conditions météo par ville
    plt.figure(figsize=(12, 7))
    weather_count = df.groupby(['city', 'weather_condition']).size().reset_index(name='count')
    sns.barplot(x='city', y='count', hue='weather_condition', data=weather_count)
    plt.title('Conditions météorologiques par ville')
    plt.xlabel('Ville')
    plt.ylabel('Fréquence')
    plt.xticks(rotation=45)
    plt.legend(title='Condition météo', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    conditions_chart_path = f"{viz_dir}/weather_conditions_chart.png"
    plt.savefig(conditions_chart_path)
    plt.close()
    
    # 4. Heatmap de corrélation
    plt.figure(figsize=(8, 6))
    correlation = df[['temperature', 'humidity', 'pressure', 'wind_speed']].corr()
    mask = np.triu(correlation)
    sns.heatmap(correlation, annot=True, cmap='coolwarm', vmin=-1, vmax=1, mask=mask)
    plt.title('Corrélation entre les variables météorologiques')
    plt.tight_layout()
    corr_chart_path = f"{viz_dir}/correlation_chart.png"
    plt.savefig(corr_chart_path)
    plt.close()
    
    # --- GÉNÉRER LE RAPPORT HTML ---
    
    # Préparer les données pour le template
    city_data = []
    for city in df['city'].unique():
        city_df = df[df['city'] == city]
        city_info = {
            'name': city,
            'temperature': city_df['temperature'].mean(),
            'humidity': city_df['humidity'].mean(),
            'pressure': city_df['pressure'].mean(),
            'wind_speed': city_df['wind_speed'].mean(),
            'conditions': city_df['weather_description'].iloc[0],
            'category': city_df['weather_category'].iloc[0]
        }
        city_data.append(city_info)
    
    # Créer le template HTML
    template_str = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Rapport Météorologique - {{ date }}</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
            h1, h2, h3 { color: #2c3e50; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { text-align: center; margin-bottom: 30px; padding-bottom: 20px; border-bottom: 1px solid #eee; }
            .section { margin-bottom: 30px; }
            .insights { background-color: #f8f9fa; padding: 20px; border-radius: 5px; }
            .insights ul { padding-left: 20px; }
            .city-cards { display: flex; flex-wrap: wrap; justify-content: space-between; }
            .city-card { width: 30%; margin-bottom: 20px; padding: 15px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
            .viz-section { display: flex; flex-wrap: wrap; justify-content: center; }
            .viz-item { margin: 10px; text-align: center; }
            img { max-width: 100%; height: auto; border-radius: 5px; }
            .footer { text-align: center; margin-top: 50px; padding-top: 20px; border-top: 1px solid #eee; color: #7f8c8d; }
            table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
            table, th, td { border: 1px solid #ddd; }
            th, td { padding: El0px; text-align: left; }
            th { background-color: #f2f2f2; }
            tr:nth-child(even) { background-color: #f9f9f9; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Rapport Météorologique</h1>
                <h2>{{ date }}</h2>
            </div>
            
            <div class="section insights">
                <h2>Insights Clés</h2>
                <ul>
                    {% for insight in insights %}
                    <li>{{ insight }}</li>
                    {% endfor %}
                </ul>
            </div>
            
            <div class="section">
                <h2>Résumé par Ville</h2>
                <div class="city-cards">
                    {% for city in city_data %}
                    <div class="city-card">
                        <h3>{{ city.name }}</h3>
                        <p><strong>Température:</strong> {{ "%.1f"|format(city.temperature) }}°C</p>
                        <p><strong>Humidité:</strong> {{ "%.1f"|format(city.humidity) }}%</p>
                        <p><strong>Pression:</strong> {{ "%.1f"|format(city.pressure) }} hPa</p>
                        <p><strong>Vent:</strong> {{ "%.1f"|format(city.wind_speed) }} m/s</p>
                        <p><strong>Conditions:</strong> {{ city.conditions }}</p>
                        <p><strong>Catégorie:</strong> {{ city.category }}</p>
                    </div>
                    {% endfor %}
                </div>
            </div>
            
            <div class="section">
                <h2>Visualisations</h2>
                <div class="viz-section">
                    <div class="viz-item">
                        <h3>Températures par ville</h3>
                        <img src="visualizations/temperature_chart.png" alt="Températures par ville">
                    </div>
                    <div class="viz-item">
                        <h3>Relation température-humidité</h3>
                        <img src="visualizations/humidity_temp_chart.png" alt="Relation température-humidité">
                    </div>
                    <div class="viz-item">
                        <h3>Conditions météorologiques</h3>
                        <img src="visualizations/weather_conditions_chart.png" alt="Conditions météorologiques">
                    </div>
                    <div class="viz-item">
                        <h3>Corrélations</h3>
                        <img src="visualizations/correlation_chart.png" alt="Corrélations">
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h2>Détails des Analyses</h2>
                <h3>Comparaison entre villes</h3>
                <table>
                    <tr>
                        <th>Ville</th>
                        <th>Temp. moyenne</th>
                        <th>Temp. min</th>
                        <th>Temp. max</th>
                        <th>Humidité moyenne</th>
                        <th>Vent moyen</th>
                    </tr>
                    {% for city in city_comparison %}
                    <tr>
                        <td>{{ city.city }}</td>
                        <td>{{ "%.1f"|format(city.temperature_mean) }}°C</td>
                        <td>{{ "%.1f"|format(city.temperature_min) }}°C</td>
                        <td>{{ "%.1f"|format(city.temperature_max) }}°C</td>
                        <td>{{ "%.1f"|format(city.humidity_mean) }}%</td>
                        <td>{{ "%.1f"|format(city.wind_speed_mean) }} m/s</td>
                    </tr>
                    {% endfor %}
                </table>
            </div>
            
            <div class="footer">
                <p>Rapport généré automatiquement le {{ timestamp }}</p>
                <p>Pipeline Airflow de traitement des données météorologiques</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Compiler le template
    template = jinja2.Template(template_str)
    
    # Générer le rapport HTML
    html_content = template.render(
        date=date_str,
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        insights=analysis_results['insights'],
        city_data=city_data,
        city_comparison=analysis_results['city_comparison']
    )
    
    # Enregistrer le rapport HTML
    report_file = f"{report_dir}/weather_report_{date_str}.html"
    with open(report_file, 'w') as f:
        f.write(html_content)
    
    print(f"Rapport météorologique créé: {report_file}")
    
    # Partager le chemin du rapport HTML
    ti.xcom_push(key='report_file', value=report_file)
    
    return report_file

# Définir les tâches
t1 = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag,
)

t4 = PythonOperator(
    task_id='create_report',
    python_callable=create_report,
    dag=dag,
)


# Définir les dépendances
t1 >> t2 >> t3 >> t4