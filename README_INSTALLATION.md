# ClimaCity Paris -- Guide d'installation

Environnement Docker pour le projet PySpark "ClimaCity Paris".  
Contient JupyterLab, PySpark 3.5, Delta Lake 3.2 et MLflow 2.17.

---

## Prérequis

| Outil | Version minimale | Installation |
|-------|-----------------|--------------|
| Docker Desktop | 4.25 | https://www.docker.com/products/docker-desktop/ |
| Docker Compose | 2.20 (inclus dans Docker Desktop) | -- |
| RAM disponible | 8 Go (6 Go alloués à Docker) | -- |
| Espace disque | 10 Go libres | -- |

> **Windows** : Docker Desktop doit être configuré pour utiliser le backend WSL 2
> (Paramètres -> General -> "Use the WSL 2 based engine").  
> **macOS Apple Silicon** : l'image est compatible arm64/amd64 via Rosetta.

---

## Installation (première fois)

### 1. Cloner ou télécharger le projet

```bash
# Via Git
git clone <url-du-depot> climacity-paris
cd climacity-paris

# Ou extraire l'archive ZIP fournie par l'enseignant, puis :
cd climacity-paris
```

### 2. Ajuster les ressources si nécessaire

Ouvrez le fichier `.env` et adaptez les valeurs à votre machine :

```
# Laptop 8 Go RAM (défaut)
DOCKER_MEMORY_LIMIT=6g
SPARK_DRIVER_MEMORY=4g

# Laptop 16 Go RAM
DOCKER_MEMORY_LIMIT=10g
SPARK_DRIVER_MEMORY=7g
```

> Sur macOS/Linux vous pouvez aussi utiliser `make` : `make help` liste toutes
> les commandes disponibles.

### 3. Construire l'image

```bash
docker compose build jupyter
# Durée : 5-10 minutes selon la connexion (télécharge ~2 Go)
```

L'image inclut les JARs Delta Lake -- aucun téléchargement supplémentaire
ne sera nécessaire lors de la création des SparkSessions.

### 4. Démarrer les services

```bash
docker compose up -d
```

Attendez 30 secondes, puis vérifiez que les deux services sont démarrés :

```bash
docker compose ps
```

Vous devez voir `climacity-jupyter` et `climacity-mlflow` avec le statut `healthy`.

---

## Accès aux services

| Service | URL | Identifiant |
|---------|-----|-------------|
| JupyterLab | http://localhost:8888 | Token : `climacity` |
| Spark UI | http://localhost:4040 | Aucun (disponible après `getOrCreate()`) |
| MLflow UI | http://localhost:5000 | Aucun |

> Le **Spark UI** n'est disponible qu'après la création d'une `SparkSession` dans
> un notebook. Si vous voyez "Ce site est inaccessible", créez d'abord une session.

---

## Structure des répertoires

```
climacity-paris/
├── docker-compose.yml       # Orchestration des services
├── Dockerfile               # Image JupyterLab + PySpark personnalisée
├── requirements.txt         # Paquets Python ajoutés à l'image de base
├── .env                     # Configuration (ports, mémoire, token)
├── Makefile                 # Commandes de gestion (optionnel)
│
├── conf/
│   ├── spark-defaults.conf  # Configuration Spark globale (Delta, mémoire…)
│   └── start-climacity.sh   # Script de démarrage du conteneur
│
├── notebooks/               # Monté dans /home/jovyan/notebooks
│   └── Spark_DIA3_Session_1.ipynb ... Session_6.ipynb
│
├── scripts/                 # Monté dans /home/jovyan/scripts
│   ├── velib_prep.py        # Téléchargement et mise en forme des données
│   ├── simulateur_flux.py   # Simulateur de flux pour le Jour 2
│   ├── mesures.py           # Mesures de performance partagées
│   └── executer_carnets.py  # Rejeu des carnets par paires
│
└── data/                    # Monté dans /home/jovyan/data
    ├── velib/
    │   ├── raw/             # CSV.gz Vélib' historiques (téléchargés au Jour 1)
    │   └── parquet/         # Table Parquet consolidée (produite au Jour 1)
    ├── meteo/               # CSV météo horaire (Open-Meteo)
    └── output/              # Sorties : Delta, modèles, résultats
```

Les répertoires `notebooks/`, `scripts/` et `data/` sont des **volumes montés** :
les modifications faites dans le conteneur sont immédiatement visibles sur l'hôte,
et vice versa. Vos données et notebooks sont persistés entre les redémarrages.

---

## Commandes courantes

```bash
# Démarrer
docker compose up -d

# Arrêter (conserve les données)
docker compose stop

# Redémarrer
docker compose restart

# Voir les logs en temps réel
docker compose logs -f

# Ouvrir un terminal dans le conteneur
docker exec -it climacity-jupyter bash

# Arrêter et supprimer les conteneurs (les volumes sont conservés)
docker compose down

# Réinitialisation complète (SUPPRIME les données MLflow dans le volume Docker)
docker compose down --volumes
```

---

## Résolution des problèmes courants

### "Port already in use" au démarrage

Un autre service utilise le port 8888 (ou 4040, ou 5000).  
Modifiez les ports dans `.env` :

```
JUPYTER_PORT=8889
SPARK_UI_PORT=4041
MLFLOW_PORT=5001
```

Puis redémarrez : `docker compose up -d`.

### Le Spark UI (port 4040) est inaccessible

C'est normal avant la création d'une SparkSession. Exécutez la cellule
de configuration du notebook (`SparkSession.builder...getOrCreate()`),
attendez 5 secondes, puis rechargez http://localhost:4040.

### "OutOfMemoryError" dans un notebook

Augmentez la mémoire allouée dans `.env` :

```
DOCKER_MEMORY_LIMIT=8g
SPARK_DRIVER_MEMORY=6g
```

Puis reconstruisez et redémarrez :

```bash
docker compose down
docker compose up -d
```

### Le simulateur de flux ne produit pas de fichiers (Jour 2)

Le simulateur doit être lancé dans un terminal **séparé**, à l'intérieur du
conteneur. Ouvrez un terminal dans JupyterLab (menu File -> New -> Terminal)
et exécutez :

```bash
python /home/jovyan/scripts/simulateur_flux.py \
    --output /home/jovyan/data/output/stream_input \
    --vitesse 3
```

Laissez ce terminal ouvert pendant toute la session de streaming.

### La construction de l'image échoue (réseau)

Les JARs Delta Lake sont téléchargés depuis Maven Central pendant `docker compose build`.
Si votre réseau bloque Maven Central, demandez à l'enseignant de distribuer
l'image pré-construite :

```bash
# Enseignant : exporter l'image
docker save climacity-jupyter:latest | gzip > climacity-jupyter.tar.gz

# Étudiant : importer l'image (pas besoin de build)
docker load < climacity-jupyter.tar.gz
docker compose up -d
```

---

## Notes techniques

- **PySpark en mode local** : Spark s'exécute entièrement dans le conteneur Jupyter,
  sans cluster. Tous les coeurs du conteneur sont utilisés (`local[*]`).
- **Delta Lake** : les JARs sont inclus dans l'image (`${SPARK_HOME}/jars/`).
  La configuration dans `spark-defaults.conf` active automatiquement les extensions
  SQL Delta à chaque SparkSession.
- **MLflow** : le conteneur `jupyter` pointe vers `http://mlflow:5000` via le réseau
  Docker interne. Les runs sont persistés dans le volume nommé `climacity_mlflow_data`.
- **Données** : le répertoire `./data` de l'hôte est monté dans `/home/jovyan/data`
  dans le conteneur. Les chemins dans les notebooks utilisent des chemins relatifs
  (`../data/…`) depuis `/home/jovyan/notebooks`.
