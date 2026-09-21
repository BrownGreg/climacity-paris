# ─────────────────────────────────────────────────────────────────────────────
# Makefile -- Commandes de gestion de l'environnement ClimaCity Paris
#
# Usage :
#   make build      Construire l'image Docker (première fois ou après modification)
#   make start      Démarrer tous les services en arrière-plan
#   make stop       Arrêter les services (conserve les données)
#   make restart    Redémarrer tous les services
#   make logs       Afficher les logs en temps réel
#   make status     État des conteneurs et des ports
#   make clean      Supprimer les conteneurs ET les volumes (réinitialisation complète)
#   make shell      Ouvrir un terminal dans le conteneur Jupyter
#   make urls       Afficher les URLs d'accès
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: build start stop restart logs status clean shell urls help

# Chargement du fichier .env pour les variables
include .env
export

COMPOSE = docker compose
JUPYTER_CONTAINER = climacity-jupyter

## ── Cycle de vie ──────────────────────────────────────────────────────────────

build: ## Construire l'image Docker (nécessaire la première fois)
	@echo "Construction de l'image climacity-jupyter..."
	$(COMPOSE) build jupyter
	@echo "Image construite."

start: ## Démarrer tous les services
	@echo "Démarrage des services ClimaCity Paris..."
	$(COMPOSE) up -d
	@echo ""
	@$(MAKE) urls

stop: ## Arrêter les services (les données sont conservées)
	@echo "Arrêt des services..."
	$(COMPOSE) stop

restart: stop start ## Redémarrer tous les services

down: ## Arrêter et supprimer les conteneurs (les volumes sont conservés)
	$(COMPOSE) down

clean: ## Supprimer conteneurs + volumes (réinitialisation complète)
	@echo "ATTENTION : suppression des conteneurs, images et volumes."
	@read -p "Confirmer ? [o/N] " rep; \
	if [ "$$rep" = "o" ] || [ "$$rep" = "O" ]; then \
		$(COMPOSE) down --volumes --rmi local; \
		echo "Nettoyage terminé."; \
	else \
		echo "Annulé."; \
	fi

## ── Inspection ────────────────────────────────────────────────────────────────

logs: ## Afficher les logs en temps réel (Ctrl+C pour quitter)
	$(COMPOSE) logs -f

logs-jupyter: ## Logs du conteneur Jupyter uniquement
	$(COMPOSE) logs -f jupyter

logs-mlflow: ## Logs du conteneur MLflow uniquement
	$(COMPOSE) logs -f mlflow

status: ## État des conteneurs
	@echo "=== Conteneurs ==="
	$(COMPOSE) ps
	@echo ""
	@echo "=== Ports exposés ==="
	@docker port $(JUPYTER_CONTAINER) 2>/dev/null || echo "  Conteneur Jupyter non démarré"

urls: ## Afficher les URLs d'accès
	@echo ""
	@echo "=================================================="
	@echo "  ClimaCity Paris -- Accès aux services"
	@echo "=================================================="
	@echo "  JupyterLab : http://localhost:$(JUPYTER_PORT)"
	@echo "  Token      : $(JUPYTER_TOKEN)"
	@echo "  Spark UI   : http://localhost:$(SPARK_UI_PORT)"
	@echo "              (disponible après SparkSession.builder...getOrCreate())"
	@echo "  MLflow UI  : http://localhost:$(MLFLOW_PORT)"
	@echo "=================================================="
	@echo ""

## ── Utilitaires ────────────────────────────────────────────────────────────────

shell: ## Ouvrir un terminal bash dans le conteneur Jupyter
	docker exec -it $(JUPYTER_CONTAINER) bash

shell-root: ## Ouvrir un terminal root dans le conteneur Jupyter (debug)
	docker exec -it --user root $(JUPYTER_CONTAINER) bash

ps-spark: ## Lister les processus Java/Spark dans le conteneur
	docker exec $(JUPYTER_CONTAINER) ps aux | grep -E "java|spark" | grep -v grep

check-delta: ## Vérifier que les JARs Delta Lake sont présents
	@echo "JARs Delta Lake dans le conteneur :"
	@docker exec $(JUPYTER_CONTAINER) ls -lh $${SPARK_HOME}/jars/delta*.jar 2>/dev/null \
		|| echo "  [ERREUR] JARs manquants -- reconstruisez l'image avec 'make build'"

check-ports: ## Vérifier que les ports nécessaires sont libres sur l'hôte
	@echo "Vérification des ports $(JUPYTER_PORT), $(SPARK_UI_PORT), $(MLFLOW_PORT)..."
	@for port in $(JUPYTER_PORT) $(SPARK_UI_PORT) $(MLFLOW_PORT); do \
		if lsof -Pi :$$port -sTCP:LISTEN -t >/dev/null 2>&1; then \
			echo "  [OCCUPÉ] Port $$port"; \
		else \
			echo "  [LIBRE]  Port $$port"; \
		fi; \
	done

## ── Données ────────────────────────────────────────────────────────────────────

init-data: ## Créer la structure de répertoires de données
	mkdir -p data/velib/raw data/velib/parquet data/meteo data/output
	@echo "Structure de données créée dans ./data/"

help: ## Afficher cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""

.DEFAULT_GOAL := help
