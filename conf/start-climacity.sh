#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# start-climacity.sh -- Script de démarrage du conteneur Jupyter
# Démarre JupyterLab sur le port 8888, sans authentification par token
# (l'accès est déjà protégé par le réseau Docker interne).
# ─────────────────────────────────────────────────────────────────────────────
set -e

echo "=============================================="
echo " ClimaCity Paris -- Environnement PySpark"
echo " Spark  : $(python -c 'import pyspark; print(pyspark.__version__)')"
echo " Python : $(python --version)"
echo " Delta  : $(python -c 'import delta; print(delta.__version__)')"
echo " MLflow : $(python -c 'import mlflow; print(mlflow.__version__)')"
echo "=============================================="
echo ""
echo " JupyterLab : http://localhost:${JUPYTER_PORT:-8888}"
echo " Spark UI   : http://localhost:${SPARK_UI_PORT:-4040}  (disponible après création d'une SparkSession)"
echo " MLflow UI  : http://localhost:${MLFLOW_PORT:-5000}  (service mlflow)"
echo "=============================================="
echo ""

exec jupyter lab \
    --ip=0.0.0.0 \
    --port="${JUPYTER_PORT:-8888}" \
    --no-browser \
    --NotebookApp.token="${JUPYTER_TOKEN:-climacity}" \
    --NotebookApp.password="" \
    --ServerApp.root_dir=/home/jovyan \
    --ServerApp.allow_origin='*' \
    --ServerApp.disable_check_xsrf=True
