# ─────────────────────────────────────────────────────────────────────────────
# ClimaCity Paris -- Image JupyterLab / PySpark
#
# Base : jupyter/pyspark-notebook (JupyterLab + OpenJDK + Spark pré-installés)
# Ajouts :
#   - delta-spark (JARs bundlés dans l'image pour fonctionner hors ligne)
#   - mlflow, folium, plotly, openmeteo-requests, tqdm
#   - spark-defaults.conf préconfiguré
# ─────────────────────────────────────────────────────────────────────────────
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

LABEL maintainer="ClimaCity Paris -- Cours PySpark" \
      description="Environnement JupyterLab pour le projet ClimaCity Paris" \
      spark.version="3.5.3" \
      delta.version="3.2.0"

# Passage en root pour les installations système
USER root

# ── 1. Dépendances système légères ───────────────────────────────────────────
RUN apt-get update --quiet && \
    apt-get install --quiet --yes --no-install-recommends \
        curl wget procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ── 2. JARs Delta Lake (bundlés pour éviter le téléchargement au runtime) ────
# delta-spark_2.12 : bindings Spark ↔ Delta
# delta-storage    : couche de stockage abstraite (local, S3, ADLS…)
ENV DELTA_VERSION=3.2.0

RUN wget --quiet --show-progress \
        --progress=bar:force:noscroll \
        -P "${SPARK_HOME}/jars/" \
        "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" && \
    wget --quiet --show-progress \
        --progress=bar:force:noscroll \
        -P "${SPARK_HOME}/jars/" \
        "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"

# ── 3. Configuration Spark globale ───────────────────────────────────────────
COPY conf/spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf"

# ── 4. Paquets Python supplémentaires ────────────────────────────────────────
# On passe par pip (mamba est disponible mais plus lent pour ces paquets)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# ── 5. Répertoires de travail ─────────────────────────────────────────────────
# /home/jovyan est le répertoire home de l'utilisateur jovyan (utilisateur Jupyter)
RUN mkdir -p \
        /home/jovyan/notebooks \
        /home/jovyan/scripts \
        /home/jovyan/data/velib/raw \
        /home/jovyan/data/velib/parquet \
        /home/jovyan/data/meteo \
        /home/jovyan/data/output && \
    chown -R jovyan:users /home/jovyan/

# ── 6. Script de démarrage personnalisé ──────────────────────────────────────
COPY conf/start-climacity.sh /usr/local/bin/start-climacity.sh
RUN chmod +x /usr/local/bin/start-climacity.sh

# Retour à l'utilisateur non-privilégié
USER jovyan

WORKDIR /home/jovyan

# Port JupyterLab
EXPOSE 8888
# Port Spark UI (local mode)
EXPOSE 4040

CMD ["/usr/local/bin/start-climacity.sh"]
