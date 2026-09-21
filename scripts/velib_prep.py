"""Préparation des données brutes ClimaCity Paris (Vélib' + météo).

Ce module remplace la « distribution pré-préparée » mentionnée dans l'énoncé
du projet, qui n'est pas disponible publiquement. Il :

1. télécharge l'historique public ``lovasoa/historique-velib-opendata`` ;
2. construit un référentiel de stations (identifiant entier stable, position,
   code d'arrondissement) ;
3. convertit l'historique au format CSV compressé attendu par le Jour 1
   (``station_id;nom_station;code_arrondissement;capacite;velos_meca;
   velos_elec;bornettes_libres;horodatage``), un fichier par quinzaine ;
4. télécharge les observations horaires Open-Meteo.

Ce code ne fait que de l'**acquisition** (téléchargement et remise en forme
d'un format) : tout le traitement analytique du projet se fait ensuite avec
l'API Spark, conformément à la contrainte n°4 de l'énoncé.
"""

from __future__ import annotations

import gzip
import time
import zipfile
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
import requests

# ── Sources de données ──────────────────────────────────────────────────────
GBFS_STATION_INFO = (
    "https://velib-metropole-opendata.smovengo.cloud"
    "/opendata/Velib_Metropole/station_information.json"
)
HISTORIQUE_ZIP_URL = (
    "https://github.com/lovasoa/historique-velib-opendata"
    "/releases/download/new/stations.zip"
)
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
# Coordonnées utilisées dans l'exemple de l'enseignant (centre de Paris)
PARIS_LAT, PARIS_LON = 48.8575, 2.3514

# Colonnes du fichier de l'historique (il n'a pas d'en-tête)
COLONNES_HISTORIQUE = [
    "date", "capacite", "velos_meca", "velos_elec",
    "nom_station", "geo", "operative",
]
# Colonnes du CSV brut attendu par les carnets (format documenté au Jour 1)
COLONNES_BRUT = [
    "station_id", "nom_station", "code_arrondissement", "capacite",
    "velos_meca", "velos_elec", "bornettes_libres", "horodatage",
]
ID_PREMIERE_STATION = 1001  # identifiants denses : 1001, 1002, ...


def _get(url: str, tentatives: int = 4, **kwargs) -> requests.Response:
    """GET avec nouvelles tentatives (réseau ou DNS ponctuellement indisponible).

    Args:
        url: Adresse à interroger.
        tentatives: Nombre maximal d'essais.
        **kwargs: Arguments transmis à ``requests.get`` (``timeout``, ``params``, ``stream``...).

    Returns:
        La réponse HTTP (code de succès vérifié).

    Raises:
        requests.RequestException: Si tous les essais échouent (la dernière erreur est relevée).
    """
    derniere: Exception | None = None
    for essai in range(tentatives):
        try:
            reponse = requests.get(url, **kwargs)
            reponse.raise_for_status()
            return reponse
        except requests.RequestException as e:
            derniere = e
            time.sleep(2 ** essai)          # attente croissante : 1, 2, 4, 8 s
    raise derniere  # type: ignore[misc]


def telecharger(url: str, destination: Path, chunk_mo: int = 4) -> Path:
    """Télécharge un fichier en flux (sans le charger en mémoire).

    Args:
        url: Adresse à télécharger.
        destination: Chemin du fichier local. S'il existe déjà, rien n'est
            retéléchargé (l'opération est idempotente).
        chunk_mo: Taille des blocs lus, en mégaoctets.

    Returns:
        Le chemin du fichier local.

    Raises:
        requests.HTTPError: Si le serveur répond avec un code d'erreur.

    Example:
        >>> telecharger("https://exemple.org/a.zip", Path("/tmp/a.zip"))
        PosixPath('/tmp/a.zip')
    """
    if destination.exists() and destination.stat().st_size > 0:
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    partiel = destination.with_suffix(destination.suffix + ".part")
    with _get(url, stream=True, timeout=60) as reponse:
        with open(partiel, "wb") as f:
            for bloc in reponse.iter_content(chunk_size=chunk_mo * 1_048_576):
                f.write(bloc)
    partiel.rename(destination)  # renommage atomique : jamais de demi-fichier
    return destination


def lire_historique(zip_path: Path, chunksize: int = 1_000_000) -> Iterator[pd.DataFrame]:
    """Lit le CSV de l'archive par blocs, sans l'extraire sur le disque.

    Args:
        zip_path: Archive ``stations.zip`` de l'historique.
        chunksize: Nombre de lignes par bloc.

    Yields:
        Des DataFrames Pandas de ``chunksize`` lignes (colonnes de
        ``COLONNES_HISTORIQUE``).
    """
    with zipfile.ZipFile(zip_path) as archive:
        membre = archive.namelist()[0]
        with archive.open(membre) as flux:
            for bloc in pd.read_csv(
                flux, names=COLONNES_HISTORIQUE, chunksize=chunksize,
                dtype={"operative": str},
            ):
                # le fichier source contient des espaces parasites en tête de nom
                bloc["nom_station"] = bloc["nom_station"].str.strip()
                yield bloc


def charger_stations_gbfs(url: str = GBFS_STATION_INFO) -> pd.DataFrame:
    """Charge la liste actuelle des stations depuis l'API GBFS de Vélib'.

    Args:
        url: Point d'accès ``station_information.json``.

    Returns:
        DataFrame avec ``stationCode`` (str), ``name``, ``lat``, ``lon``,
        ``capacity``.

    Raises:
        requests.HTTPError: Si l'API ne répond pas correctement.
    """
    reponse = _get(url, timeout=30)
    stations = reponse.json()["data"]["stations"]
    df = pd.DataFrame(stations)[["stationCode", "name", "lat", "lon", "capacity"]]
    df["stationCode"] = df["stationCode"].astype(str)
    return df


def cle_station(bloc: pd.DataFrame, homonymes: set[str]) -> pd.Series:
    """Clé unique d'une station dans l'historique.

    Le nom suffit presque toujours, mais quelques stations *distinctes* portent le même nom
    (position différente) : les fusionner doublerait leurs relevés. Pour ces noms ambigus,
    la position est ajoutée à la clé.

    Args:
        bloc: Bloc de l'historique (colonnes ``nom_station`` et ``geo``).
        homonymes: Noms portés par plusieurs stations.

    Returns:
        Série de clés, alignée sur ``bloc``.
    """
    ambigu = bloc["nom_station"].isin(homonymes)
    return bloc["nom_station"].where(~ambigu, bloc["nom_station"] + " @" + bloc["geo"])


def trouver_homonymes(zip_path: Path) -> set[str]:
    """Repère les noms partagés par plusieurs stations *au même instant*.

    Args:
        zip_path: Archive de l'historique.

    Returns:
        Ensemble des noms qui apparaissent plusieurs fois pour un même horodatage.
    """
    homonymes: set[str] = set()
    for bloc in lire_historique(zip_path):
        par_instant = bloc.groupby(["date", "nom_station"]).size()
        homonymes.update(par_instant[par_instant > 1].index.get_level_values("nom_station"))
    return homonymes


def construire_referentiel(zip_path: Path, gbfs: pd.DataFrame) -> pd.DataFrame:
    """Construit le référentiel des stations à partir de l'historique.

    L'historique ne contient ni identifiant ni arrondissement : on les
    reconstitue. Chaque station reçoit un ``station_id`` entier dense (trié
    par nom puis clé, donc reproductible). Le ``code_arr`` vient du code de
    station Vélib' actuel (``stationCode // 1000``, ex. 16107 -> 16e). Le
    rapprochement avec l'API actuelle se fait parmi les stations de même nom
    (ou, à défaut, parmi toutes) en prenant la **géographiquement la plus
    proche** : cela couvre à la fois les homonymes et les stations renommées.

    Args:
        zip_path: Archive de l'historique.
        gbfs: Résultat de ``charger_stations_gbfs``.

    Returns:
        DataFrame ``station_id, name, lat, lon, capacity, stationCode,
        code_arr, cle`` (une ligne par station).
    """
    homonymes = trouver_homonymes(zip_path)
    morceaux = []
    for bloc in lire_historique(zip_path):
        bloc["cle"] = cle_station(bloc, homonymes)
        morceaux.append(
            bloc.groupby("cle").agg(
                name=("nom_station", "first"), geo=("geo", "last"),
                capacite=("capacite", "max"),
            )
        )
    stations = pd.concat(morceaux).groupby(level=0).agg(
        name=("name", "first"), geo=("geo", "last"), capacite=("capacite", "max")
    )
    latlon = stations["geo"].str.split(",", expand=True).astype(float)
    ref = pd.DataFrame({
        "cle": stations.index,
        "name": stations["name"].to_numpy(),
        "lat": latlon[0].to_numpy(),
        "lon": latlon[1].to_numpy(),
        "capacity": stations["capacite"].to_numpy(),
    }).sort_values("cle").reset_index(drop=True)
    ref["station_id"] = ID_PREMIERE_STATION + ref.index

    # Code de station : parmi les stations actuelles de même nom (à défaut : toutes),
    # on retient la plus proche (distance euclidienne en degrés : suffisante à Paris).
    gbfs = gbfs.assign(name=gbfs["name"].str.strip())
    codes = []
    for ligne in ref.itertuples():
        candidates = gbfs[gbfs["name"] == ligne.name]
        if candidates.empty:
            candidates = gbfs
        d2 = (candidates["lat"] - ligne.lat) ** 2 + (candidates["lon"] - ligne.lon) ** 2
        codes.append(candidates["stationCode"].iloc[int(np.argmin(d2.to_numpy()))])
    ref["stationCode"] = codes
    ref["code_arr"] = ref["stationCode"].astype(int) // 1000
    return ref[["station_id", "name", "lat", "lon", "capacity", "stationCode", "code_arr", "cle"]]


def convertir_historique(
    zip_path: Path, referentiel: pd.DataFrame, dossier_sortie: Path
) -> list[Path]:
    """Convertit l'historique au format CSV brut du Jour 1, un fichier/quinzaine.

    Traitements appliqués (et uniquement ceux-ci) :

    * ajout de ``station_id`` et ``code_arrondissement`` (référentiel) ;
    * calcul de ``bornettes_libres = capacité - vélos`` (**non borné** : les
      valeurs négatives sont volontairement conservées pour l'exercice de
      nettoyage Spark du Jour 1) ;
    * horodatage ISO 8601 avec fuseau (``2020-11-26T12:59:00+00:00``) ;
    * suppression des relevés de stations hors service (``operative=False``) :
      un vélo « indisponible » n'est alors pas une vraie rupture.

    Args:
        zip_path: Archive de l'historique.
        referentiel: Résultat de ``construire_referentiel``.
        dossier_sortie: Dossier où écrire les ``velib_AAAA-MM-A|B.csv.gz``.

    Returns:
        La liste triée des fichiers écrits.
    """
    dossier_sortie.mkdir(parents=True, exist_ok=True)
    par_cle = referentiel.set_index("cle")
    ids = par_cle["station_id"]
    arr = par_cle["code_arr"]
    homonymes = trouver_homonymes(zip_path)
    sorties: dict[str, gzip.GzipFile] = {}

    try:
        for bloc in lire_historique(zip_path):
            bloc = bloc[bloc["operative"] == "True"].copy()
            cle = cle_station(bloc, homonymes)
            out = pd.DataFrame({
                "station_id": cle.map(ids),
                "nom_station": bloc["nom_station"],
                "code_arrondissement": cle.map(arr),
                "capacite": bloc["capacite"],
                "velos_meca": bloc["velos_meca"],
                "velos_elec": bloc["velos_elec"],
                "bornettes_libres": bloc["capacite"] - bloc["velos_meca"] - bloc["velos_elec"],
                # "2020-11-26T12:59Z" -> "2020-11-26T12:59:00+00:00"
                "horodatage": bloc["date"].str.replace("Z", ":00+00:00", regex=False),
            })[COLONNES_BRUT]
            # clé de fichier : année-mois + quinzaine (A = jours 1-15, B = suite)
            jour = bloc["date"].str.slice(8, 10).astype(int)
            fichier = bloc["date"].str.slice(0, 7) + np.where(jour <= 15, "-A", "-B")
            for k, sous in out.groupby(fichier.to_numpy()):
                if k not in sorties:
                    chemin = dossier_sortie / f"velib_{k}.csv.gz"
                    sorties[k] = gzip.open(chemin, "wt", encoding="utf-8", newline="")
                    sorties[k].write(";".join(COLONNES_BRUT) + "\n")
                sous.to_csv(sorties[k], sep=";", header=False, index=False)
    finally:
        for f in sorties.values():
            f.close()
    return sorted(dossier_sortie.glob("velib_*.csv.gz"))


def telecharger_meteo(debut: str, fin: str, destination: Path) -> Path:
    """Télécharge les observations horaires Open-Meteo pour Paris.

    Args:
        debut: Date de début ``AAAA-MM-JJ``.
        fin: Date de fin ``AAAA-MM-JJ`` (incluse).
        destination: Fichier CSV (séparateur ``;``) à écrire.

    Returns:
        Le chemin du fichier écrit, de colonnes ``time, temperature_2m,
        precipitation, windspeed_10m, relativehumidity_2m`` (UTC, 1 ligne/h).

    Raises:
        requests.HTTPError: Si l'API répond avec une erreur.
        KeyError: Si la réponse ne contient pas le bloc ``hourly`` attendu.
    """
    params = {
        "latitude": PARIS_LAT, "longitude": PARIS_LON,
        "start_date": debut, "end_date": fin,
        "hourly": "temperature_2m,precipitation,windspeed_10m,relativehumidity_2m",
        "timezone": "UTC",
    }
    reponse = _get(OPEN_METEO_URL, params=params, timeout=60)
    df = pd.DataFrame(reponse.json()["hourly"])
    destination.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(destination, sep=";", index=False)
    return destination


def preparer_tout(data_dir: Path) -> dict[str, object]:
    """Exécute toute la préparation (idempotente) et renvoie un résumé.

    Args:
        data_dir: Racine des données (``../data`` dans les carnets).

    Returns:
        Dictionnaire décrivant ce qui a été produit : nombre de stations,
        fichiers bruts, période couverte, fichier météo.
    """
    velib = data_dir / "velib"
    zip_path = telecharger(HISTORIQUE_ZIP_URL, velib / "historique_stations.zip")
    gbfs = charger_stations_gbfs()
    ref = construire_referentiel(zip_path, gbfs)
    stations_csv = velib / "stations_info.csv"
    ref.to_csv(stations_csv, sep=";", index=False)

    fichiers = convertir_historique(zip_path, ref, velib / "raw")
    noms = [f.name for f in fichiers]
    debut = noms[0].split("_")[1][:7] + "-01"
    fin_mois = pd.Period(noms[-1].split("_")[1][:7]).end_time.strftime("%Y-%m-%d")
    meteo = telecharger_meteo(debut, fin_mois, data_dir / "meteo" / "paris_montsouris_horaire.csv")
    return {
        "stations": len(ref), "fichiers_bruts": len(fichiers),
        "periode": (debut, fin_mois), "stations_csv": stations_csv, "meteo_csv": meteo,
    }
