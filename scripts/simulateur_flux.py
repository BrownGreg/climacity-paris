#!/usr/bin/env python3
"""
Simulateur de flux Vélib' pour Structured Streaming.

Rejoue les données historiques Parquet en écrivant des fichiers JSON
dans un répertoire surveillé par Spark, à un rythme configurable.

Modifications par rapport à la version fournie (voir le README du rendu) :

* ``--vitesse`` était déclaré mais jamais utilisé. Il pilote maintenant le rythme :
  1 seconde réelle = ``vitesse`` minutes de données historiques.
* Un fichier JSON = un **relevé complet** du réseau (toutes les stations à un même instant),
  au lieu de tranches arbitraires de 50 lignes. Les fenêtres temporelles et la détection de
  ruptures consécutives ont ainsi un sens : deux fichiers successifs sont deux relevés
  successifs (≈ 15 minutes d'écart dans l'historique).
* Les horodatages historiques sont conservés tels quels (l'ancien « recalage sur
  maintenant » les collait tous à l'heure courante et rendait la durée d'une rupture
  incalculable).
* Le simulateur s'arrête à la fin des données au lieu de reboucler dans le passé (un
  rebouclage produirait des données « tardives » sans fin).

Usage
-----
    python simulateur_flux.py --input data/output/disponibilite_consolidee.parquet
                               --output data/output/stream_input
                               --vitesse 3

Options
-------
--input        : Répertoire Parquet source (produit au Jour 1).
--output       : Répertoire de sortie surveillé par Spark readStream.
--vitesse      : Minutes de données historiques rejouées par seconde réelle
                 (défaut : 3, donc un relevé de 15 minutes toutes les 5 secondes).
--batch-size   : Nombre de relevés (instants) par fichier JSON (défaut : 1).
--intervalle   : Force un intervalle fixe (secondes réelles) entre deux fichiers, à la place
                 de celui déduit de --vitesse.
--annee/--mois : Mois de départ (défaut : 2021-01, premier mois complet de l'historique).
--max-fichiers : Arrête après ce nombre de fichiers (défaut : illimité).
"""

import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd

COLONNES = [
    "station_id", "nom_station", "code_arr", "capacite",
    "velos_meca", "velos_elec", "bornettes_libres", "horodatage",
]


def parse_args() -> argparse.Namespace:
    """Lit les options de la ligne de commande.

    Returns:
        L'espace de noms des arguments.
    """
    parser = argparse.ArgumentParser(
        description="Simulateur de flux Vélib' pour Structured Streaming."
    )
    parser.add_argument("--input",
        default="data/output/disponibilite_consolidee.parquet",
        help="Répertoire Parquet source.")
    parser.add_argument("--output",
        default="data/output/stream_input",
        help="Répertoire de sortie surveillé par Spark readStream.")
    parser.add_argument("--vitesse", type=float, default=3.0,
        help="Minutes de données historiques par seconde réelle (défaut : 3).")
    parser.add_argument("--batch-size", type=int, default=1,
        help="Nombre de relevés (instants) par fichier JSON (défaut : 1).")
    parser.add_argument("--intervalle", type=float, default=None,
        help="Intervalle fixe entre deux fichiers, en secondes réelles.")
    parser.add_argument("--annee", type=int, default=2021,
        help="Année de départ (défaut : 2021).")
    parser.add_argument("--mois", type=int, default=1,
        help="Mois de départ (défaut : 1).")
    parser.add_argument("--max-fichiers", type=int, default=None,
        help="Nombre maximal de fichiers à émettre.")
    return parser.parse_args()


def charger_partitions(chemin: str, annee: int, mois: int) -> pd.DataFrame:
    """Charge à partir d'un mois donné toutes les partitions Parquet suivantes.

    Args:
        chemin: Répertoire Parquet partitionné par ``annee=`` puis ``mois=``.
        annee: Année de départ.
        mois: Mois de départ.

    Returns:
        DataFrame Pandas trié par horodatage, vide si aucune partition ne correspond.
    """
    morceaux = []
    for dossier_annee in sorted(Path(chemin).glob("annee=*")):
        a = int(dossier_annee.name.split("=")[1])
        for dossier_mois in sorted(dossier_annee.glob("mois=*")):
            m = int(dossier_mois.name.split("=")[1])
            if (a, m) < (annee, mois):
                continue
            try:
                morceaux.append(pd.read_parquet(dossier_mois, columns=COLONNES))
            except Exception as e:  # partition illisible : on la signale et on continue
                print(f"  [ERREUR] Lecture de {dossier_mois} : {e}", file=sys.stderr)
    if not morceaux:
        return pd.DataFrame(columns=COLONNES)
    df = pd.concat(morceaux, ignore_index=True)
    df["horodatage"] = pd.to_datetime(df["horodatage"], utc=True)
    return df.sort_values("horodatage").reset_index(drop=True)


def serialiser_releves(df: pd.DataFrame) -> list[dict]:
    """Convertit des relevés Pandas en dictionnaires JSON-sérialisables.

    Args:
        df: Relevés (colonnes de ``COLONNES``).

    Returns:
        Liste de dictionnaires, horodatage au format ISO 8601 UTC (``...Z``).
    """
    sortie = df.copy()
    sortie["horodatage"] = sortie["horodatage"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    # to_json/loads convertit proprement les types numpy en types Python natifs
    return json.loads(sortie.to_json(orient="records", force_ascii=False))


def main() -> None:
    """Rejoue l'historique en écrivant un fichier JSON par relevé, à la vitesse demandée."""
    args = parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Simulateur de flux Vélib' démarré")
    print(f"  Source   : {args.input}")
    print(f"  Sortie   : {args.output}")
    print(f"  Vitesse  : {args.vitesse} min de données par seconde réelle")
    print(f"  Départ   : {args.annee}-{args.mois:02d}")
    print("  Ctrl+C pour arrêter.\n", flush=True)

    df = charger_partitions(args.input, args.annee, args.mois)
    if df.empty:
        print("  [INFO] Aucune donnée à rejouer à partir de cette date.", file=sys.stderr)
        return
    print(f"  {len(df):,} lignes chargées, {df['horodatage'].nunique():,} relevés\n", flush=True)

    # Un « relevé » = toutes les lignes portant le même horodatage.
    instants = list(df.groupby("horodatage", sort=True))
    emis, precedent = 0, None
    for i in range(0, len(instants), args.batch_size):
        groupe = instants[i : i + args.batch_size]
        payload = serialiser_releves(pd.concat([g for _, g in groupe]))

        # Écriture atomique : Spark ignore les fichiers commençant par « . » ; il ne voit le
        # fichier qu'une fois complet, jamais à moitié écrit.
        nom = output_dir / f"velib_{emis:06d}.json"
        tmp = output_dir / f".tmp_{emis:06d}.json"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        tmp.rename(nom)
        emis += 1

        instant = groupe[-1][0]
        print(f"  [{emis:04d}] {nom.name}  {len(payload)} lignes  (ts={payload[-1]['horodatage']})",
              flush=True)
        if args.max_fichiers and emis >= args.max_fichiers:
            break

        # Cadence : l'écart entre deux relevés historiques, divisé par la vitesse.
        if args.intervalle is not None:
            pause = args.intervalle
        elif precedent is not None:
            ecart_min = (instant - precedent).total_seconds() / 60
            pause = min(max(ecart_min / args.vitesse, 0.2), 30.0)  # bornes : jamais 0, jamais 30 s+
        else:
            pause = 1.0
        precedent = instant
        time.sleep(pause)

    print("\nFin des données : simulateur terminé.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nSimulateur arrêté proprement.")
        sys.exit(0)
