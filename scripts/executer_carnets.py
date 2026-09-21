#!/usr/bin/env python3
"""Exécute une suite de carnets Jupyter dans UN SEUL noyau et enregistre les sorties.

Les carnets du projet forment des paires qui partagent leur état (le Jour 1 est coupé en
deux carnets, comme le Jour 2 et le Jour 3) : les variables et la SparkSession du premier
doivent exister dans le second. Ce script les exécute donc dans le même noyau, dans
l'ordre donné, et réécrit chaque carnet avec ses sorties.

Usage (depuis le conteneur) :
    python scripts/executer_carnets.py notebooks/Spark_DIA3_Session_1.ipynb \
                                       notebooks/Spark_DIA3_Session_2.ipynb
Options :
    --sortie DOSSIER   écrit les carnets exécutés dans ce dossier (défaut : écrase l'entrée)
    --timeout SEC      durée maximale par cellule (défaut : 3600)
    --continuer        ne s'arrête pas à la première cellule en erreur
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError


def executer(chemins: list[Path], sortie: Path | None, timeout: int, continuer: bool) -> int:
    """Exécute les carnets à la suite dans un même noyau.

    Args:
        chemins: Carnets à exécuter, dans l'ordre.
        sortie: Dossier de sortie, ou None pour écraser les fichiers d'entrée.
        timeout: Durée maximale par cellule, en secondes.
        continuer: Si vrai, une cellule en erreur n'interrompt pas l'exécution.

    Returns:
        Le nombre de cellules en erreur (0 = succès).
    """
    carnets = [nbformat.read(c, as_version=4) for c in chemins]
    # Un carnet "virtuel" contenant toutes les cellules : un seul noyau, un seul état.
    fusion = nbformat.v4.new_notebook(metadata=carnets[0].metadata)
    bornes = []
    for nb in carnets:
        debut = len(fusion.cells)
        fusion.cells.extend(nb.cells)
        bornes.append((debut, len(fusion.cells)))

    erreurs = 0
    client = NotebookClient(
        fusion, timeout=timeout, kernel_name="python3",
        allow_errors=continuer, resources={"metadata": {"path": str(chemins[0].parent)}},
    )
    t0 = time.perf_counter()
    try:
        client.execute()
    except CellExecutionError as e:
        erreurs = 1
        print(f"[ERREUR] {str(e)[:2000]}", file=sys.stderr)
    for cellule in fusion.cells:
        if cellule.cell_type == "code":
            erreurs += sum(1 for o in cellule.get("outputs", []) if o.get("output_type") == "error")
    print(f"Durée totale : {time.perf_counter() - t0:.0f} s")

    for chemin, nb, (debut, fin) in zip(chemins, carnets, bornes):
        nb.cells = fusion.cells[debut:fin]
        cible = (sortie / chemin.name) if sortie else chemin
        cible.parent.mkdir(parents=True, exist_ok=True)
        nbformat.write(nb, cible)
        print(f"  -> {cible}")
    return erreurs


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("carnets", nargs="+", type=Path)
    p.add_argument("--sortie", type=Path, default=None)
    p.add_argument("--timeout", type=int, default=3600)
    p.add_argument("--continuer", action="store_true")
    a = p.parse_args()
    sys.exit(1 if executer(a.carnets, a.sortie, a.timeout, a.continuer) else 0)


if __name__ == "__main__":
    main()
