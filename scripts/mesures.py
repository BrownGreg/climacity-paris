"""Enregistrement partagé des mesures de performance du projet.

Chaque carnet tourne dans son propre noyau : pour réunir dans le bilan final (carnet 6) les
mesures prises dans les carnets 2, 5 et 6, elles sont accumulées dans un fichier JSON commun.
"""
from __future__ import annotations

import json
from pathlib import Path


def enregistrer(fichier: Path, cle: str, valeur: object) -> None:
    """Ajoute (ou remplace) une mesure dans le fichier JSON commun.

    Args:
        fichier: Chemin du fichier JSON (créé s'il n'existe pas).
        cle: Nom de la mesure, par exemple ``"cache_jointure"``.
        valeur: Valeur sérialisable en JSON (nombre, liste, dictionnaire).

    Example:
        >>> enregistrer(Path("/tmp/m.json"), "gain_cache", 22.7)
    """
    mesures = lire(fichier)
    mesures[cle] = valeur
    fichier.parent.mkdir(parents=True, exist_ok=True)
    fichier.write_text(json.dumps(mesures, indent=2, ensure_ascii=False), encoding="utf-8")


def lire(fichier: Path) -> dict:
    """Lit toutes les mesures enregistrées.

    Args:
        fichier: Chemin du fichier JSON.

    Returns:
        Dictionnaire des mesures (vide si le fichier n'existe pas ou est illisible).
    """
    try:
        return json.loads(fichier.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
