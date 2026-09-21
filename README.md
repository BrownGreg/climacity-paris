# ClimaCity Paris

Projet Spark / PySpark du module « Traitement de données massives ». Je l'ai réalisé seul.

Le dépôt contient six carnets Jupyter avec leurs sorties, quatre scripts Python et l'environnement Docker pour
les rejouer. Je ne versionne pas les données : le premier carnet les télécharge.

## Lancer le projet

Docker et 10 Go de RAM disponibles pour lui suffisent (réglages dans `.env`). Le guide d'installation d'origine
reste dans `README_INSTALLATION.md`.

```bash
docker compose up -d
```

JupyterLab répond sur http://localhost:8888/lab?token=climacity, MLflow sur http://localhost:5000, et le Spark UI
sur le port 4040 dès qu'une session est ouverte.

Les carnets vont par paires qui partagent leur noyau : le second reprend la SparkSession et les variables du
premier. Les paires sont 1+2 (Jour 1), 3+4 (Jour 2) et 5+6 (Jour 3). Cette commande rejoue tout sans clic :

```bash
docker exec climacity-jupyter bash -lc 'cd ~ &&
  python scripts/executer_carnets.py notebooks/Spark_DIA3_Session_1.ipynb notebooks/Spark_DIA3_Session_2.ipynb &&
  python scripts/executer_carnets.py notebooks/Spark_DIA3_Session_3.ipynb notebooks/Spark_DIA3_Session_4.ipynb &&
  python scripts/executer_carnets.py notebooks/Spark_DIA3_Session_5.ipynb notebooks/Spark_DIA3_Session_6.ipynb'
```

Elle dure une vingtaine de minutes sur une machine à 8 coeurs, téléchargement de 236 Mo compris. Les chemins
sont relatifs et déclarés en tête de chaque carnet.

## Écarts avec l'énoncé

L'énoncé annonce des données que je n'ai pas pu obtenir. Chaque adaptation porte le mot « ADAPTATION » dans les
carnets.

- **Période.** L'énoncé parle d'un extrait Vélib' 2022-2023 distribué aux étudiants. Le seul historique public,
  `lovasoa/historique-velib-opendata`, couvre du 26/11/2020 au 09/04/2021 : 10,8 millions de relevés dans une
  archive unique, sans identifiant de station. Tout mon travail porte sur cet hiver.
- **Entraînement et test.** Sans deuxième année, j'ai coupé dans le temps, 85 % puis 15 %, comme le carnet 5 le
  prévoit en repli. La coupure tombe le 1er avril 2021 à 19 h 13 UTC, avec une heure d'écart entre les deux jeux.
- **Météo.** J'utilise Open-Meteo, la source indiquée par l'enseignant le 26/06, à la place du SYNOP.
- **Identifiant de station.** `scripts/velib_prep.py` en fabrique un : un entier trié par nom, et la position
  départage les homonymes. L'arrondissement vient du code de station de l'API GBFS (`code // 1000`), ou de la
  station actuelle la plus proche quand le nom a changé.
- **Simulateur de flux.** Celui de l'énoncé ignorait son option `--vitesse` et recalait tous les horodatages sur
  l'instant présent. Je l'ai réécrit : un fichier JSON par relevé complet, cadence déduite de `--vitesse`.
- **Docker.** Dans `docker-compose.yml`, la commande de MLflow était mal repliée et le serveur n'écoutait que
  sur 127.0.0.1. Le healthcheck appelait `curl`, absent de l'image, et Jupyter n'atteignait pas les artefacts.
  J'ai corrigé les trois.

Cet hiver 2020-2021 comprend le couvre-feu puis le confinement de mars-avril, et le jeu de test (avril) tombe en
plein dedans. Les usages observés diffèrent de ceux d'une année ordinaire.

## Ce que j'ai fait, jour par jour

### Jour 1 : RDD et DataFrame (carnets 1 et 2)

Avec les RDD, je lis l'en-tête par `first()` et je le retire par `filter()`, puisque les dix fichiers ont le
même. `parse_ligne` refuse toute ligne qui n'a pas huit champs ou dont un entier est illisible. `reduceByKey`
convient parce que l'addition est associative et commutative : chaque partition somme de son côté avant
l'échange. Pour une moyenne, j'agrège le couple (somme, effectif) et je divise à la fin. Un fichier `.gz` ne se
découpe pas, donc chaque fichier forme une partition.

Le carnet d'origine créait le DataFrame à partir de `step3`. Ce RDD garde les stations presque vides et porte
deux clés de plus, si bien que `createDataFrame` échouait et qu'un échantillon biaisé aurait suivi. Je pars de
tous les relevés valides, et je calcule le profil horaire du carnet 1 sur la même base.

Le nettoyage ramène les valeurs négatives à zéro au lieu de supprimer la ligne : une mesure fausse ne coûte pas
un relevé entier. Les données sont en UTC, mais je calcule l'heure et le jour à l'heure de Paris. Sans ça, la
pointe « entre 7 h et 10 h » du carnet 3 se décalerait d'une heure. La jointure météo est un `left join` sur
`date_trunc("hour")` avec `broadcast`, et une assertion vérifie que le nombre de lignes reste à 10 769 262.

### Jour 2 : SQL, Delta Lake, streaming (carnets 3 et 4)

Pour la première question métier, un `LEFT ANTI JOIN` retire les jours fériés. Je le préfère à `NOT IN`, qui se
comporte mal avec des valeurs nulles, et je compare la date de Paris. Sur la pluie, le taux d'occupation monte
de 1,58 point en brut. Mais la pluie tombe surtout à certaines heures, donc j'ai comparé à heure et type de jour
égaux : l'écart devient 1,52 point. Le taux d'occupation mesure un niveau, alors j'ai ajouté un indicateur de
flux, le nombre de vélos qui entrent ou sortent entre deux relevés consécutifs. Il baisse de 25 % sous la pluie.

Avec Delta Lake, j'écris la table, j'ajoute un second lot, je relis une version antérieure (`versionAsOf`) et je
fais un `MERGE`, une transaction atomique. Je dédoublonne le lot sur la clé de fusion et je le mets en cache.
Sinon `limit()` peut renvoyer d'autres lignes à chaque recalcul, et le lot que je compte ne serait plus celui
que je fusionne.

En streaming, les fenêtres glissent de 2 minutes sur 10, avec un watermark de 5 minutes, obligatoire en mode
`append`. Les alertes passent par `foreachBatch`. Je mesure une rupture entre le premier et le dernier relevé
vide : deux relevés vides consécutifs donnent 15 minutes et déclenchent l'alerte au-delà de 10 minutes, un relevé
vide isolé n'en déclenche aucune.

### Jour 3 : apprentissage automatique (carnet 5)

Les variables sont l'heure, le jour et le mois en sinus et cosinus, la météo, et le taux d'occupation un quart
d'heure et une heure plus tôt. La cible est le taux une heure plus tard. Une seule fonction, `ajouter_features`,
alimente l'entraînement et le pipeline final du carnet 6. Le carnet d'origine fixait `cluster = 0` en production,
ce qui faussait les prédictions.

`lag` et `lead` comptent des lignes, alors que la collecte a des trous de plusieurs heures : « quatre lignes
avant » peut viser un relevé très ancien. Je ne garde que les lignes dont les relevés décalés se situent à
environ 15 minutes et une heure. Il reste 4,7 millions de lignes sur 10,8 millions : 3,99 millions pour
l'entraînement et 0,70 million pour le test.

Je découpe dans le temps, car un tirage aléatoire montrerait le futur au modèle. Je calcule le profil du K-Means
sur l'entraînement seul : sur toutes les données, le cluster de chaque station porterait une trace du test. J'ai
retenu k = 4, où la silhouette remonte (0,55 contre 0,51 à k = 3) et où l'inertie baisse encore de 25 %, contre
12 % de 4 à 5. Les groupes comptent 212, 559, 265 et 331 stations : les unes se remplissent en journée, les
autres restent presque toujours vides ou presque toujours pleines, et les dernières, résidentielles, se vident le
jour et se remplissent le soir.

Le pipeline `VectorAssembler`, `StandardScaler`, `GBTRegressor` s'ajuste sur la moitié de l'entraînement, environ
2 millions de lignes, pour tenir dans mon temps de calcul. L'évaluation couvre tout le test. La validation
croisée (3 plis, 8 combinaisons) utilise 5 % de l'entraînement. MLflow compare quatre runs. J'enregistre le
meilleur dans le Model Registry sous l'alias `champion`, je le recharge, je le sauvegarde et je l'applique à un
lot neuf dans le carnet 6.

### Optimisation (carnet 6)

Spark diffuse de lui-même toute table de moins de 50 Mo : sans précaution, ma comparaison entre `SortMergeJoin`
et broadcast n'aurait rien comparé. J'ai désactivé ce seuil le temps de la mesure. J'ai aussi coupé l'exécution
adaptative pendant l'essai du nombre de partitions, sinon elle les fusionne. Le carnet interroge enfin le Spark
UI par son API REST pour lister les stages les plus longs.

## Résultats

Sur 10 769 264 relevés et 1 402 stations, le modèle atteint un RMSE de 0,097, une MAE de 0,064 et un R² de 0,829
sur avril 2021. Le taux mesuré un quart d'heure plus tôt pèse environ 80 % de la prédiction, l'heure et le
cluster suivent, et chaque variable météo reste sous 0,3 %. La validation croisée retient profondeur 5, 50 arbres
et pas 0,1, sans améliorer le modèle de départ. Les quatre runs MLflow vont de 0,0969 à 0,0989 de RMSE. Trois captures dans `docs/` les montrent : la liste des runs (`mlflow_runs.png`), où
`medium-balanced` porte le modèle `climacity-gbt` en version 1, alias `champion`, puis la comparaison des quatre runs,
en graphique (`mlflow_comparaison_graphique.png`) et en tableaux de paramètres et de métriques
(`mlflow_comparaison_tableaux.png`).

| Mesure | Avant | Après |
|--------|-------|-------|
| Jointure répétée | 3,3 s par passage | 0,15 s avec cache (8 s de remplissage) |
| Jointure météo | SortMergeJoin, 1,09 s, 2 shuffles | broadcast, 0,37 s, aucun shuffle |
| 9 200 lignes | Pandas, 0,010 s | Spark, 0,080 s |
| 7,5 millions de lignes | | Spark, 0,35 s |

Ces temps viennent d'une seule exécution : ils donnent des ordres de grandeur. Pandas gagne sur 9 200 lignes.
Spark met 0,35 s sur 800 fois plus de données. Le salting a gagné une fraction de seconde, dans le bruit de
mesure.

## Limites

- Je n'ai qu'une saison, et elle sort de l'ordinaire. Rien n'autorise à généraliser.
- Je n'ai pas prouvé le rejet des données tardives. Après l'injection d'un retard de 30 minutes, le compteur
  `numRowsDroppedByWatermark` reste à 0. Le fichier n'était peut-être pas traité, ou ce compteur ignore ce cas.
  Le carnet 4 décrit le comportement attendu, sans preuve.
- Le jeu de test n'a aucune ligne valide entre 2 h et 4 h ni entre 18 h et 20 h. Des trous de collecte en sont
  la cause probable, et je ne l'ai pas vérifié.
- Le stage `javaToPython` du Spark UI affiche un ratio maximum sur médiane très élevé. Je ne l'ai pas examiné.
- Deux stations « Château - République » ont le même code d'arrondissement, faute de station actuelle distincte
  pour les départager.
- Je n'ai pas traité les bonus (qualité de l'air, modèle alternatif).

## Arborescence

```
notebooks/   les six carnets exécutés
scripts/     velib_prep.py (collecte), simulateur_flux.py, mesures.py, executer_carnets.py
conf/        configuration Spark
docs/        captures de l'interface MLflow
data/        données, Parquet, Delta, modèles, carte, mesures, journaux (hors dépôt)
```

`Dockerfile`, `docker-compose.yml`, `.env`, `requirements.txt` et `Makefile` décrivent l'environnement.
