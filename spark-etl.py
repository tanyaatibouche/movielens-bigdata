
"""
MovieLens ETL Pipeline avec Apache Spark

Ce script implémente un pipeline ETL complet pour transformer les données brutes 
MovieLens en un dataset silver enrichi et structuré.

Architecture ETL:
- EXTRACT: Chargement des CSV depuis HDFS
- TRANSFORM: Nettoyage, enrichissement et agrégation des données
- LOAD: Sauvegarde au format Parquet pour analyse

Auteur: Tanya TIBOUCHE - Fitahiany Michèle MBOHOAZY
Date: Janvier 2026
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, year, regexp_extract, explode, split, 
    round as spark_round, desc, trim
)
from pyspark.sql.types import IntegerType, FloatType
import sys
import logging

# Configuration du logging pour suivre l'exécution
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name="MovieLens-ETL"):
    """
    Crée et configure une session Spark
    
    SparkSession est le point d'entrée pour utiliser Spark.
    Il configure:
    - La connexion au cluster Hadoop
    - Les ressources mémoire et CPU
    - Les paramètres d'optimisation
    
    Args:
        app_name: Nom de l'application Spark
        
    Returns:
        SparkSession configurée
    """
    logger.info(f"Création de la session Spark: {app_name}")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Définir le niveau de log pour réduire la verbosité
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Session Spark créée - Version: {spark.version}")
    return spark


def extract_data(spark, hdfs_path="/user/hadoop/movielens/"):
    """
    EXTRACT: Charge les données brutes depuis HDFS
    
    Cette fonction lit les fichiers CSV stockés dans HDFS et crée des DataFrames.
    Les DataFrames sont des collections distribuées de données organisées en colonnes,
    similaires à des tables SQL ou des DataFrames pandas mais distribuées.
    
    Args:
        spark: Session Spark active
        hdfs_path: Chemin HDFS où se trouvent les fichiers CSV
        
    Returns:
        dict: Dictionnaire contenant les DataFrames movies et ratings
    """
    logger.info("=" * 60)
    logger.info("PHASE EXTRACT: Chargement des données depuis HDFS")
    logger.info("=" * 60)
    
    try:
        # Chargement du fichier movies.csv
        # Options:
        # - header=True: La première ligne contient les noms de colonnes
        # - inferSchema=True: Spark détecte automatiquement les types de données
        logger.info(f"Chargement de {hdfs_path}movies.csv")
        movies_df = spark.read.csv(
            f"{hdfs_path}movies.csv",
            header=True,
            inferSchema=True
        )
        
        logger.info(f"Chargement de {hdfs_path}ratings.csv")
        ratings_df = spark.read.csv(
            f"{hdfs_path}ratings.csv",
            header=True,
            inferSchema=True
        )
        
        # Affichage des informations sur les DataFrames
        logger.info(f"\n📊 Movies DataFrame:")
        logger.info(f"   - Nombre de lignes: {movies_df.count():,}")
        logger.info(f"   - Nombre de colonnes: {len(movies_df.columns)}")
        logger.info(f"   - Colonnes: {movies_df.columns}")
        movies_df.printSchema()
        
        logger.info(f"\n📊 Ratings DataFrame:")
        logger.info(f"   - Nombre de lignes: {ratings_df.count():,}")
        logger.info(f"   - Nombre de colonnes: {len(ratings_df.columns)}")
        logger.info(f"   - Colonnes: {ratings_df.columns}")
        ratings_df.printSchema()
        
        # Affichage des premières lignes pour vérification
        logger.info("\n🔍 Aperçu des données movies:")
        movies_df.show(5, truncate=False)
        
        logger.info("\n🔍 Aperçu des données ratings:")
        ratings_df.show(5, truncate=False)
        
        return {
            "movies": movies_df,
            "ratings": ratings_df
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement des données: {str(e)}")
        sys.exit(1)


def transform_data(dataframes):
    """
    TRANSFORM: Nettoie, enrichit et agrège les données
    
    Cette fonction applique plusieurs transformations:
    1. Extraction de l'année de sortie depuis le titre
    2. Explosion des genres (1 ligne par genre par film)
    3. Agrégation des ratings (moyenne et compte)
    4. Jointure des données movies et ratings
    
    Concepts Spark importants:
    - withColumn(): Ajoute ou modifie une colonne
    - regexp_extract(): Extraction par expression régulière
    - explode(): Transforme un array en plusieurs lignes
    - groupBy() + agg(): Agrégation distribuée
    - join(): Jointure distribuée entre DataFrames
    
    Args:
        dataframes: Dict contenant les DataFrames movies et ratings
        
    Returns:
        DataFrame: Dataset silver enrichi et structuré
    """
    logger.info("\n" + "=" * 60)
    logger.info("PHASE TRANSFORM: Transformation des données")
    logger.info("=" * 60)
    
    movies_df = dataframes["movies"]
    ratings_df = dataframes["ratings"]
    
    # -----------------------------------------------------------------------
    # Étape 1: Extraction de l'année de sortie
    # -----------------------------------------------------------------------
    logger.info("\n🔧 Étape 1: Extraction de l'année depuis le titre")
    
    # Expression régulière pour extraire l'année entre parenthèses
    # Exemple: "Toy Story (1995)" -> 1995
    movies_with_year = movies_df.withColumn(
        "year",
        regexp_extract(col("title"), r"\((\d{4})\)", 1).cast(IntegerType())
    )
    
    # Nettoyage du titre (enlever l'année)
    movies_with_year = movies_with_year.withColumn(
        "clean_title",
        regexp_extract(col("title"), r"^(.*?)\s*\(\d{4}\)", 1)
    )
    
    logger.info("✅ Années extraites et titres nettoyés")
    movies_with_year.select("movieId", "title", "clean_title", "year").show(5, truncate=False)
    
    # -----------------------------------------------------------------------
    # Étape 2: Explosion des genres
    # -----------------------------------------------------------------------
    logger.info("\n🔧 Étape 2: Explosion des genres (1 ligne par genre)")
    
    # Les genres sont séparés par '|' dans le CSV
    # Exemple: "Adventure|Animation|Children" devient 3 lignes séparées
    # explode() est une transformation qui "déroule" un array en plusieurs lignes
    movies_exploded = movies_with_year.withColumn(
        "genre",
        explode(split(col("genres"), "\\|"))
    )
    
    # Nettoyage des espaces et gestion du cas "(no genres listed)"
    movies_exploded = movies_exploded.withColumn(
        "genre",
        trim(col("genre"))
    ).filter(
        col("genre") != "(no genres listed)"
    )
    
    logger.info("✅ Genres explosés")
    logger.info(f"   Nombre de lignes après explosion: {movies_exploded.count():,}")
    movies_exploded.select("movieId", "clean_title", "year", "genre").show(10)
    
    # -----------------------------------------------------------------------
    # Étape 3: Agrégation des ratings
    # -----------------------------------------------------------------------
    logger.info("\n🔧 Étape 3: Calcul des statistiques de rating")
    
    # GroupBy distribue le calcul sur tous les workers du cluster
    # agg() permet de calculer plusieurs agrégations en une passe
    ratings_agg = ratings_df.groupBy("movieId").agg(
        count("rating").alias("num_ratings"),
        spark_round(avg("rating"), 2).alias("avg_rating")
    )
    
    logger.info("✅ Agrégations calculées")
    logger.info(f"   Nombre de films avec des ratings: {ratings_agg.count():,}")
    ratings_agg.orderBy(desc("num_ratings")).show(10)
    
    # -----------------------------------------------------------------------
    # Étape 4: Jointure finale
    # -----------------------------------------------------------------------
    logger.info("\n🔧 Étape 4: Jointure movies + ratings")
    
    # Left join pour garder tous les films, même sans ratings
    # La jointure est distribuée: Spark optimise automatiquement
    silver_dataset = movies_exploded.join(
        ratings_agg,
        on="movieId",
        how="left"
    )
    
    # Remplacement des valeurs nulles pour les films sans ratings
    silver_dataset = silver_dataset.fillna({
        "num_ratings": 0,
        "avg_rating": 0.0
    })
    
    # Sélection et réorganisation des colonnes finales
    silver_dataset = silver_dataset.select(
        col("movieId").alias("movie_id"),
        col("clean_title").alias("movie_name"),
        col("year").alias("year_of_release"),
        col("num_ratings").alias("number_of_ratings"),
        col("genre"),
        col("avg_rating").alias("rating_average")
    )
    
    logger.info("✅ Jointure terminée")
    logger.info(f"\n📊 Dataset Silver final:")
    logger.info(f"   - Nombre total de lignes: {silver_dataset.count():,}")
    logger.info(f"   - Schéma:")
    silver_dataset.printSchema()
    
    logger.info("\n🔍 Aperçu du dataset final:")
    silver_dataset.orderBy(desc("number_of_ratings")).show(20, truncate=False)
    
    return silver_dataset


def load_data(silver_dataset, output_path="/user/hadoop/movielens/silver/"):
    """
    LOAD: Sauvegarde le dataset silver au format Parquet
    
    Parquet est un format de fichier columnar optimisé pour:
    - La compression (fichiers plus petits)
    - Les requêtes analytiques (lecture rapide de colonnes spécifiques)
    - La compatibilité avec tout l'écosystème big data
    
    Le mode "overwrite" écrase les données existantes.
    
    Args:
        silver_dataset: DataFrame à sauvegarder
        output_path: Chemin HDFS de destination
    """
    logger.info("\n" + "=" * 60)
    logger.info("PHASE LOAD: Sauvegarde du dataset silver")
    logger.info("=" * 60)
    
    try:
        logger.info(f"💾 Sauvegarde vers: {output_path}")
        logger.info("   Format: Parquet (columnar, compressé)")
        
        # Repartition pour optimiser la taille des fichiers
        # coalesce(10) réduit le nombre de partitions à 10
        silver_dataset.coalesce(10).write.parquet(
            output_path,
            mode="overwrite"
        )
        
        logger.info("✅ Sauvegarde réussie!")
        logger.info(f"   Les données sont disponibles dans HDFS: {output_path}")
        
        # Optionnel: sauvegarder aussi en CSV pour compatibilité
        csv_path = output_path.replace("/silver/", "/silver_csv/")
        logger.info(f"\n💾 Sauvegarde supplémentaire en CSV: {csv_path}")
        
        silver_dataset.coalesce(1).write.csv(
            csv_path,
            mode="overwrite",
            header=True
        )
        
        logger.info("✅ CSV sauvegardé également")
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la sauvegarde: {str(e)}")
        sys.exit(1)


def main():
    """
    Fonction principale orchestrant le pipeline ETL complet
    
    Workflow:
    1. Création de la session Spark
    2. EXTRACT: Chargement des données brutes
    3. TRANSFORM: Transformations et enrichissements
    4. LOAD: Sauvegarde du dataset silver
    5. Nettoyage et fermeture
    """
    logger.info("\n" + "🎬 " * 20)
    logger.info("DÉMARRAGE DU PIPELINE ETL MOVIELENS")
    logger.info("🎬 " * 20 + "\n")
    
    spark = None
    
    try:
        # Création de la session Spark
        spark = create_spark_session()
        
        # Exécution du pipeline ETL
        dataframes = extract_data(spark)
        silver_dataset = transform_data(dataframes)
        load_data(silver_dataset)
        
        # Résumé final
        logger.info("\n" + "🎉 " * 20)
        logger.info("PIPELINE ETL TERMINÉ AVEC SUCCÈS!")
        logger.info("🎉 " * 20)
        logger.info("\n📋 Résumé:")
        logger.info(f"   ✓ Données extraites de HDFS")
        logger.info(f"   ✓ Transformations appliquées")
        logger.info(f"   ✓ Dataset silver sauvegardé en Parquet et CSV")
        logger.info(f"\n➡️  Prochaine étape: Exécuter spark-data-analysis.py")
        
    except Exception as e:
        logger.error(f"\n❌ Erreur fatale dans le pipeline: {str(e)}")
        sys.exit(1)
        
    finally:
        # Toujours fermer la session Spark pour libérer les ressources
        if spark:
            logger.info("\n🔒 Fermeture de la session Spark...")
            spark.stop()
            logger.info("✅ Session fermée")


if __name__ == "__main__":
    main()