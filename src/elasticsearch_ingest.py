from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, array, array_remove, size
from pyspark.sql.types import IntegerType, ArrayType, StringType
from elasticsearch import Elasticsearch
import time

# Création de la session Spark
spark = SparkSession.builder \
    .appName("Indexation des films") \
    .getOrCreate()

time.sleep(5)  # Attendre que Elasticsearch soit prêt

# Liste des genres de films
genres = ["unknown", "Action", "Adventure", "Animation", "Childrens", "Comedy", "Crime", 
          "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystery", 
          "Romance", "Sci-Fi", "Thriller", "War", "Western"]

# Colonnes dans le fichier de données
columns = ["movieId", "title", "release_date", "video_release_date", "imdb_url"] + genres

# Lecture des données
print("Lecture du fichier de données...")
movies_df = spark.read \
    .option("delimiter", "|") \
    .csv("/data/u.item") \
    .toDF(*columns)

# Conversion des types pour les colonnes numériques
movies_df = movies_df.withColumn("movieId", col("movieId").cast(IntegerType()))

# Affichage des premières lignes pour vérification
print("Échantillon de données brutes (5 premières lignes):")
movies_df.select("movieId", "title", "Animation").show(5, truncate=False)

# Création d'un dataframe simplifié avec movieId, title et genres
# MÉTHODE SIMPLIFIÉE POUR EXTRAIRE LES GENRES
movies_with_genres = movies_df.select(
    col("movieId").cast(IntegerType()),
    col("title"),
    col("release_date")
)

# Création d'un dataframe intermédiaire pour les genres
# Pour chaque film, nous allons créer des lignes pour chaque genre présent
genres_df_list = []

for genre in genres:
    if genre != "unknown":  # Ignorer le genre "unknown"
        # Sélectionner les films qui ont ce genre (valeur 1)
        genre_df = movies_df.filter(col(genre) == "1").select(
            col("movieId").cast(IntegerType()),
            lit(genre).alias("genre")
        )
        genres_df_list.append(genre_df)

# Combiner tous les dataframes de genres
if genres_df_list:
    all_genres_df = genres_df_list[0]
    for df in genres_df_list[1:]:
        all_genres_df = all_genres_df.union(df)
    
    # Grouper par movieId pour obtenir une liste de genres pour chaque film
    from pyspark.sql.functions import collect_list
    movie_genres_df = all_genres_df.groupBy("movieId").agg(
        collect_list("genre").alias("genres")
    )
    
    # Joindre avec le dataframe des films
    final_df = movies_with_genres.join(movie_genres_df, on="movieId", how="left")
else:
    # Si aucun genre n'est trouvé, ajouter une colonne vide
    final_df = movies_with_genres.withColumn("genres", lit(None).cast(ArrayType(StringType())))

# Affichage des films avec leurs genres
print("Films avec leurs genres (5 premières lignes):")
final_df.show(5, truncate=False)

# Vérification du nombre de films par nombre de genres
print("Nombre de films par nombre de genres:")
final_df.withColumn("num_genres", size(col("genres"))).groupBy("num_genres").count().orderBy("num_genres").show()

# Conversion vers Pandas pour l'indexation
movies_pd = final_df.toPandas()
print(f"Conversion vers Pandas terminée, total des lignes: {len(movies_pd)}")

# Configuration de la connexion Elasticsearch
es = Elasticsearch("http://elasticsearch:9200")

# Vérification que l'index existe ou le créer
index_name = "movies"
if not es.indices.exists(index=index_name):
    # Utiliser une syntaxe compatible avec Elasticsearch 8.x
    mapping = {
        "mappings": {
            "properties": {
                "movieId": {"type": "integer"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "release_date": {"type": "text"},
                "genres": {"type": "keyword"}
            }
        }
    }
    es.indices.create(index=index_name, mappings=mapping["mappings"])
    print(f"Index '{index_name}' créé avec mapping approprié")

# Indexation des documents
print("Début de l'indexation vers Elasticsearch...")
successful = 0
failed = 0

for index, row in movies_pd.iterrows():
    try:
        # Traitement des genres pour s'assurer que c'est une liste
        if isinstance(row["genres"], list):
            genres_list = row["genres"]
        elif hasattr(row["genres"], "tolist"):
            genres_list = row["genres"].tolist()
        else:
            genres_list = []
        
        # Vérification supplémentaire pour les types de données
        if genres_list is None:
            genres_list = []
        
        # Construction du document
        doc = {
            "movieId": int(row["movieId"]),
            "title": str(row["title"]),
            "release_date": str(row["release_date"]) if row["release_date"] else None,
            "genres": genres_list
        }
        
        # Indexer le document
        response = es.index(index=index_name, id=row["movieId"], document=doc)
        
        if index % 100 == 0 or index < 5:  # Afficher les 5 premiers et ensuite tous les 100
            print(f"Indexé: {index}/{len(movies_pd)} - ID: {row['movieId']} - Titre: {row['title']} - Genres: {genres_list}")
        
        successful += 1
    except Exception as e:
        print(f"Erreur lors de l'indexation du film {row['movieId']}: {str(e)}")
        failed += 1

# Rafraîchir l'index pour rendre les documents recherchables immédiatement
es.indices.refresh(index=index_name)

print(f"Indexation terminée. Succès: {successful}, Échecs: {failed}")

# Afficher quelques exemples pour vérification en utilisant la nouvelle syntaxe d'Elasticsearch
print("\nVérification de quelques documents dans Elasticsearch:")
results = es.search(index=index_name, query={"match_all": {}}, size=5)
for hit in results["hits"]["hits"]:
    print(f"ID: {hit['_id']}, Title: {hit['_source']['title']}, Genres: {hit['_source']['genres']}")

# Vérification que les genres ne sont pas vides
print("\nDistribution des genres dans l'index:")
results_with_genres = es.search(
    index=index_name,
    query={"range": {"movieId": {"gte": 1}}},
    aggs={"genres_count": {"terms": {"field": "genres", "size": 20}}},
    size=0
)

if "aggregations" in results_with_genres and "genres_count" in results_with_genres["aggregations"]:
    for bucket in results_with_genres["aggregations"]["genres_count"]["buckets"]:
        print(f"Genre: {bucket['key']}, Count: {bucket['doc_count']}")
else:
    print("Aucune agrégation de genres trouvée")

spark.stop()