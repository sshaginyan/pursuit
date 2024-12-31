from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    length,
    levenshtein,
    greatest,
    row_number,
    desc
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
        .appName("Pursuit")
        .getOrCreate()
)

df_places = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("csv/places.csv")
    .alias("places")
)

df_contacts = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("csv/contacts.csv")
    .alias("contacts")
)

# Define fuzzy similarity based on Levenshtein distance
def similarity_expr(col1, col2):
    # Similarity = 1 - (distance / maxLength)
    return 1 - (levenshtein(col1, col2) / greatest(length(col1), length(col2)))

df_cross = df_places.crossJoin(df_contacts)

df_with_scores = (
    df_cross
    .withColumn("similarity_url",
        similarity_expr(col("places.url"), col("contacts.url"))
    )
    .withColumn("similarity_emails",
        similarity_expr(col("places.url"), col("contacts.emails"))
    )
    .withColumn(
        "similarity",
        greatest(col("similarity_url"), col("similarity_emails"))
    )
)

# df_with_scores.show(truncate=False)

windowSpec = Window.partitionBy("places.url").orderBy(desc("similarity"))

df_ranked = df_with_scores.withColumn(
    "rank", 
    row_number().over(windowSpec)
)

df_top_match = df_ranked.filter(col("rank") == 1)

# df_top_match.show(truncate=False)

df_enriched = (
    df_places.alias("p")
    .join(
        df_top_match.select(
            col("places.url").alias("purl"),
            col("similarity").alias("highest_score"),
            col("contacts.place_id").alias("contact_place_id"),
        ),
        on=[col("p.url") == col("purl")],
        how="left"
    )
    .drop("purl")
)

df_enriched \
    .coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("csv/enriched_places")