from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col
import pandas as pd
import os

spark =  SparkSession.builder.appName("Jupyter").getOrCreate()
 
match_details = spark.read.option("header", "true").csv("match_details.csv")

matches = spark.read.option("header", "true").csv("matches.csv")

medals_matches_players = spark.read.option("header", "true").csv("medals_matches_players.csv")

medals = spark \
          .read \
          .option("header", "true").csv("medals.csv") \
          .select(col("medal_id"),
                  col("name").alias("medal_name"))

maps = spark \
        .read \
        .option("header", "true") \
        .csv("maps.csv") \
        .select(col("mapid"),
                col("name").alias("map_name"))
 
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
 
spark.sql("DROP TABLE IF EXISTS bucketed_matches")
spark.sql("DROP TABLE IF EXISTS bucketed_match_details")
spark.sql("DROP TABLE IF EXISTS bucketed_medals_matches_players")

(matches
 .write
 .format("parquet")
 .bucketBy(16, "match_id")
 .sortBy("match_id")
 .saveAsTable("bucketed_matches"))
 
(match_details
 .write
 .format("parquet")
 .bucketBy(16, "match_id")
 .sortBy("match_id")
 .saveAsTable("bucketed_match_details"))
 
(medals_matches_players
 .write
 .format("parquet")
 .bucketBy(16, "match_id")
 .sortBy("match_id")
 .saveAsTable("bucketed_medals_matches_players"))
 
joined_df = spark.sql("""
 SELECT 
 m.match_id,
 m.mapid,
 m.playlist_id,
 m.game_variant_id,
 m.map_variant_id,
 md.player_gamertag,
 md.player_total_kills,
 md.player_total_deaths,
 md.did_win,
 md.team_id,
 mmp.medal_id,
 mmp.count AS count_medals
 FROM bucketed_matches m
 JOIN bucketed_match_details md
 JOIN bucketed_medals_matches_players mmp
 ON m.match_id = md.match_id AND md.match_id = mmp.match_id
 """)

final_df = joined_df \
            .join(broadcast(medals), "medal_id") \
            .join(broadcast(maps), "mapid")

final_df.createOrReplaceTempView("final_df")

# Which player averages the most kills per game?
player_avg_most_kills = spark.sql("""
    WITH agg_avg_cte AS (    
      SELECT 
        player_gamertag, 
        AVG(player_total_kills) AS avg_kills
      FROM
        final_df
      GROUP BY
        player_gamertag)

    SELECT 
      player_gamertag, 
      avg_kills
    FROM 
      agg_avg_cte
    WHERE
      avg_kills = (SELECT MAX(avg_kills) FROM agg_avg_cte)
            """)
# Which playlist gets played the most?
playlist_most_played = spark.sql("""
    WITH count_playlist_cte AS (
      SELECT 
        playlist_id, 
        COUNT(playlist_id) AS count_playlist_id
      FROM
        final_df
      GROUP BY
        playlist_id)
    
    SELECT
      playlist_id,
      count_playlist_id
    FROM
      count_playlist_cte
    WHERE count_playlist_id = (SELECT MAX(count_playlist_id) FROM count_playlist_cte)
""")

# Which map gets played the most?
map_most_played = spark.sql("""
    WITH count_mapid_cte AS (
      SELECT 
        mapid,
        map_name, 
        COUNT(mapid) AS count_mapid
      FROM
        final_df
      GROUP BY
        mapid,
        map_name)
    
    SELECT
      mapid,
      map_name,
      count_mapid
    FROM
      count_mapid_cte
    WHERE count_mapid = (SELECT MAX(count_mapid) FROM count_mapid_cte)
""")

# Which map do players get the most Killing Spree medals on?
map_most_played_killing_on_spree_medal = spark.sql("""
    WITH count_mapid_cte AS (
      SELECT 
        mapid,
        map_name, 
        COUNT(mapid) AS count_mapid
      FROM
        final_df
      WHERE
        medal_name = "Killing Spree"
      GROUP BY
        mapid,
        map_name)
    
    SELECT
      mapid,
      map_name,
      count_mapid
    FROM
      count_mapid_cte
    WHERE count_mapid = (SELECT MAX(count_mapid) FROM count_mapid_cte)
""")

#finding out columns cardinality
spark.sql("SELECT COUNT(DISTINCT playlist_id) FROM final_df").show()
spark.sql("SELECT COUNT(DISTINCT mapid) FROM final_df").show()
spark.sql("SELECT COUNT(DISTINCT player_gamertag) FROM final_df").show()
spark.sql("SELECT COUNT(DISTINCT match_id) FROM final_df").show()


sort_by_low_cardinality_df = final_df.sortWithinPartitions(col("mapid"), col("playlist_id"))
sort_by_high_cardinality_df = final_df.sortWithinPartitions(col("player_gamertag"))

sort_by_low_cardinality_df.write.format("parquet").mode("overwrite").saveAsTable("optimized_table")
sort_by_high_cardinality_df.write.format("parquet").mode("overwrite").saveAsTable("non_optimized_table")

parquet_dir_opt = "/content/spark-warehouse/optimized_table"
files_opt = []
for f in os.listdir(parquet_dir_opt):
    path = os.path.join(parquet_dir_opt, f)
    size = os.path.getsize(path)
    files_opt.append({"file_path": path, "file_size_in_bytes": size})

df_files_opt = pd.DataFrame(files_opt)


parquet_dir_non_opt = "/content/spark-warehouse/non_optimized_table"
files_non_opt = []
for f in os.listdir(parquet_dir_non_opt):
    path = os.path.join(parquet_dir_non_opt, f)
    size = os.path.getsize(path)
    files_non_opt.append({"file_path": path, "file_size_in_bytes": size})

df_files_non_opt = pd.DataFrame(files_non_opt)

opt_total = df_files_opt["file_size_in_bytes"].sum()
non_opt_total = df_files_non_opt["file_size_in_bytes"].sum()

df_summary = pd.DataFrame([
    {"table": "optimized", "total_size_bytes": opt_total},
    {"table": "non_optimized", "total_size_bytes": non_opt_total}
])

df_summary

#since I used google colab, I couldn't use format("delta") and SELECT * FROM .files to check sizes