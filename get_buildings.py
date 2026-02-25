import duckdb

con = duckdb.connect()
con.execute("INSTALL spatial; INSTALL httpfs;")
con.execute("LOAD spatial; LOAD httpfs;")
con.execute("SET s3_region='us-west-2';")
# Enable progress bar
con.execute("SET enable_progress_bar=true;")

print("connected...")


# check that the script is working...
# result = con.sql("""
#   SELECT COUNT(*) 
#   FROM read_parquet(
#     's3://overturemaps-us-west-2/release/2026-02-18.0/theme=buildings/type=building/*',
#     filename=true,
#     hive_partitioning=1
#   )
#   WHERE
#     bbox.xmin > -77.12 AND bbox.xmax < -76.91
#     AND bbox.ymin > 38.79 AND bbox.ymax < 38.99
# """).fetchone()
# print(result)

# Run the query and save to CSV
con.execute("""
  COPY (
    SELECT
      id,
      height,
      ST_X(ST_Centroid(geometry)) AS centroid_lon,
      ST_Y(ST_Centroid(geometry)) AS centroid_lat
    FROM read_parquet(
      's3://overturemaps-us-west-2/release/2026-02-18.0/theme=buildings/type=building/*',
      filename=true,
      hive_partitioning=1
    )
    WHERE
      bbox.xmin > -77.12 AND bbox.xmax < -76.91
      AND bbox.ymin > 38.79 AND bbox.ymax < 38.99
      AND height IS NOT NULL
  ) TO 'dc_buildings.csv' (HEADER, DELIMITER ',')
""")

print("attempted to grab data...")

print("end of script")
