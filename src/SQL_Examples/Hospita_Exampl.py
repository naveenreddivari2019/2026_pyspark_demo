import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Hospital_Example") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ─────────────────────────────────────────
# PREPARE DATA — DOCTORS Table
# ─────────────────────────────────────────
doctors_schema = StructType([
    StructField("id",               IntegerType(), False),
    StructField("name",             StringType(),  True),
    StructField("speciality",       StringType(),  True),
    StructField("hospital",         StringType(),  True),
    StructField("city",             StringType(),  True),
    StructField("consultation_fee", IntegerType(), True)
])

doctors_data = [
    (1, "Dr. Shashank", "Ayurveda",     "Apollo Hospital",      "Bangalore", 2500),
    (2, "Dr. Abdul",    "Homeopathy",   "Fortis Hospital",      "Bangalore", 2000),
    (3, "Dr. Shwetha",  "Homeopathy",   "KMC Hospital",         "Manipal",   1000),
    (4, "Dr. Murphy",   "Dermatology",  "KMC Hospital",         "Manipal",   1500),
    (5, "Dr. Farhana",  "Physician",    "Gleneagles Hospital",  "Bangalore", 1700),
    (6, "Dr. Maryam",   "Physician",    "Gleneagles Hospital",  "Bangalore", 1500)
]

doctors_df = spark.createDataFrame(doctors_data, doctors_schema)

# Create Temp View
doctors_df.createOrReplaceTempView("DOCTORS")

print("=== DOCTORS TABLE ===")
doctors_df.show(truncate=False)

# ─────────────────────────────────────────
# Q4. Doctors in SAME Hospital, DIFFERENT Specialty
# ─────────────────────────────────────────

# ── SQL Approach ──────────────────────────
print("=== Q4. SQL — Same Hospital, Different Specialty ===")
spark.sql("""
    SELECT
        d1.name        AS doctor1,
        d2.name        AS doctor2,
        d1.hospital    AS hospital,
        d1.speciality  AS specialty1,
        d2.speciality  AS specialty2
    FROM DOCTORS d1
    JOIN DOCTORS d2
        ON  d1.hospital  = d2.hospital      -- Same Hospital
        AND d1.speciality != d2.speciality  -- Different Specialty
        AND d1.id < d2.id                   -- Avoid duplicates (A,B) & (B,A)
    ORDER BY d1.hospital
""").show(truncate=False)

# ── PySpark API Approach ──────────────────
print("=== Q4. PySpark — Same Hospital, Different Specialty ===")
d1 = doctors_df.alias("d1")
d2 = doctors_df.alias("d2")

result_q4 = d1.join(d2,
        (F.col("d1.hospital")   == F.col("d2.hospital"))   &   # Same Hospital
        (F.col("d1.speciality") != F.col("d2.speciality")) &   # Different Specialty
        (F.col("d1.id")          < F.col("d2.id"))             # Avoid duplicates
    ) \
    .select(
        F.col("d1.name").alias("doctor1"),
        F.col("d2.name").alias("doctor2"),
        F.col("d1.hospital").alias("hospital"),
        F.col("d1.speciality").alias("specialty1"),
        F.col("d2.speciality").alias("specialty2")
    ) \
    .orderBy("hospital")

result_q4.show(truncate=False)

# ─────────────────────────────────────────
# ADDITIONAL QUERY — Same Hospital, ANY Specialty
# ─────────────────────────────────────────

# ── SQL Approach ──────────────────────────
print("=== Additional SQL — Same Hospital (Any Specialty) ===")
spark.sql("""
    SELECT
        d1.name       AS doctor1,
        d2.name       AS doctor2,
        d1.hospital   AS hospital,
        d1.speciality AS specialty1,
        d2.speciality AS specialty2
    FROM DOCTORS d1
    JOIN DOCTORS d2
        ON  d1.hospital = d2.hospital   -- Same Hospital
        AND d1.id < d2.id               -- Avoid duplicates
    ORDER BY d1.hospital
""").show(truncate=False)

# ── PySpark API Approach ──────────────────
print("=== Additional PySpark — Same Hospital (Any Specialty) ===")
result_additional = d1.join(d2,
        (F.col("d1.hospital") == F.col("d2.hospital")) &   # Same Hospital
        (F.col("d1.id")        < F.col("d2.id"))           # Avoid duplicates
    ) \
    .select(
        F.col("d1.name").alias("doctor1"),
        F.col("d2.name").alias("doctor2"),
        F.col("d1.hospital").alias("hospital"),
        F.col("d1.speciality").alias("specialty1"),
        F.col("d2.speciality").alias("specialty2")
    ) \
    .orderBy("hospital")

result_additional.show(truncate=False)

# ─────────────────────────────────────────
# BONUS — Count of Doctors per Hospital
# ─────────────────────────────────────────
print("=== BONUS — Doctors Count per Hospital ===")
spark.sql("""
    SELECT
        hospital,
        city,
        COUNT(id)            AS total_doctors,
        COUNT(DISTINCT speciality) AS total_specialities,
        MIN(consultation_fee) AS min_fee,
        MAX(consultation_fee) AS max_fee,
        ROUND(AVG(consultation_fee), 0) AS avg_fee
    FROM DOCTORS
    GROUP BY hospital, city
    ORDER BY total_doctors DESC
""").show(truncate=False)

print("✅ Done!")