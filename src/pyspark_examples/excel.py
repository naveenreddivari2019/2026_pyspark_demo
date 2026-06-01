from pyspark.sql import SparkSession
import pandas as pd
import os

# Set environment variables to force localhost
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Create SparkSession
spark = SparkSession.builder \
    .appName("ReadExcelExample") \
    .master("local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.shuffle.service.enabled", "false") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

# Path to Excel file (relative to repo root)
script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
excel_file_path = os.path.join(script_dir, "sample_data.xlsx")

# Method 1: Read specific sheet using pandas and convert to PySpark DataFrame
sheet_name = "Sheet1"  # Change to desired sheet name
pdf = pd.read_excel(excel_file_path, sheet_name=sheet_name)
df = spark.createDataFrame(pdf)

print(f"Data from sheet '{sheet_name}':")
df.show(truncate=False)
df.printSchema()

# Method 2: Read all sheet names
excel_file = pd.ExcelFile(excel_file_path)
print(f"\nAvailable sheets: {excel_file.sheet_names}")

# Method 3: Read multiple sheets
all_dfs = {}
for sheet in excel_file.sheet_names:
    pdf_sheet = pd.read_excel(excel_file_path, sheet_name=sheet)
    all_dfs[sheet] = spark.createDataFrame(pdf_sheet)
    print(f"\nData from sheet '{sheet}':")
    all_dfs[sheet].show(truncate=False)

# Write to CSV (optional)
output_path = os.path.join(script_dir, "excel_output")
df.write.mode("overwrite").csv(output_path, header=True)
print(f"\nData written to: {output_path}")

spark.stop()