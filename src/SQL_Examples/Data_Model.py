import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Data_Model") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ─────────────────────────────────────────
# 1. DIMENSION TABLE — Customers
# ─────────────────────────────────────────
customers_schema = StructType([
    StructField("CustomerID",   StringType(),  False),
    StructField("CustomerName", StringType(),  True)
])

customers_data = [
    ("CUST001", "Alice Johnson"),
    ("CUST002", "Bob Smith"),
    ("CUST003", "Charlie Brown"),
    ("CUST004", "Diana Prince"),
    ("CUST005", "Eve Wilson")
]

customers_df = spark.createDataFrame(customers_data, customers_schema)

# ─────────────────────────────────────────
# 2. DIMENSION TABLE — Products
# ─────────────────────────────────────────
products_schema = StructType([
    StructField("ProductID",   StringType(), False),
    StructField("ProductName", StringType(), True)
])

products_data = [
    ("PROD001", "Basic Plan"),
    ("PROD002", "Standard Plan"),
    ("PROD003", "Premium Plan"),
    ("PROD004", "Enterprise Plan")
]

products_df = spark.createDataFrame(products_data, products_schema)

# ─────────────────────────────────────────
# 3. DIMENSION TABLE — StatusDefinition
# ─────────────────────────────────────────
status_schema = StructType([
    StructField("StatusID",    IntegerType(), False),
    StructField("Description", StringType(),  True)
])

status_data = [
    (1, "Active"),
    (2, "Inactive"),
    (3, "Cancelled"),
    (4, "Suspended"),
    (5, "Expired")
]

status_df = spark.createDataFrame(status_data, status_schema)

# ─────────────────────────────────────────
# 4. DIMENSION TABLE — Users
# ─────────────────────────────────────────
users_schema = StructType([
    StructField("UserID",     StringType(), False),
    StructField("Name",       StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Email",      StringType(), True),
    StructField("CustomerID", StringType(), True),   # FK → Customers
    StructField("AdminID",    StringType(), True)    # FK → Users (self)
])

users_data = [
    ("USR001", "Alice Johnson", "Engineering", "alice@company.com",  "CUST001", None),
    ("USR002", "Bob Smith",     "Finance",     "bob@company.com",    "CUST002", "USR001"),
    ("USR003", "Charlie Brown", "Marketing",   "charlie@company.com","CUST003", "USR001"),
    ("USR004", "Diana Prince",  "HR",          "diana@company.com",  "CUST004", "USR002"),
    ("USR005", "Eve Wilson",    "Engineering", "eve@company.com",    "CUST005", "USR002"),
    ("USR006", "Frank Miller",  "Finance",     "frank@company.com",  "CUST001", "USR003"),
    ("USR007", "Grace Lee",     "Marketing",   "grace@company.com",  "CUST002", "USR003")
]

users_df = spark.createDataFrame(users_data, users_schema)

# ─────────────────────────────────────────
# 5. DIMENSION TABLE — FrontendEventDefinitions
# ─────────────────────────────────────────
event_def_schema = StructType([
    StructField("EventID",     IntegerType(), False),
    StructField("Description", StringType(),  True),
    StructField("EventType",   StringType(),  True)
])

event_def_data = [
    (1,  "Page View",        "Navigation"),
    (2,  "Button Click",     "Interaction"),
    (3,  "Form Submit",      "Interaction"),
    (4,  "Login",            "Authentication"),
    (5,  "Logout",           "Authentication"),
    (6,  "Purchase",         "Transaction"),
    (7,  "Subscription",     "Transaction"),
    (8,  "Cancellation",     "Transaction"),
    (9,  "Profile Update",   "Account"),
    (10, "Password Change",  "Account")
]

event_def_df = spark.createDataFrame(event_def_data, event_def_schema)

# ─────────────────────────────────────────
# 6. FACT TABLE — Subscriptions
# ─────────────────────────────────────────
subscriptions_schema = StructType([
    StructField("SubscriptionID",  StringType(),  False),
    StructField("CustomerID",      StringType(),  True),   # FK → Customers
    StructField("ProductID",       StringType(),  True),   # FK → Products
    StructField("NumberofUsers",   IntegerType(), True),
    StructField("CurrentStatus",   IntegerType(), True),
    StructField("Revenue",         IntegerType(), True),
    StructField("OrderDate",       DateType(),    True),
    StructField("ExpirationDate",  DateType(),    True),
    StructField("Active",          IntegerType(), True)
])

from datetime import date

subscriptions_data = [
    ("SUB001", "CUST001", "PROD001", 5,  1, 500,  date(2024, 1, 1),  date(2025, 1, 1),  1),
    ("SUB002", "CUST001", "PROD003", 10, 1, 1500, date(2024, 2, 1),  date(2025, 2, 1),  1),
    ("SUB003", "CUST002", "PROD002", 3,  2, 300,  date(2023, 6, 1),  date(2024, 6, 1),  0),
    ("SUB004", "CUST002", "PROD004", 20, 1, 4000, date(2024, 3, 15), date(2025, 3, 15), 1),
    ("SUB005", "CUST003", "PROD001", 2,  3, 200,  date(2023, 1, 1),  date(2024, 1, 1),  0),
    ("SUB006", "CUST003", "PROD002", 8,  1, 800,  date(2024, 5, 1),  date(2025, 5, 1),  1),
    ("SUB007", "CUST004", "PROD003", 15, 4, 2250, date(2024, 4, 1),  date(2025, 4, 1),  0),
    ("SUB008", "CUST004", "PROD001", 1,  1, 100,  date(2024, 7, 1),  date(2025, 7, 1),  1),
    ("SUB009", "CUST005", "PROD004", 25, 1, 5000, date(2024, 8, 1),  date(2025, 8, 1),  1),
    ("SUB010", "CUST005", "PROD002", 6,  5, 600,  date(2023, 3, 1),  date(2024, 3, 1),  0)
]

subscriptions_df = spark.createDataFrame(subscriptions_data, subscriptions_schema)

# ─────────────────────────────────────────
# 7. FACT TABLE — PaymentStatusLog
# ─────────────────────────────────────────
payment_schema = StructType([
    StructField("StatusMovementID", StringType(),   False),
    StructField("SubscriptionID",   StringType(),   True),  # FK → Subscriptions
    StructField("StatusID",         IntegerType(),  True),  # FK → StatusDefinition
    StructField("MovementDate",     TimestampType(),True)
])

from datetime import datetime

payment_data = [
    ("PAY001", "SUB001", 1, datetime(2024, 1,  1,  9, 0, 0)),
    ("PAY002", "SUB002", 1, datetime(2024, 2,  1,  9, 0, 0)),
    ("PAY003", "SUB003", 1, datetime(2023, 6,  1,  9, 0, 0)),
    ("PAY004", "SUB003", 2, datetime(2024, 1,  1, 10, 0, 0)),
    ("PAY005", "SUB004", 1, datetime(2024, 3, 15,  9, 0, 0)),
    ("PAY006", "SUB005", 1, datetime(2023, 1,  1,  9, 0, 0)),
    ("PAY007", "SUB005", 3, datetime(2023, 6,  1, 11, 0, 0)),
    ("PAY008", "SUB006", 1, datetime(2024, 5,  1,  9, 0, 0)),
    ("PAY009", "SUB007", 1, datetime(2024, 4,  1,  9, 0, 0)),
    ("PAY010", "SUB007", 4, datetime(2024, 9,  1, 14, 0, 0)),
    ("PAY011", "SUB008", 1, datetime(2024, 7,  1,  9, 0, 0)),
    ("PAY012", "SUB009", 1, datetime(2024, 8,  1,  9, 0, 0)),
    ("PAY013", "SUB010", 1, datetime(2023, 3,  1,  9, 0, 0)),
    ("PAY014", "SUB010", 5, datetime(2024, 3,  1, 12, 0, 0))
]

payment_df = spark.createDataFrame(payment_data, payment_schema)

# ─────────────────────────────────────────
# 8. FACT TABLE — FrontendEventLog
# ─────────────────────────────────────────
event_log_schema = StructType([
    StructField("EventLogID",      StringType(),   False),
    StructField("UserID",          StringType(),   True),  # FK → Users
    StructField("EventID",         IntegerType(),  True),  # FK → FrontendEventDefinitions
    StructField("EventTimestamp",  TimestampType(),True)
])

event_log_data = [
    ("EVT001", "USR001", 4,  datetime(2024, 1,  1,  8, 30, 0)),
    ("EVT002", "USR001", 1,  datetime(2024, 1,  1,  8, 35, 0)),
    ("EVT003", "USR001", 7,  datetime(2024, 1,  1,  9,  0, 0)),
    ("EVT004", "USR002", 4,  datetime(2024, 2,  1,  9,  0, 0)),
    ("EVT005", "USR002", 6,  datetime(2024, 2,  1,  9, 30, 0)),
    ("EVT006", "USR003", 4,  datetime(2024, 3,  1, 10,  0, 0)),
    ("EVT007", "USR003", 8,  datetime(2024, 3, 15, 11,  0, 0)),
    ("EVT008", "USR004", 4,  datetime(2024, 4,  1,  8,  0, 0)),
    ("EVT009", "USR004", 3,  datetime(2024, 4,  1,  8, 30, 0)),
    ("EVT010", "USR005", 4,  datetime(2024, 5,  1,  9,  0, 0)),
    ("EVT011", "USR005", 2,  datetime(2024, 5,  1,  9, 15, 0)),
    ("EVT012", "USR006", 9,  datetime(2024, 6,  1, 10,  0, 0)),
    ("EVT013", "USR007", 10, datetime(2024, 7,  1, 11,  0, 0)),
    ("EVT014", "USR001", 5,  datetime(2024, 1,  1, 17,  0, 0))
]

event_log_df = spark.createDataFrame(event_log_data, event_log_schema)

# ─────────────────────────────────────────
# 9. FACT TABLE — Cancelations
# ─────────────────────────────────────────
cancel_schema = StructType([
    StructField("SubscriptionID",   StringType(), False),  # FK → Subscriptions
    StructField("CancelationReason1", StringType(), True),
    StructField("CancelationReason2", StringType(), True),
    StructField("CancelationReason3", StringType(), True),
    StructField("CancelDate",        DateType(),    True)
])

cancel_data = [
    ("SUB003", "Too Expensive",    "Lack of Features", None,               date(2024, 1,  1)),
    ("SUB005", "Found Alternative","Poor Support",     "Technical Issues", date(2023, 6,  1)),
    ("SUB007", "Budget Cuts",      "Downsizing",       None,               date(2024, 9,  1)),
    ("SUB010", "Contract Ended",   None,               None,               date(2024, 3,  1))
]

cancel_df = spark.createDataFrame(cancel_data, cancel_schema)

# ─────────────────────────────────────────
# CREATE TEMP VIEWS
# ─────────────────────────────────────────
customers_df.createOrReplaceTempView("Customers")
products_df.createOrReplaceTempView("Products")
status_df.createOrReplaceTempView("StatusDefinition")
users_df.createOrReplaceTempView("Users")
event_def_df.createOrReplaceTempView("FrontendEventDefinitions")
subscriptions_df.createOrReplaceTempView("Subscriptions")
payment_df.createOrReplaceTempView("PaymentStatusLog")
event_log_df.createOrReplaceTempView("FrontendEventLog")
cancel_df.createOrReplaceTempView("Cancelations")

# ─────────────────────────────────────────
# PRINT ALL TABLES
# ─────────────────────────────────────────
print("=== CUSTOMERS (Dimension) ===")
customers_df.show()

print("=== PRODUCTS (Dimension) ===")
products_df.show()

print("=== STATUS DEFINITION (Dimension) ===")
status_df.show()

print("=== USERS (Dimension) ===")
users_df.show()

print("=== FRONTEND EVENT DEFINITIONS (Dimension) ===")
event_def_df.show()

print("=== SUBSCRIPTIONS (Fact) ===")
subscriptions_df.show()

print("=== PAYMENT STATUS LOG (Fact) ===")
payment_df.show()

print("=== FRONTEND EVENT LOG (Fact) ===")
event_log_df.show()

print("=== CANCELATIONS (Fact) ===")
cancel_df.show()

print("✅ All tables created successfully!")