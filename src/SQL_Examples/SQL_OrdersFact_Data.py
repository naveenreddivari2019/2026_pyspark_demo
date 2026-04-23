import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

spark = SparkSession.builder \
    .appName("OrdersFactData") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


class OrdersFactDataExample:

    def PrepareData(self):

        # ─────────────────────────────────────────
        # 1. DIMENSION — Customers
        # ─────────────────────────────────────────
        customers_schema = StructType([
            StructField("CustomerID",   IntegerType(), False),
            StructField("CustomerName", StringType(),  True),
            StructField("Email",        StringType(),  True),
            StructField("City",         StringType(),  True),
            StructField("Country",      StringType(),  True),
            StructField("Segment",      StringType(),  True)   # Consumer/Corporate/Home Office
        ])

        customers_data = [
            (1, "Alice Johnson",  "alice@email.com",   "New York",    "USA",    "Consumer"),
            (2, "Bob Smith",      "bob@email.com",     "Los Angeles", "USA",    "Corporate"),
            (3, "Charlie Brown",  "charlie@email.com", "Chicago",     "USA",    "Home Office"),
            (4, "Diana Prince",   "diana@email.com",   "Houston",     "USA",    "Consumer"),
            (5, "Eve Wilson",     "eve@email.com",     "Toronto",     "Canada", "Corporate"),
            (6, "Frank Miller",   "frank@email.com",   "Vancouver",   "Canada", "Consumer"),
            (7, "Grace Lee",      "grace@email.com",   "London",      "UK",     "Corporate"),
            (8, "Henry Ford",     "henry@email.com",   "Manchester",  "UK",     "Home Office"),
            (9, "Ivy Chen",       "ivy@email.com",     "Shanghai",    "China",  "Corporate"),
            (10,"Jack Wang",      "jack@email.com",    "Beijing",     "China",  "Consumer")
        ]

        customers_df = spark.createDataFrame(customers_data, customers_schema)

        # ─────────────────────────────────────────
        # 2. DIMENSION — Products
        # ─────────────────────────────────────────
        products_schema = StructType([
            StructField("ProductID",    IntegerType(), False),
            StructField("ProductName",  StringType(),  True),
            StructField("Category",     StringType(),  True),
            StructField("SubCategory",  StringType(),  True),
            StructField("UnitPrice",    FloatType(),   True),
            StructField("CostPrice",    FloatType(),   True)
        ])

        products_data = [
            (1,  "MacBook Pro",      "Technology",  "Laptops",      1500.0, 1100.0),
            (2,  "iPhone 15",        "Technology",  "Phones",        999.0,  700.0),
            (3,  "Office Chair",     "Furniture",   "Chairs",        299.0,  150.0),
            (4,  "Standing Desk",    "Furniture",   "Tables",        499.0,  250.0),
            (5,  "Notebook Pack",    "Stationery",  "Paper",          12.0,    5.0),
            (6,  "Pen Set",          "Stationery",  "Pens",            8.0,    3.0),
            (7,  "Monitor 27inch",   "Technology",  "Monitors",      399.0,  250.0),
            (8,  "Wireless Mouse",   "Technology",  "Accessories",    49.0,   20.0),
            (9,  "Bookshelf",        "Furniture",   "Storage",       199.0,   90.0),
            (10, "Printer Paper",    "Stationery",  "Paper",          25.0,   10.0),
            (11, "Keyboard",         "Technology",  "Accessories",    79.0,   35.0),
            (12, "Desk Lamp",        "Furniture",   "Lighting",       45.0,   18.0)
        ]

        products_df = spark.createDataFrame(products_data, products_schema)

        # ─────────────────────────────────────────
        # 3. DIMENSION — Shipping
        # ─────────────────────────────────────────
        shipping_schema = StructType([
            StructField("ShipModeID",   IntegerType(), False),
            StructField("ShipMode",     StringType(),  True),
            StructField("DeliveryDays", IntegerType(), True),
            StructField("ShipCost",     FloatType(),   True)
        ])

        shipping_data = [
            (1, "Same Day",     1,  25.0),
            (2, "First Class",  2,  15.0),
            (3, "Second Class", 4,  10.0),
            (4, "Standard",     7,   5.0)
        ]

        shipping_df = spark.createDataFrame(shipping_data, shipping_schema)

        # ─────────────────────────────────────────
        # 4. DIMENSION — Region
        # ─────────────────────────────────────────
        region_schema = StructType([
            StructField("RegionID",   IntegerType(), False),
            StructField("Region",     StringType(),  True),
            StructField("Country",    StringType(),  True),
            StructField("Zone",       StringType(),  True)
        ])

        region_data = [
            (1, "East",      "USA",    "North America"),
            (2, "West",      "USA",    "North America"),
            (3, "Central",   "USA",    "North America"),
            (4, "North",     "Canada", "North America"),
            (5, "West",      "Canada", "North America"),
            (6, "South",     "UK",     "Europe"),
            (7, "North",     "UK",     "Europe"),
            (8, "East",      "China",  "Asia Pacific"),
            (9, "North",     "China",  "Asia Pacific")
        ]

        region_df = spark.createDataFrame(region_data, region_schema)

        # ─────────────────────────────────────────
        # 5. DIMENSION — Date
        # ─────────────────────────────────────────
        date_schema = StructType([
            StructField("DateID",      IntegerType(), False),
            StructField("FullDate",    DateType(),    True),
            StructField("Year",        IntegerType(), True),
            StructField("Quarter",     IntegerType(), True),
            StructField("Month",       IntegerType(), True),
            StructField("MonthName",   StringType(),  True),
            StructField("Week",        IntegerType(), True),
            StructField("DayOfWeek",   StringType(),  True)
        ])

        date_data = [
            (1,  date(2023, 1, 15),  2023, 1, 1,  "January",   3,  "Sunday"),
            (2,  date(2023, 3, 22),  2023, 1, 3,  "March",    12,  "Wednesday"),
            (3,  date(2023, 5, 10),  2023, 2, 5,  "May",      19,  "Wednesday"),
            (4,  date(2023, 7,  4),  2023, 3, 7,  "July",     27,  "Tuesday"),
            (5,  date(2023, 9, 18),  2023, 3, 9,  "September",38,  "Monday"),
            (6,  date(2023, 11, 25), 2023, 4, 11, "November", 47,  "Saturday"),
            (7,  date(2024, 2, 14),  2024, 1, 2,  "February",  7,  "Wednesday"),
            (8,  date(2024, 4, 20),  2024, 2, 4,  "April",    16,  "Saturday"),
            (9,  date(2024, 6, 30),  2024, 2, 6,  "June",     26,  "Sunday"),
            (10, date(2024, 8, 12),  2024, 3, 8,  "August",   33,  "Monday"),
            (11, date(2024, 10, 5),  2024, 4, 10, "October",  40,  "Saturday"),
            (12, date(2024, 12, 20), 2024, 4, 12, "December", 51,  "Friday")
        ]

        date_df = spark.createDataFrame(date_data, date_schema)

        # ─────────────────────────────────────────
        # 6. FACT TABLE — Orders
        # ─────────────────────────────────────────
        orders_schema = StructType([
            StructField("OrderID",      StringType(),  False),
            StructField("CustomerID",   IntegerType(), True),   # FK → Customers
            StructField("ProductID",    IntegerType(), True),   # FK → Products
            StructField("ShipModeID",   IntegerType(), True),   # FK → Shipping
            StructField("RegionID",     IntegerType(), True),   # FK → Region
            StructField("DateID",       IntegerType(), True),   # FK → Date
            StructField("OrderDate",    DateType(),    True),
            StructField("ShipDate",     DateType(),    True),
            StructField("Quantity",     IntegerType(), True),
            StructField("UnitPrice",    FloatType(),   True),
            StructField("Discount",     FloatType(),   True),
            StructField("Sales",        FloatType(),   True),
            StructField("Profit",       FloatType(),   True),
            StructField("ShippingCost", FloatType(),   True),
            StructField("OrderStatus",  StringType(),  True)    # Pending/Shipped/Delivered/Returned
        ])

        orders_data = [
            # OrderID   CustID ProdID ShipID RegID DateID  OrderDate            ShipDate             Qty  UnitPr  Disc    Sales    Profit  ShipCost  Status
            ("ORD-001",  1,     1,     2,     1,    1,  date(2023,1,15),  date(2023,1,17),  2, 1500.0, 0.10, 2700.0,  700.0,  15.0, "Delivered"),
            ("ORD-002",  2,     3,     4,     2,    2,  date(2023,3,22),  date(2023,3,29),  4,  299.0, 0.00, 1196.0,  596.0,   5.0, "Delivered"),
            ("ORD-003",  3,     5,     3,     3,    3,  date(2023,5,10),  date(2023,5,14), 10,   12.0, 0.05,  114.0,   64.0,  10.0, "Delivered"),
            ("ORD-004",  4,     7,     1,     1,    4,  date(2023,7,4),   date(2023,7,5),   1,  399.0, 0.00,  399.0,  149.0,  25.0, "Delivered"),
            ("ORD-005",  5,     2,     2,     4,    5,  date(2023,9,18),  date(2023,9,20),  3,  999.0, 0.15, 2547.0, 1047.0,  15.0, "Delivered"),
            ("ORD-006",  6,     4,     3,     5,    6,  date(2023,11,25), date(2023,11,29), 2,  499.0, 0.00,  998.0,  498.0,  10.0, "Delivered"),
            ("ORD-007",  7,     8,     4,     6,    7,  date(2024,2,14),  date(2024,2,21),  5,   49.0, 0.10,  220.5,   85.5,   5.0, "Delivered"),
            ("ORD-008",  8,     11,    1,     7,    8,  date(2024,4,20),  date(2024,4,21),  2,   79.0, 0.00,  158.0,   88.0,  25.0, "Delivered"),
            ("ORD-009",  9,     1,     2,     8,    9,  date(2024,6,30),  date(2024,7,2),   1, 1500.0, 0.20, 1200.0,  400.0,  15.0, "Delivered"),
            ("ORD-010", 10,     9,     3,     9,   10,  date(2024,8,12),  date(2024,8,16),  3,  199.0, 0.00,  597.0,  327.0,  10.0, "Delivered"),
            ("ORD-011",  1,     6,     4,     1,   11,  date(2024,10,5),  date(2024,10,12), 8,    8.0, 0.05,   60.8,   36.8,   5.0, "Shipped"),
            ("ORD-012",  2,     12,    2,     2,   12,  date(2024,12,20), date(2024,12,22), 4,   45.0, 0.10,  162.0,   90.0,  15.0, "Pending"),
            ("ORD-013",  3,     2,     1,     3,    7,  date(2024,2,14),  date(2024,2,15),  1,  999.0, 0.00,  999.0,  299.0,  25.0, "Delivered"),
            ("ORD-014",  4,     10,    3,     1,    8,  date(2024,4,20),  date(2024,4,24),  6,   25.0, 0.00,  150.0,   90.0,  10.0, "Returned"),
            ("ORD-015",  5,     4,     2,     4,    9,  date(2024,6,30),  date(2024,7,2),   1,  499.0, 0.10,  449.1,  199.1,  15.0, "Delivered"),
            ("ORD-016",  6,     7,     4,     5,   10,  date(2024,8,12),  date(2024,8,19),  2,  399.0, 0.05,  758.1,  258.1,   5.0, "Delivered"),
            ("ORD-017",  7,     3,     1,     6,   11,  date(2024,10,5),  date(2024,10,6),  3,  299.0, 0.00,  897.0,  447.0,  25.0, "Shipped"),
            ("ORD-018",  8,     1,     3,     7,   12,  date(2024,12,20), date(2024,12,24), 1, 1500.0, 0.20, 1200.0,  400.0,  10.0, "Pending"),
            ("ORD-019",  9,     5,     2,     8,    5,  date(2023,9,18),  date(2023,9,20), 20,   12.0, 0.00,  240.0,  140.0,  15.0, "Delivered"),
            ("ORD-020", 10,     11,    4,     9,    6,  date(2023,11,25), date(2023,12,2),  4,   79.0, 0.10,  284.4,  144.4,   5.0, "Delivered")
        ]

        orders_df = spark.createDataFrame(orders_data, orders_schema)

        return customers_df, products_df, shipping_df, region_df, date_df, orders_df


# ─────────────────────────────────────────
# MAIN — Create Views & Print Tables
# ─────────────────────────────────────────
if __name__ == "__main__":

    obj = OrdersFactDataExample()
    customers_df, products_df, shipping_df, \
    region_df, date_df, orders_df = obj.PrepareData()

    # Create Temp Views
    customers_df.createOrReplaceTempView("Customers")
    products_df.createOrReplaceTempView("Products")
    shipping_df.createOrReplaceTempView("Shipping")
    region_df.createOrReplaceTempView("Region")
    date_df.createOrReplaceTempView("DateDim")
    orders_df.createOrReplaceTempView("Orders")

    # Print All Tables
    print("=== CUSTOMERS (Dimension) ===")
    customers_df.show(truncate=False)

    print("=== PRODUCTS (Dimension) ===")
    products_df.show(truncate=False)

    print("=== SHIPPING (Dimension) ===")
    shipping_df.show(truncate=False)

    print("=== REGION (Dimension) ===")
    region_df.show(truncate=False)

    print("=== DATE (Dimension) ===")
    date_df.show(truncate=False)

    print("=== ORDERS (Fact) ===")
    orders_df.show(truncate=False)

    # ── Quick Summary Queries ──────────────
    print("=== Total Sales by Category ===")
    spark.sql("""
        SELECT p.Category,
               COUNT(o.OrderID)        AS TotalOrders,
               SUM(o.Quantity)         AS TotalQty,
               ROUND(SUM(o.Sales), 2)  AS TotalSales,
               ROUND(SUM(o.Profit), 2) AS TotalProfit
        FROM Orders o
        JOIN Products p ON o.ProductID = p.ProductID
        GROUP BY p.Category
        ORDER BY TotalSales DESC
    """).show()

    print("=== Total Sales by Country ===")
    spark.sql("""
        SELECT c.Country,
               COUNT(o.OrderID)        AS TotalOrders,
               ROUND(SUM(o.Sales), 2)  AS TotalSales,
               ROUND(SUM(o.Profit), 2) AS TotalProfit
        FROM Orders o
        JOIN Customers c ON o.CustomerID = c.CustomerID
        GROUP BY c.Country
        ORDER BY TotalSales DESC
    """).show()

    print("=== Sales by Year & Quarter ===")
    spark.sql("""
        SELECT d.Year, d.Quarter,
               COUNT(o.OrderID)        AS TotalOrders,
               ROUND(SUM(o.Sales), 2)  AS TotalSales,
               ROUND(SUM(o.Profit), 2) AS TotalProfit
        FROM Orders o
        JOIN DateDim d ON o.DateID = d.DateID
        GROUP BY d.Year, d.Quarter
        ORDER BY d.Year, d.Quarter
    """).show()

    print("=== Order Status Summary ===")
    spark.sql("""
        SELECT OrderStatus,
               COUNT(OrderID)         AS TotalOrders,
               ROUND(SUM(Sales), 2)   AS TotalSales
        FROM Orders
        GROUP BY OrderStatus
        ORDER BY TotalOrders DESC
    """).show()

    print("✅ Orders Fact Table Created Successfully!")