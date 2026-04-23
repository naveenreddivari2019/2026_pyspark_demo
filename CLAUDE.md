# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Purpose

This is a learning and demonstration repository containing practical examples of Python, PySpark, SQL, and concurrent programming patterns. Each example is a standalone script demonstrating specific concepts or techniques.

## Project Structure

- `src/pyspark_examples/` - PySpark demonstrations including joins, streaming, SCD patterns, caching, salting, and accumulator/broadcast variables
- `src/python_examples/` - Core Python examples covering decorators, classes, generators, collections, exception handling, and more
- `src/python_Threading/` - Concurrency examples demonstrating threading, multiprocessing, asyncio, GIL behavior, locks, and race conditions
- `src/SQL_Examples/` - SQL query examples using PySpark SQL, including joins, window functions, subqueries, and aggregations

## Running Examples

All examples are standalone Python scripts that can be executed directly:

```bash
# Run a PySpark example
python src/pyspark_examples/SparkJoins.py

# Run a Python threading example
python src/python_Threading/01_threading.py

# Run a SQL example
python src/SQL_Examples/SQL_EmpDeptMgr.py
```

**Note**: PySpark examples require a Spark installation or will run in local mode. Some examples (like Streaming_exmpl.py) require additional infrastructure (Kafka, Delta Lake) and are meant as reference implementations.

## Dependencies

Install required packages:
```bash
pip install -r requirements.txt
```

Current dependencies:
- pandas==2.2.3
- pyspark==3.5.4

## Architecture Patterns

### PySpark Examples
- Use `SparkSession.builder` pattern to create Spark contexts
- Schema definitions use either DDL strings (`"id INT, name STRING"`) or `StructType` objects
- Examples demonstrate both DataFrame API and SQL API approaches

### SQL Examples Pattern
- Reusable data generation through `SQLDataExample` class in `SQL_data.py`
- Creates standardized Employee, Department, and Manager DataFrames
- Use `createOrReplaceTempView()` to enable SQL queries on DataFrames
- Import and instantiate: `from SQL_data import SQLDataExample; obj = SQLDataExample(); emp_df, dept_df, mgr_df = obj.PrepareData()`

### Key PySpark Concepts Demonstrated
- **Joins**: All join types (inner, left, right, outer, semi, anti, cross) in `SparkJoins.py`
- **SCD Type 1 & 2**: Delta Lake merge operations for slowly changing dimensions in `SCD1&2.py`
- **Streaming**: Kafka integration with windowing and watermarks in `Streaming_exmpl.py`
- **Optimization**: Caching/persistence strategies, data skew handling via salting
- **Distributed Variables**: Accumulators for aggregations, broadcast variables for small datasets

### Threading/Concurrency Patterns
- Files prefixed with numbers (01-12) for progressive learning
- Demonstrates threading vs multiprocessing vs asyncio trade-offs
- Covers GIL impact, race conditions, deadlocks, locks, queues, daemon threads

## Code Conventions

- Example scripts are self-contained with inline data generation (no external files required)
- PySpark scripts print schemas and show DataFrames for demonstration
- SQL examples include commented descriptions of what each query demonstrates
- Threading examples use simple domains (chai shop, orders) to illustrate concurrency concepts
