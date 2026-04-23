"""
Monitoring Spark Stages and Tasks

Demonstrates various ways to check and monitor stages and tasks in Spark jobs:
1. Spark UI - Web interface for monitoring
2. StatusTracker API - Programmatic access to job/stage/task information
3. SparkListener - Custom event listeners
4. Understanding stages and tasks breakdown
5. Logging and metrics
"""

import os
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_func, count, avg, max as max_func
import time
import tempfile

# Create temp directory for event logs
event_log_dir = tempfile.mkdtemp(prefix="spark_events_")

# Create Spark session with custom configuration for monitoring
spark = SparkSession.builder \
    .appName("Spark Stages and Tasks Monitoring") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", event_log_dir) \
    .config("spark.sql.adaptive.enabled", "false") \
    .master("local[4]") \
    .getOrCreate()

sc = spark.sparkContext

print("=" * 80)
print("MONITORING SPARK STAGES AND TASKS")
print("=" * 80)

# ============================================================================
# 1. Spark UI Information
# ============================================================================
print("\n1. SPARK UI (WEB INTERFACE)")
print("-" * 80)
print(f"Spark UI is available at: http://localhost:4040")
print(f"Application ID: {sc.applicationId}")
print(f"Application Name: {sc.appName}")
print(f"Spark Version: {sc.version}")
print(f"Master: {sc.master}")
print(f"Default Parallelism: {sc.defaultParallelism}")
print("\nNote: Keep the Spark session alive to access the UI")
print("      The UI shows: Jobs, Stages, Tasks, Storage, Environment, Executors")

# ============================================================================
# 2. StatusTracker API - Get Job/Stage/Task Information
# ============================================================================
print("\n2. STATUS TRACKER API - Programmatic Monitoring")
print("-" * 80)

# Get status tracker
tracker = sc.statusTracker()

print(f"\nActive job IDs: {tracker.getJobIdsForGroup(None)}")
print(f"Active stage IDs: {tracker.getActiveStageIds()}")

# Create sample data and trigger some actions
print("\n\nCreating sample data and executing operations...")

# Sample data - large enough to see multiple tasks
data = [(i, f"Name_{i % 100}", i * 100, i % 10)
        for i in range(1, 10001)]

df = spark.createDataFrame(data, ["id", "name", "value", "category"])
df.cache()

print("\nDataFrame created. Schema:")
df.printSchema()

# ============================================================================
# 3. Monitor Job Execution with StatusTracker
# ============================================================================
print("\n3. MONITORING JOB EXECUTION")
print("-" * 80)

def monitor_job(action_name, action_func):
    """Execute an action and monitor its jobs, stages, and tasks"""
    print(f"\n{'='*70}")
    print(f"Executing: {action_name}")
    print(f"{'='*70}")

    # Get initial state
    initial_jobs = set(tracker.getJobIdsForGroup(None))

    # Execute the action
    start_time = time.time()
    result = action_func()
    end_time = time.time()

    # Get new jobs
    final_jobs = set(tracker.getJobIdsForGroup(None))
    new_jobs = final_jobs - initial_jobs

    print(f"\nExecution Time: {end_time - start_time:.3f} seconds")
    print(f"Result: {result}")

    # Give Spark a moment to update status
    time.sleep(0.5)

    # Get all job IDs (including completed)
    for job_id in range(max(initial_jobs | final_jobs | {0})):
        job_info = tracker.getJobInfo(job_id)
        if job_info:
            print(f"\nJob ID: {job_id}")
            print(f"  Status: {job_info.status}")
            print(f"  Stage IDs: {job_info.stageIds}")

            # Get stage information
            for stage_id in job_info.stageIds:
                stage_info = tracker.getStageInfo(stage_id)
                if stage_info:
                    print(f"\n  Stage ID: {stage_id}")
                    print(f"    Stage Name: {stage_info.name}")
                    print(f"    Status: {stage_info.currentAttemptId}")
                    print(f"    Number of Tasks: {stage_info.numTasks}")
                    print(f"    Active Tasks: {stage_info.numActiveTasks}")
                    print(f"    Completed Tasks: {stage_info.numCompletedTasks}")
                    print(f"    Failed Tasks: {stage_info.numFailedTasks}")

# Monitor different operations
monitor_job("1. Count operation", lambda: df.count())

monitor_job("2. Aggregation with groupBy",
            lambda: df.groupBy("category").agg(
                count("*").alias("count"),
                avg("value").alias("avg_value")
            ).collect())

monitor_job("3. Filter and sum operation",
            lambda: df.filter(col("value") > 5000).agg(
                sum_func("value").alias("total")
            ).collect())

# ============================================================================
# 4. Understanding Stages Breakdown
# ============================================================================
print("\n\n4. UNDERSTANDING STAGES BREAKDOWN")
print("-" * 80)
print("""
Spark Job Execution Hierarchy:
- JOB: Triggered by an action (count, collect, save, etc.)
- STAGE: Created at shuffle boundaries or result operations
  * Wide transformations (groupBy, join) create new stages
  * Narrow transformations (filter, map) stay in same stage
- TASK: One task per partition within a stage

Example: df.groupBy("category").count()
  Stage 0: Read data and partial aggregation (tasks = # partitions)
  Stage 1: Shuffle and final aggregation (tasks = # shuffle partitions)
""")

# Demonstrate stage creation with explicit shuffle
print("\nDemonstrating stage creation with join operation:")
df1 = spark.createDataFrame([(i, f"A_{i}") for i in range(100)], ["id", "col_a"])
df2 = spark.createDataFrame([(i, f"B_{i}") for i in range(100)], ["id", "col_b"])

print("\nJoin operation (creates multiple stages due to shuffle):")
monitor_job("Join two DataFrames",
            lambda: df1.join(df2, "id").count())

# ============================================================================
# 5. Partition and Task Relationship
# ============================================================================
print("\n\n5. PARTITION AND TASK RELATIONSHIP")
print("-" * 80)

# Check partitions
print(f"Number of partitions in df: {df.rdd.getNumPartitions()}")
print("Each partition = One task in a stage")

# Repartition and see task count change
df_repart = df.repartition(8)
print(f"\nAfter repartition(8): {df_repart.rdd.getNumPartitions()} partitions")

monitor_job("Count with 8 partitions", lambda: df_repart.count())

# Coalesce (no shuffle)
df_coalesce = df.coalesce(2)
print(f"\nAfter coalesce(2): {df_coalesce.rdd.getNumPartitions()} partitions")

monitor_job("Count with 2 partitions (coalesce)", lambda: df_coalesce.count())

# ============================================================================
# 6. Explain Plan - See Physical Execution
# ============================================================================
print("\n\n6. EXPLAIN PLAN - Understanding Execution Strategy")
print("-" * 80)

print("\nSimple query explain:")
simple_query = df.filter(col("value") > 5000).select("id", "name", "value")
simple_query.explain(mode="simple")

print("\n\nComplex query with aggregation explain:")
complex_query = df.groupBy("category").agg(
    count("*").alias("count"),
    sum_func("value").alias("total_value"),
    avg("value").alias("avg_value")
)
complex_query.explain(mode="formatted")

# ============================================================================
# 7. RDD Operations and Stage Visualization
# ============================================================================
print("\n\n7. RDD OPERATIONS - Stage Dependencies")
print("-" * 80)

# Create RDD with transformations
rdd = sc.parallelize(range(1, 1001), 4)
print(f"Initial RDD partitions: {rdd.getNumPartitions()}")

# Narrow transformation (no shuffle, same stage)
rdd_mapped = rdd.map(lambda x: (x % 10, x))
print(f"After map: {rdd_mapped.getNumPartitions()} partitions")

# Wide transformation (shuffle, new stage)
rdd_reduced = rdd_mapped.reduceByKey(lambda a, b: a + b)
print(f"After reduceByKey: {rdd_reduced.getNumPartitions()} partitions")

print("\n\nRDD Debug String (shows stage lineage):")
print(rdd_reduced.toDebugString().decode('utf-8'))

monitor_job("RDD collect operation", lambda: rdd_reduced.collect())

# ============================================================================
# 8. Task Metrics and Statistics
# ============================================================================
print("\n\n8. TASK METRICS COLLECTION")
print("-" * 80)

from pyspark import TaskContext

def process_with_metrics(partition):
    """Function that shows task context information"""
    ctx = TaskContext.get()
    if ctx:
        print(f"Task ID: {ctx.taskAttemptId()}, "
              f"Partition: {ctx.partitionId()}, "
              f"Attempt: {ctx.attemptNumber()}")
    return [sum(partition)]

rdd_with_context = sc.parallelize(range(1, 101), 4)
print("\nProcessing with task context (check console for task info):")
results = rdd_with_context.mapPartitions(process_with_metrics).collect()
print(f"Results from each partition: {results}")

# ============================================================================
# 9. SparkListener Example (for advanced monitoring)
# ============================================================================
print("\n\n9. SPARK LISTENER (Advanced Monitoring)")
print("-" * 80)
print("""
SparkListener allows you to track events like:
- Job start/end
- Stage start/end/complete
- Task start/end
- Executor added/removed

Example listener code (Java/Scala typically):

class CustomSparkListener extends SparkListener {
  override def onJobStart(jobStart: SparkListenerJobStart) {
    println(s"Job ${jobStart.jobId} started with ${jobStart.stageIds.length} stages")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    val info = stageCompleted.stageInfo
    println(s"Stage ${info.stageId} completed: ${info.numTasks} tasks")
  }
}

// Add listener
sc.addSparkListener(new CustomSparkListener())
""")

# ============================================================================
# 10. Summary and Best Practices
# ============================================================================
print("\n\n10. MONITORING BEST PRACTICES")
print("-" * 80)
print("""
Ways to Monitor Spark Jobs:

1. SPARK UI (http://localhost:4040)
   - Real-time monitoring of jobs, stages, tasks
   - DAG visualization
   - Storage and executor metrics
   - SQL query execution plans

2. StatusTracker API
   - Programmatic access to job/stage/task status
   - Useful for automated monitoring

3. Event Logs
   - Enable with: spark.eventLog.enabled=true
   - Replay history after job completion
   - Access via Spark History Server

4. Metrics and Logging
   - Configure log4j for detailed logging
   - Use metrics system for custom metrics

5. Explain Plans
   - Use df.explain() to understand physical plan
   - Identify shuffle operations and stage boundaries

Key Metrics to Monitor:
- Number of stages (fewer is better)
- Number of shuffles (minimize these)
- Task duration distribution (look for stragglers)
- Data skew (uneven task durations)
- Memory usage and spills
- GC time
""")

# ============================================================================
# 11. Current Job Statistics Summary
# ============================================================================
print("\n\n11. CURRENT SESSION STATISTICS")
print("-" * 80)

print(f"Spark UI URL: http://localhost:4040")
print(f"Application ID: {sc.applicationId}")
print(f"Total number of active stages: {len(tracker.getActiveStageIds())}")

# Get all pool names
print(f"\nScheduler pools: {sc.getLocalProperty('spark.scheduler.pool')}")

print(f"\n{'='*80}")
print("Keep the Spark session running and visit the Spark UI for detailed insights!")
print(f"{'='*80}")

# Keep application alive for UI inspection
print("\nWaiting 10 seconds so you can inspect Spark UI...")
print("Press Ctrl+C to stop early, or wait for auto-shutdown")
print("(In production, you can increase this time to inspect the UI)")

try:
    time.sleep(10)
except KeyboardInterrupt:
    print("\n\nInterrupted by user")

spark.stop()

# Cleanup
import shutil
shutil.rmtree(event_log_dir, ignore_errors=True)

print("\nSpark session stopped. Application completed.")
