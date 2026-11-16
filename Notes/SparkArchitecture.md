---
title: Learning Markdown

---

# Apache Spark Architecture – Complete Interview Preparation (5 YOE)

## 1. Introduction

Apache Spark is a distributed data processing engine used to process large volumes of structured, semi-structured, and unstructured data across multiple machines.

Spark follows a distributed execution architecture consisting of three primary components:

1. **Driver:** Coordinates the Spark application and manages execution.
2. **Cluster Manager:** Allocates compute resources to the Spark application.
3. **Executors:** Execute distributed tasks and process data partitions.

Spark uses a DAG-based execution model to distribute workloads across available executors.

When working with DataFrame and SQL APIs, Spark also uses the Catalyst Optimizer to analyze and optimize queries before execution.

### Why is Spark architecture important?

Understanding Spark architecture helps a Data Engineer:

- Understand how distributed processing works.
- Identify where computation is happening.
- Understand how data is distributed across executors.
- Identify expensive shuffle operations.
- Troubleshoot slow-running Spark jobs.
- Handle executor failures and resource constraints.
- Optimize cluster utilization and processing costs.
- Design scalable ETL pipelines.

---

## 2. High-Level Spark Architecture

```text
                  PySpark Application
                          |
                          v
                     SparkSession
                          |
                          v
                        Driver
                          |
             +------------+-------------+
             |                          |
             v                          v
       SparkContext              Query Execution
             |                  (DataFrame / SQL)
             |                          |
             |                  Catalyst Optimizer
             |                          |
             |                   Physical Plan
             |                          |
             +------------+-------------+
                          |
                          v
                    DAG Scheduler
                          |
                  Divide into Stages
                          |
                          v
                    Task Scheduler
                          |
                          v
                    Cluster Manager
                          |
             +------------+-------------+
             |            |             |
             v            v             v
          Worker 1     Worker 2      Worker 3
             |            |             |
          Executor     Executor      Executor
             |            |             |
          Tasks        Tasks         Tasks
             |            |             |
             +------------+-------------+
                          |
                          v
                 Distributed Processing
                          |
                          v
                  Shuffle if Required
                          |
                          v
                   Final Processing
                          |
                          v
                 ADLS / Delta / Other
                    Data Sources
```

The Driver coordinates execution, while Executors perform the distributed computation.

The Cluster Manager provides the resources required to run the application.

---

## 3. Core Components of Spark Architecture

### 3.1 Driver

The Driver is the central coordinator of a Spark application.

It is responsible for managing the application's execution lifecycle.

**Responsibilities:**

1. Initializes the Spark application.
2. Creates SparkSession and SparkContext.
3. Maintains application execution information.
4. Builds and coordinates execution plans.
5. Coordinates DAG and task scheduling.
6. Tracks job and stage execution.
7. Communicates with executors.
8. Handles task scheduling and execution results.

**Example:**

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RetailAnalytics")
    .getOrCreate()
)

df = spark.read.parquet("/data/transactions")

result = (
    df.filter("amount > 1000")
      .groupBy("customer_id")
      .sum("amount")
)

result.show()
```

Here, the Driver coordinates the complete execution of the PySpark application.

The Driver does not normally process all distributed records itself. It coordinates the work performed by Executors.

**Important:**

The Driver can become a bottleneck if too much data is brought back to it.

For example:

```python
df.collect()
```

Collecting a very large dataset transfers all resulting records to the Driver, potentially causing driver memory issues.

Instead, perform distributed processing on executors and bring back only small results when necessary.

**What happens if the Driver fails?**

The Spark application generally fails because the Driver manages application scheduling and execution coordination.

Executor recovery alone cannot replace a failed Driver.

---

### 3.2 SparkSession

SparkSession is the primary entry point for working with Spark SQL and DataFrame APIs.

It provides access to:

- DataFrame operations.
- SQL execution.
- Data source reading and writing.
- Catalog operations.
- Spark SQL configuration.
- SparkContext.

Example:

```python
spark = (
    SparkSession.builder
    .appName("CustomerPipeline")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
```

The SparkSession provides the higher-level interface, while SparkContext provides access to lower-level Spark functionality.

---

### 3.3 SparkContext

SparkContext is the lower-level coordination entry point for Spark Core.

It communicates with the cluster execution infrastructure and coordinates application resources and scheduling.

Responsibilities include:

- Establishing the Spark application context.
- Connecting with the cluster manager.
- Managing application execution.
- Coordinating scheduling components.
- Providing access to RDD functionality.

In modern PySpark applications, SparkContext is generally accessed through SparkSession:

```python
sc = spark.sparkContext

print(sc.applicationId)
print(sc.appName)
```

**Interview distinction:**

SparkSession is the primary entry point for DataFrame and SQL operations.

SparkContext provides access to Spark Core functionality and the underlying application context.

---

### 3.4 Cluster Manager

The Cluster Manager is responsible for allocating compute resources to Spark applications.

It manages resources such as:

- CPU cores.
- Executor memory.
- Worker resources.
- Executor allocation.

Common cluster managers include:

| Cluster Manager | Description |
|---|---|
| Standalone | Spark's built-in cluster manager |
| YARN | Hadoop resource management framework |
| Kubernetes | Container orchestration platform |
| Managed cloud compute | Cloud platforms manage underlying cluster infrastructure |

**Important distinction:**

The Cluster Manager allocates resources and launches application executors.

It does not decide how transformations are divided into Spark stages.

That responsibility belongs to the DAG Scheduler.

---

### 3.5 Worker Nodes

Worker Nodes are the machines or compute nodes that provide resources for executing Spark applications.

They host Executor processes.

A worker node may contain:

- CPU resources.
- Memory.
- Local disk.
- One or more Executor processes.

Example:

```text
Cluster
   |
   +-- Worker Node 1
   |      |
   |      +-- Executor 1
   |
   +-- Worker Node 2
   |      |
   |      +-- Executor 2
   |
   +-- Worker Node 3
          |
          +-- Executor 3
```

A Worker Node is an infrastructure concept, whereas an Executor is an application-specific process running on that infrastructure.

---

### 3.6 Executors

Executors are application-specific processes launched on worker nodes.

They perform the actual distributed computation.

**Responsibilities:**

1. Execute tasks assigned by the Driver.
2. Process data partitions.
3. Perform transformations.
4. Execute joins and aggregations.
5. Store cached and persisted data.
6. Write intermediate shuffle data.
7. Communicate execution status to the Driver.

Example:

Suppose we have:

- 4 Executors.
- 4 CPU cores per Executor.
- 100 input partitions.

Total available task slots:

```text
4 Executors × 4 Cores = 16 Concurrent Tasks
```

Therefore, approximately 16 tasks can execute concurrently, assuming all cores are available for Spark tasks.

The remaining tasks wait for available execution slots.

**Important distinction:**

An Executor is a long-running process that can execute multiple tasks during its lifetime.

A Task is an individual unit of work that processes a partition within a stage.

**What happens if an Executor fails?**

Spark can retry failed tasks on available executors.

If cached partitions are lost, Spark may recompute them using lineage.

If sufficient resources cannot be recovered, the application can fail.

---

## 4. Internal Scheduling Components

These components are particularly important for experienced Data Engineer interviews.

### 4.1 DAG Scheduler

DAG stands for Directed Acyclic Graph.

The DAG Scheduler converts job dependencies into stages.

It identifies shuffle dependencies and divides the execution graph into appropriate stages.

Example:

```python
df = spark.read.parquet("/data/transactions")

result = (
    df.filter("amount > 1000")
      .groupBy("customer_id")
      .sum("amount")
)

result.show()
```

Conceptual execution:

```text
Read Data
    |
Filter
    |
Partial Aggregation
    |
Shuffle
    |
Final Aggregation
    |
Display Results
```

The DAG Scheduler identifies dependencies and organizes execution into stages.

**Important:**

Narrow transformations can generally be pipelined within a stage.

Wide transformations introduce shuffle dependencies and typically create stage boundaries.

### 4.2 Task Scheduler

The Task Scheduler receives task sets from the DAG Scheduler and coordinates task execution.

Responsibilities include:

- Scheduling tasks.
- Assigning tasks to available executor resources.
- Monitoring task status.
- Handling task failures and retries.
- Coordinating task execution through the scheduler backend.

**Difference between DAG Scheduler and Task Scheduler:**

| DAG Scheduler | Task Scheduler |
|---|---|
| Creates stages | Schedules tasks |
| Understands dependencies | Manages task execution |
| Identifies shuffle boundaries | Assigns work to executors |
| Coordinates stage progression | Tracks task status |

### 4.3 Block Manager

Block Manager manages data blocks used by Spark.

It handles:

- Cached partitions.
- Persisted RDD/DataFrame blocks.
- Intermediate shuffle blocks.
- Block storage and retrieval.
- Communication of block locations.

It plays an important role in caching and shuffle operations.

---

## 5. Complete Spark Application Execution Flow

This is the most important section to understand for interviews.

Consider a retail analytics pipeline.

Business requirement:

Read transaction data, filter invalid records, aggregate customer-wise sales, and write the result to Delta Lake.

### Step 1: Application submission

The Data Engineer submits a PySpark application.

The application initializes SparkSession.

```python
spark = (
    SparkSession.builder
    .appName("RetailAnalytics")
    .getOrCreate()
)
```

### Step 2: Driver initialization

The Driver initializes the Spark application and establishes the application context.

### Step 3: Resource allocation

The Driver communicates with the Cluster Manager to obtain the required compute resources.

The Cluster Manager allocates resources and launches Executors.

### Step 4: Define transformations

The Data Engineer writes transformations.

```python
df = spark.read.parquet("/data/transactions")

filtered_df = df.filter("amount > 1000")

result = (
    filtered_df
    .groupBy("customer_id")
    .sum("amount")
)
```

These transformations are generally lazy.

Spark builds the execution plan instead of immediately executing every operation.

### Step 5: Query optimization

For DataFrame and SQL operations, Spark performs:

1. Analysis.
2. Logical optimization.
3. Physical planning.
4. Physical execution preparation.

Catalyst optimizes the logical query plan.

The physical planner selects suitable execution strategies.

### Step 6: Action triggers execution

```python
result.show()
```

The action triggers execution.

Spark creates the required job and the scheduling process begins.

### Step 7: DAG creation and stage division

The DAG Scheduler identifies dependencies and divides the job into stages.

Shuffle dependencies typically create stage boundaries.

### Step 8: Task creation

Each stage is divided into tasks.

Generally, one task processes one partition for that stage.

### Step 9: Task scheduling

The Task Scheduler schedules tasks on available executors.

### Step 10: Distributed processing

Executors process the assigned partitions in parallel.

If a shuffle is required, intermediate data is redistributed between partitions.

### Step 11: Final output

The final results are returned to the Driver for operations such as show or collect, or written to external storage through distributed execution.

---

## 6. Spark Architecture – PySpark Internals

An important additional point for PySpark interviews is understanding the Python and JVM execution boundary.

Apache Spark's core execution engine is primarily implemented in Scala and runs on the JVM.

PySpark provides Python APIs that communicate with the Spark engine.

A simplified architecture is:

```text
             Python Application
                    |
                    v
               PySpark API
                    |
                    v
             Py4J Gateway
                    |
                    v
                JVM Driver
                    |
                    v
              Spark Engine
                    |
                    v
                Executors
                    |
          +---------+---------+
          |                   |
          v                   v
     JVM Execution       Python Workers
          |                   |
          +---------+---------+
                    |
                    v
              Final Results
```

### What is Py4J?

Py4J enables communication between Python and JVM processes.

When we write PySpark DataFrame operations, PySpark communicates with the JVM-side Spark SQL engine through this interface.

### What are Python Workers?

Python Workers are separate Python processes used for executing Python-side functions such as Python UDFs.

For standard built-in DataFrame operations, Spark can execute optimized JVM-side operators without sending every record through a Python UDF.

This is one reason built-in Spark SQL functions are generally preferred over regular Python UDFs.

**Interview question: Why are Python UDFs sometimes slower?**

Regular Python UDFs can introduce serialization and communication overhead between JVM and Python processes.

Whenever possible, I prefer built-in PySpark SQL functions because they allow Spark to optimize the operations more effectively.

---

## 7. Driver vs Executor vs Cluster Manager

| Feature | Driver | Executor | Cluster Manager |
|---|---|---|---|
| Main responsibility | Application coordination | Task execution | Resource allocation |
| Runs application logic | Yes | Executes assigned operations | No |
| Builds execution plans | Yes | No | No |
| Schedules stages | DAG Scheduler within Driver | No | No |
| Executes tasks | No, generally | Yes | No |
| Stores cached data | Not its primary role | Yes | No |
| Manages cluster resources | Requests resources | Uses allocated resources | Yes |
| Communicates with other components | Yes | Yes | Yes |

---

## 8. Deploy Modes – Client vs Cluster

Deploy mode determines where the Driver process runs.

### Client Mode

The Driver runs on the machine submitting the Spark application.

```text
Submission Machine
       |
     Driver
       |
 Cluster Manager
       |
    Executors
```

Suitable for interactive development and certain remote submission workflows.

### Cluster Mode

The Driver runs inside the cluster environment.

```text
Submission Machine
       |
 Cluster Manager
       |
     Driver
       |
    Executors
```

Commonly used for production batch workloads.

| Feature | Client Mode | Cluster Mode |
|---|---|---|
| Driver location | Submission machine | Cluster |
| Interactive development | Convenient | Less convenient |
| Production batch workloads | Supported | Commonly preferred |
| Submitting terminal availability | Driver must remain available | Can disconnect after submission |

---

## 9. Fault Tolerance in Spark Architecture

Distributed systems must account for failures.

Spark supports fault recovery through several mechanisms.

### 9.1 Task Retry

If a task fails, Spark can retry it on an available executor.

### 9.2 Executor Recovery

When an executor is lost, Spark can continue if sufficient resources remain or replacement resources become available.

### 9.3 RDD Lineage

Spark tracks dependencies between distributed datasets.

If a partition is lost, Spark can recompute it using the lineage information.

### 9.4 Checkpointing

Checkpointing saves intermediate data to reliable storage and truncates lineage.

It is particularly useful for long-running iterative or streaming computations.

### 9.5 Important distinction

Spark task retry and lineage do not automatically guarantee exactly-once behavior for every external write operation.

For production pipelines, we must also consider:

- Idempotent writes.
- Transactional table formats.
- Checkpoint management.
- Source and sink guarantees.
- Recovery after partial failures.

---

## 10. Real-Time Azure Databricks Architecture

Consider a retail company processing millions of daily transactions from multiple stores.

Business requirements:

- Ingest source files.
- Validate transaction records.
- Perform transformations.
- Calculate customer-wise sales.
- Store results in Delta Lake.

### Architecture

```text
             Source Systems
                    |
                    v
              ADLS Gen2
                    |
                    v
            Azure Databricks
                    |
              Spark Driver
                    |
          +---------+---------+
          |                   |
          v                   v
     DAG Scheduler       Catalyst Optimizer
          |                   |
          +---------+---------+
                    |
                    v
             Task Scheduler
                    |
                    v
              Executors
                    |
             Distributed ETL
                    |
                    v
               Shuffle
                    |
                    v
             Final Aggregation
                    |
                    v
               Delta Lake
                    |
             +------+------+
             |             |
             v             v
          Silver          Gold
                           |
                           v
                    Power BI / SQL
```

### PySpark implementation

```python
from pyspark.sql import functions as F

# Read Bronze data
transactions_df = (
    spark.read
    .format("delta")
    .load("/mnt/bronze/transactions")
)

# Data quality filtering
valid_transactions = (
    transactions_df
    .filter(
        (F.col("amount") > 0) &
        F.col("customer_id").isNotNull()
    )
)

# Customer-level aggregation
customer_sales = (
    valid_transactions
    .groupBy("customer_id")
    .agg(
        F.sum("amount").alias("total_sales"),
        F.count("*").alias("transaction_count")
    )
)

# Write Gold output
(
    customer_sales.write
    .format("delta")
    .mode("overwrite")
    .save("/mnt/gold/customer_sales")
)
```

### How I would explain this implementation

"In a typical Azure Databricks ETL pipeline, I read the source data from ADLS or the Bronze Delta layer and perform the required data quality checks and transformations.

Spark initially builds the execution plan. Since I use DataFrame APIs, Catalyst optimizes the logical plan and the physical planner selects execution operators.

When the write action is triggered, Spark executes the required computation. The DAG Scheduler identifies stage boundaries based on shuffle dependencies, and the Task Scheduler distributes tasks across the available executors.

For customer-level aggregation, Spark performs partial aggregation and redistributes intermediate records based on customer keys. The final aggregation is performed in downstream tasks.

After processing, the output is written into the Gold Delta layer.

For production monitoring, I inspect Spark UI to identify expensive stages, shuffle volume, skewed partitions, failed tasks and executor utilization."

---

## 11. Production Troubleshooting – What an Experienced Engineer Should Know

Understanding architecture is not sufficient. A 5 YOE Data Engineer should also know how to troubleshoot problems.

| Production problem | Possible cause | Investigation / Solution |
|---|---|---|
| Driver Out of Memory | Large collect operation | Avoid collecting large datasets |
| Executor Out of Memory | Memory pressure or large partitions | Review executor memory, partition sizes and execution plan |
| One task takes much longer | Data skew | Inspect task distribution and skewed keys |
| Excessive shuffle | Wide transformations or inefficient joins | Reduce unnecessary redistribution |
| Slow job | Poor partitioning or inefficient execution | Inspect Spark UI and physical plan |
| Too many small files | Excessive output partitions | Optimize output file sizing |
| High GC time | JVM memory pressure | Inspect GC metrics and executor memory |
| Repeated computation | Missing persistence where reuse justifies it | Consider cache/persist |
| Executor lost | Resource pressure or infrastructure failure | Inspect executor logs and cluster events |

### Example: Identifying data skew

Suppose a customer aggregation processes 100 million records.

Most partitions complete in 2 minutes, but one task takes 20 minutes.

This could indicate data skew.

I would:

1. Open Spark UI.
2. Inspect the relevant stage.
3. Compare task execution durations.
4. Compare shuffle read sizes.
5. Identify skewed keys.
6. Consider AQE skew handling or salting where appropriate.
7. Re-run and compare execution metrics.

This demonstrates an understanding of production-level Spark troubleshooting.

---

## 12. Common Interview Questions

### Q1. Explain Apache Spark architecture.

Spark follows a distributed execution architecture with a Driver, Cluster Manager and Executors.

The Driver coordinates the application, the Cluster Manager allocates resources, and Executors execute distributed tasks.

Spark uses DAG scheduling to divide jobs into stages and task scheduling to distribute tasks across executors.

### Q2. What is the difference between Driver and Executor?

Driver coordinates the application and scheduling process, whereas Executors perform distributed computation and maintain cached data.

### Q3. Does the Driver process the entire dataset?

No. Distributed transformations are executed by Executors. The Driver coordinates execution and may receive results for operations such as collect or show.

### Q4. What happens if an Executor fails?

Spark can retry failed tasks on other available executors. Lost cached partitions may be recomputed using lineage.

### Q5. What happens if the Driver fails?

The application generally fails because the Driver coordinates execution and scheduling. Recovering the application requires application-level restart or platform-specific recovery mechanisms.

### Q6. What is the difference between DAG Scheduler and Task Scheduler?

The DAG Scheduler divides jobs into stages based on dependencies. The Task Scheduler manages the scheduling of individual tasks on executors.

### Q7. Does Cluster Manager create stages?

No. The DAG Scheduler creates stages. The Cluster Manager is responsible for allocating cluster resources and launching executors.

### Q8. Can multiple Spark applications share the same executors?

In the standard Spark application model, each application has its own executors. Executors are not normally shared across independent Spark applications.

### Q9. How many tasks can an executor execute concurrently?

It depends primarily on the number of executor cores available for task execution and the task resource requirements.

For example, an executor with four available cores can generally execute four standard CPU tasks concurrently.

### Q10. Why is SparkSession important?

SparkSession is the primary entry point for DataFrame and SQL operations. It provides a unified interface for Spark SQL functionality.

### Q11. What is the role of Catalyst Optimizer?

Catalyst analyzes and optimizes DataFrame and SQL query plans before physical execution.

### Q12. What happens when you execute groupBy?

Spark generally performs partial aggregation, redistributes intermediate records through shuffle, and performs final aggregation in downstream processing.

### Q13. How do you monitor a Spark application?

I use Spark UI to inspect jobs, stages, SQL execution plans, shuffle metrics, task durations, executor metrics and cached data.

### Q14. What is the difference between a Worker Node and Executor?

Worker Node is the infrastructure machine hosting compute resources. Executor is an application-specific process running on that infrastructure.

### Q15. What is the role of Block Manager?

Block Manager manages data blocks, including cached partitions, persisted data and shuffle-related blocks.

---

## 13. Final Interview Answer – 90 to 120 Seconds

If the interviewer asks, "Explain Spark architecture", this is the answer I would prepare and deliver:

"Apache Spark follows a distributed execution architecture consisting of three primary components: Driver, Cluster Manager and Executors.

The Driver is the central coordinator of the Spark application. It initializes SparkSession and SparkContext, coordinates execution planning and manages scheduling.

The Cluster Manager is responsible for allocating resources such as CPU and memory and launching the required executors.

Executors are application-specific processes running on worker nodes. They perform the actual distributed computation, process data partitions, execute transformations and maintain cached data.

From an execution perspective, when we write PySpark DataFrame operations, Spark builds a logical execution plan. Catalyst analyzes and optimizes that plan, and the physical planner selects suitable execution strategies.

When an action such as write, count or show triggers execution, Spark creates the required job. The DAG Scheduler divides it into stages based on shuffle dependencies. The Task Scheduler then schedules individual partition-level tasks on available executors.

For example, when performing a customer-level groupBy aggregation, Spark can perform partial aggregation locally, shuffle intermediate records based on customer keys, and perform the final aggregation in downstream stages.

In production, I pay particular attention to shuffle volume, data skew, task execution time, executor utilization and memory pressure. I use Spark UI to identify bottlenecks and optimize the execution plan.

In an Azure Databricks environment, this architecture helps us process large datasets efficiently and build scalable ETL pipelines across Bronze, Silver and Gold layers."

---

## 14. Key Takeaways

| Component | What to remember |
|---|---|
| Driver | Coordinates application execution |
| SparkSession | Entry point for DataFrame and SQL |
| SparkContext | Lower-level Spark application context |
| Cluster Manager | Allocates compute resources |
| Worker Node | Hosts executor processes |
| Executor | Executes distributed tasks |
| DAG Scheduler | Divides jobs into stages |
| Task Scheduler | Schedules individual tasks |
| Block Manager | Manages data blocks and caching |
| Catalyst | Optimizes SQL and DataFrame plans |
| Shuffle | Redistributes data across partitions |
| Spark UI | Monitors and troubleshoots execution |

### What makes a 5 YOE answer different?

A junior candidate explains what Driver, Executor and Cluster Manager mean.

An experienced candidate explains:

1. How these components interact.
2. What happens after submitting an application.
3. How a job becomes stages and tasks.
4. Why shuffle introduces stage boundaries.
5. How executors process partitions in parallel.
6. How Spark handles failures.
7. How to diagnose performance issues using Spark UI.
8. How these concepts apply to production ETL pipelines.

**The goal is to demonstrate that you understand Spark as a distributed processing engine, not just as a Python API.**

---

## References

- [Apache Spark – Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Apache Spark – Web UI](https://spark.apache.org/docs/latest/web-ui.html)
- [Apache Spark – Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [DataCamp – Apache Spark Architecture](https://www.datacamp.com/blog/apache-spark-architecture)

