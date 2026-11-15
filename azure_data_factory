 Pipeline Execution & Dataset Configuration
Q1: What happens if an Azure Data Factory pipeline that writes an output file to storage is executed multiple times? Does it fail or overwrite?
Answer: Re-running the pipeline will overwrite the existing target file in storage without throwing an error
.
Q2: Why might ADF export a file with a .txt extension by default instead of .csv, and how do you resolve it?
Answer: If no file extension or name is explicitly defined in the dataset settings, ADF defaults to exporting the schema and table name with a .txt extension (e.g., dbo.orders.txt)
. To ensure it exports properly as a CSV file, you must explicitly set the file name and extension (e.g., orders.csv) inside the dataset configuration
.
2. Ingesting Multiple Files with Wildcard Paths
Q3: How do you dynamically load multiple CSV files from a folder in Azure Data Lake into Azure SQL Database?
Answer: In the DelimitedText source dataset, specify the container and folder path (e.g., yearwise, keeping in mind that folder paths are case-sensitive) without specifying individual file names
. In the Copy Activity Source settings, select Wildcard file path and set the pattern to *.csv
. This ensures all matching CSV files (such as 2021.csv, 2022.csv, 2023.csv) are picked up dynamically, while ignoring non-CSV file types that could cause copy failures
.
3. Metadata Tracking with "Additional Columns"
Q4: What is the "Additional Columns" feature in ADF Copy Activity, where is it configured, and what is a primary use case?
Answer: The Additional Columns feature allows you to attach extra metadata, parameters, or static values to the source data stream during a copy operation before it gets written to the sink table
. It is configured under the Source tab properties of the Copy Activity
. A key use case is audit tracking—adding a new column (such as FileName) using ADF system variables like $$FILENAME so each database record identifies the exact file it came from
.
4. Azure SQL Database Sink Configuration
Q5: How can ADF load data into an Azure SQL Database table if the target table does not already exist?
Answer: In the Sink configuration of the Copy Activity pointing to Azure SQL Database, you can enable the Auto create table setting
. This instructs ADF to automatically create the target database table (e.g., dbo.tbl_orders) before inserting the incoming rows
.
1. Azure Data Factory Variables & Set Variable Activity
Q1: Where are variables created in Azure Data Factory, and can you create one directly inside a Set Variable activity?
Answer: Variables can only be created at the Pipeline level in ADF
. The Set Variable activity does not create variables; its sole purpose is to assign or update the value of a variable that was already defined at the pipeline level
.
Q2: What data types are supported for variables in Azure Data Factory?
Answer: Unlike SQL databases which support extensive data types, ADF variables support a limited set of four data types: String, Integer, Boolean, and Array
.
Q3: What happens when a Set Variable activity updates a variable that already contains a value or default value?
Answer: An ADF variable can store only one value at a time in memory
. When a Set Variable activity executes, it overwrites the existing or default value with the new value
. Once overwritten, previous values cannot be retrieved or rolled back
.
Q4: Why does the Set Variable activity execute almost instantaneously compared to a Copy activity?
Answer: The Set Variable activity only updates a value in memory and does not perform any data movement or external data transport
. As a result, it typically completes in roughly one second
.
2. Pipeline Activity Dependencies & Execution Flow
Q5: How do activities execute in an ADF pipeline if they are placed on the canvas without connecting paths between them?
Answer: Activities that are not connected to each other execute in parallel simultaneously when the pipeline runs
.
Q6: How do you configure activities to run sequentially in ADF, and what execution states are available?
Answer: To run activities in serial (sequentially), you connect the output dependency port of one activity to the input of the next
. ADF provides four dependency condition states:
On Success (green line) — triggers the next activity only if the preceding activity succeeds
.
On Failure (red line) — triggers if the preceding activity fails
.
On Completion (blue line) — triggers regardless of whether the preceding activity succeeded or failed
.
On Skip — triggers if the preceding activity was skipped
.
3. Data Ingestion & Dynamic Content
Q7: Why might rows loaded into Azure SQL Database via ADF Copy Activity appear out of sequential order?
Answer: During a Copy Activity execution, data is written into the sink database in parallel streams as chunks of data arrive, which causes records to be inserted without guaranteeing a sequential order
.
Q8: What is the "Add dynamic content" feature in ADF?
Answer: Add dynamic content opens the ADF expression builder, allowing developers to write custom functions, expressions, and dynamic logic instead of hardcoding static text or values
---------------------

---

## 1. How do you rerun an ADF pipeline?

### Simple answer

If an ADF pipeline fails or you want to run it again:

1. Go to **ADF → Monitor**.
2. Find the pipeline run.
3. Select the failed/completed run.
4. Click **Rerun**.

ADF will run the pipeline again using the **same parameters and configuration**.

If you want to change the parameters, use **Rerun with different parameters**.

### Interview answer

> "I can rerun an ADF pipeline from the Monitor section. By default, rerun uses the same parameters and configuration. If I need to process a different date or file, I can use the rerun-with-different-parameters option."

**Example:**
Yesterday's pipeline failed while processing `2026-09-18`. I can rerun it instead of creating a new pipeline run.

---

# 2. How do you debug a specific activity in ADF?

### Simple idea

Suppose your pipeline has:

```text
Lookup
   ↓
Copy Data
   ↓
Databricks
   ↓
Stored Procedure
```

But you want to debug only the **Copy Data** activity.

You can put a **breakpoint** on that activity and run the pipeline in Debug mode.

### Steps

1. Open the pipeline.
2. Select the activity.
3. Set a **breakpoint**.
4. Click **Debug**.
5. ADF executes the pipeline and stops at the breakpoint.

### Interview answer

> "For debugging a specific activity, I can set a breakpoint on that activity and run the pipeline in Debug mode. This helps me inspect the activity before continuing with the remaining pipeline."

---

# 3. How do you restart a failed ADF job?

### Simple idea

Go to:

```text
ADF
 ↓
Monitor
 ↓
Pipeline Runs
 ↓
Find failed run
 ↓
Check failed activity/error
 ↓
Rerun
```

You can also rerun with different parameters if required.

### Important point

By default, **Rerun runs the pipeline again**.

If you want to retry only a particular activity, you need to design the pipeline with appropriate failure handling/checkpoint logic.

### Interview answer

> "I first check the failed activity and error message in ADF Monitor. After fixing the issue, I rerun the pipeline. If the pipeline requires activity-level recovery, I design checkpoint or failure-handling logic."

---

# 4. What are the dependency conditions in ADF?

ADF provides three important dependency conditions in the PDF:

### 1. Success

The next activity runs only if the previous activity succeeds.

```text
Activity A
    ↓ Success
Activity B
```

### 2. Failure

The next activity runs only if the previous activity fails.

```text
Activity A
    ↓ Failure
Send Alert
```

For example, if a Databricks activity fails → send an email.

### 3. Completion

The next activity runs regardless of whether the previous activity **succeeded or failed**.

```text
Activity A
    ↓ Completion
Activity B
```

### Easy way to remember

| Condition  | Meaning                              |
| ---------- | ------------------------------------ |
| Success    | Run when previous activity succeeds  |
| Failure    | Run when previous activity fails     |
| Completion | Run after previous activity finishes |

### Interview answer

> "ADF supports Success, Failure and Completion dependencies. I commonly use Success for normal workflow execution, Failure for error handling or alerts, and Completion when I want the next activity to run regardless of the previous activity's result."

---

# 5. Can multiple ADF pipelines use the same Self-Hosted Integration Runtime?

### Yes.

A **Self-Hosted Integration Runtime (SHIR)** can be shared across multiple ADF pipelines/factories.

### Simple example

Suppose you have:

```text
On-Premise SQL Server
        ↓
   SHIR Server
        ↓
 ┌───────────────┐
 │ ADF Pipeline 1│
 │ ADF Pipeline 2│
 │ ADF Pipeline 3│
 └───────────────┘
```

Instead of installing a separate SHIR for every pipeline, you can share it.

### Benefit

It reduces:

* Infrastructure overhead
* Maintenance
* Management effort

### Interview answer

> "Yes, a Self-Hosted Integration Runtime can be shared across multiple pipelines and factories. This is useful when multiple pipelines need to access the same on-premises or private network data sources."

---

# 6. How do you send email notifications from ADF?

ADF can integrate with **Logic Apps** for notifications.

### Simple flow

```text
ADF Pipeline
     ↓
Web Activity
     ↓
Logic App
     ↓
Email / Teams notification
```

ADF calls the Logic App using its HTTP trigger.

The Logic App then sends the email.

### Example

Suppose Databricks fails:

```text
ADF
 ↓
Databricks Activity
 ↓
FAILED
 ↓
Web Activity
 ↓
Logic App
 ↓
Email to support team
```

### Interview answer

> "I can integrate ADF with Azure Logic Apps for notifications. ADF uses a Web activity to call an HTTP-triggered Logic App, and the Logic App can send email or Teams notifications."

---

# 7. What is Get Metadata activity in ADF?

### Simple meaning

**Get Metadata tells you information about a file, folder, table, or other data source.**

For example, for a file:

```text
customer.csv
```

Get Metadata can help check things like:

* Does the file exist?
* File size
* Last modified time
* Child items inside a folder

### Example

Suppose you receive files every day:

```text
/raw/customer/
    customer_20260918.csv
    customer_20260919.csv
```

You can use **Get Metadata** to check the folder and get the available files.

Then use:

```text
Get Metadata
      ↓
ForEach
      ↓
Copy Activity
```

### Interview answer

> "Get Metadata is used to retrieve metadata about a file, folder, table or data source. I can use it to check file existence, size, last modified time or child items, and then use that information for further control flow."

---

# 8. What is Lookup activity in ADF?

### Simple meaning

**Lookup is used to read data from a source and return the result to the pipeline.**

A very common use case is getting a configuration value or the **last processed timestamp**.

### Example

Suppose we have a control table:

| pipeline_name | last_processed_date |
| ------------- | ------------------- |
| customer_load | 2026-09-18          |

Lookup reads this value.

Then the pipeline can use:

```text
2026-09-18
```

to determine what data needs to be processed.

The PDF also shows that a single returned row can be accessed using:

```text
@activity('Lookup1').output.firstRow
```

### Interview answer

> "Lookup is used to retrieve data from a source and make that data available to the pipeline. For example, I can use Lookup to get the last processed timestamp from a control table and use it for incremental loading."

### Easy difference

**Lookup = Get data/value**

**Get Metadata = Get information about the data/file**

---

# 9. How do you improve ADF pipeline performance?

The PDF focuses on reducing unnecessary waiting and increasing parallel processing. 

### Main techniques

### 1. Avoid unnecessary sequential activities

Bad:

```text
A → B → C → D
```

If B, C and D don't depend on each other, you may be able to run them in parallel.

Better:

```text
       → B →
A →    → C →    → Next
       → D →
```

### 2. Use parallelism

For example, with ForEach, process multiple files at the same time when appropriate.

```text
File1 ──→
File2 ──→
File3 ──→
File4 ──→
```

instead of:

```text
File1 → File2 → File3 → File4
```

### 3. Optimize Copy Activity

Use appropriate:

* Parallel copy
* Partitioning
* Staging

### 4. Optimize Mapping Data Flow

Use appropriate partitioning and compute resources.

### Interview answer

> "For ADF performance, I first identify unnecessary sequential dependencies and use parallel processing where possible. I also optimize Copy Activity using partitioning and parallel copy, optimize Data Flow partitioning, and select an appropriate Integration Runtime."

---

# 10. What is Auto Resolve Integration Runtime?

### Simple meaning

**Auto Resolve Azure Integration Runtime is a managed Integration Runtime provided by Azure.**

You don't have to manually manage the underlying infrastructure.

ADF can determine a suitable region for execution based on the data location.

### Example

Suppose:

```text
ADLS
 ↓
ADF
 ↓
Auto Resolve IR
 ↓
Processing
```

ADF manages the infrastructure for you.

### Why use it?

Because it helps with:

* Less infrastructure management
* Easier setup
* Appropriate regional execution
* Reducing unnecessary cross-region data movement

### Interview answer

> "Auto Resolve Integration Runtime is a fully managed Azure Integration Runtime. ADF automatically determines an appropriate region for execution based on the data location, so I don't have to manage the underlying infrastructure."

---

# ⭐ First 10 — What you should remember

| #  | Question           | Remember this                                                      |
| -- | ------------------ | ------------------------------------------------------------------ |
| 1  | Rerun pipeline     | **Monitor → Rerun**                                                |
| 2  | Debug activity     | **Breakpoint + Debug**                                             |
| 3  | Restart failed job | **Check error → Fix → Rerun**                                      |
| 4  | Dependencies       | **Success / Failure / Completion**                                 |
| 5  | Shared SHIR        | **One SHIR can serve multiple pipelines/factories**                |
| 6  | Email notification | **ADF → Web Activity → Logic App → Email**                         |
| 7  | Get Metadata       | **Get file/folder information**                                    |
| 8  | Lookup             | **Get data/value from source**                                     |
| 9  | Performance        | **Parallelism + partitioning + avoid unnecessary sequential flow** |
| 10 | Auto Resolve IR    | **Azure-managed execution environment**                            |

### 🧠 One simple real-project flow

You can connect many of these concepts together:

```text
        Daily Trigger
             ↓
       Get Metadata
             ↓
      File Available?
          ↓ Yes
        Lookup
             ↓
     Get Last Processed Date
             ↓
       ForEach Files
             ↓
       Copy Activity
             ↓
     Databricks Processing
             ↓
          Success
             ↓
       Update Control
```

If something fails:

```text
Databricks
    ↓
  Failure
    ↓
Logic App
    ↓
Email Alert
```

--------------------------------------------------------------------------------
Absolutely. Below are **Questions 11–20 from your PDF**, rewritten in a more **interview-friendly format** while keeping the concepts and order from the PDF. 

I’ll use this format for each one:

**Question → Simple explanation → Example → Interview answer → Key point**

---

# 11. What are the different types of triggers in Azure Data Factory?

### Simple explanation

A **trigger decides when an ADF pipeline should start**.

The PDF mentions four main types:

1. **Schedule Trigger**
2. **Tumbling Window Trigger**
3. **Event-based Trigger**
4. **Manual Trigger**

### Example

#### Schedule Trigger

Run every day at 2 AM:

```text
2:00 AM
   ↓
ADF Pipeline
```

#### Tumbling Window Trigger

Process fixed time windows:

```text
12:00–1:00
1:00–2:00
2:00–3:00
```

Each window represents a specific time period.

#### Event-based Trigger

Start when an event occurs, such as a file arriving in Blob Storage or ADLS.

```text
File arrives
     ↓
Event
     ↓
ADF Pipeline
```

#### Manual Trigger

You manually start the pipeline from ADF.

### Interview answer

> "ADF provides different types of triggers such as schedule, tumbling window, event-based and manual triggers. Schedule is used for time-based execution, tumbling window is useful for fixed time intervals and stateful processing, event-based triggers start the pipeline when an event occurs, and manual triggers are used when we want to start the pipeline manually."

### ⭐ Remember

**Schedule = Time**

**Tumbling Window = Fixed time interval**

**Event = Something happened**

**Manual = I start it**

---

# 12. How can you execute an ADF pipeline?

There are several ways to start a pipeline.

### 1. Trigger

For example:

```text
Daily at 1 AM
      ↓
Pipeline starts
```

### 2. Manual execution

You can manually click **Trigger Now**.

### 3. REST API / PowerShell

You can programmatically start a pipeline.

### 4. Logic Apps / Azure Functions / SDKs

Another Azure service can trigger the pipeline.

### 5. Execute Pipeline activity

One ADF pipeline can call another pipeline.

```text
Parent Pipeline
      ↓
Execute Pipeline
      ↓
Child Pipeline
```

### Interview answer

> "An ADF pipeline can be executed using triggers, manual execution, REST API or PowerShell, Azure services such as Logic Apps or Functions, and Execute Pipeline activity when one pipeline needs to call another pipeline."

### ⭐ Real project example

You might have:

```text
Master Pipeline
      ↓
Customer Load
      ↓
Vehicle Load
      ↓
Location Load
```

The master pipeline can use **Execute Pipeline** to run the individual pipelines.

---

# 13. What is an Event-Based Trigger in ADF?

### Simple explanation

An **event-based trigger starts a pipeline when an event happens**.

For example:

> A new file arrives in ADLS → ADF pipeline starts.

The PDF explains that ADF uses **Event Grid** behind the scenes for these events. 

### Example

Suppose a partner sends:

```text
/customer/customer_20260920.csv
```

When the file arrives:

```text
File arrives
     ↓
Event Grid event
     ↓
ADF Event Trigger
     ↓
Pipeline
     ↓
Copy Activity
```

The trigger can also provide information such as the **file name/path** to the pipeline.

### Interview answer

> "An event-based trigger starts an ADF pipeline when a specific storage event occurs, such as a file being created in Blob Storage or ADLS. ADF uses Event Grid for these events, and the event information can be passed to the pipeline."

### ⭐ Remember

**Schedule → "What time should I run?"**

**Event → "What happened that should start me?"**

---

# 14. What is the difference between Schedule Trigger and Tumbling Window Trigger?

This is a **very common interview question**.

| Schedule Trigger                                    | Tumbling Window Trigger           |
| --------------------------------------------------- | --------------------------------- |
| Runs at scheduled times                             | Runs for fixed time windows       |
| Not stateful                                        | Stateful                          |
| Doesn't inherently maintain window state            | Maintains window state            |
| Suitable for simple schedules                       | Good for time-sliced processing   |
| Doesn't provide the same built-in backfill behavior | Supports retry/backfill scenarios |

### Example

### Schedule

```text
Every day at 2 AM
      ↓
Run pipeline
```

### Tumbling Window

```text
00:00–01:00 → Process
01:00–02:00 → Process
02:00–03:00 → Process
```

Each window represents a defined time period.

### Interview answer

> "A schedule trigger is mainly used to run a pipeline at specific times. A tumbling window trigger divides processing into fixed, non-overlapping time windows and maintains state for those windows. Tumbling windows are useful when we need time-based processing, retries, dependencies or backfill."

### ⭐ Easy memory trick

**Schedule = When to run**

**Tumbling Window = Which time window to process**

---

# 15. How can you trigger a pipeline in another ADF?

Suppose you have:

```text
ADF Factory A
     ↓
ADF Factory B
```

You need to start a pipeline in Factory B from Factory A.

### Approach from the PDF

Use:

```text
ADF Factory A
      ↓
Web Activity
      ↓
ADF REST API
      ↓
ADF Factory B
      ↓
Pipeline
```

Authentication can use mechanisms such as a **service principal or managed identity**.

### Example

```text
Master ADF
    ↓
Web Activity
    ↓
Target ADF REST API
    ↓
Customer Pipeline
```

### Interview answer

> "To trigger a pipeline in another ADF instance, I can use a Web activity to call the target ADF REST API. Authentication can be handled using a service principal or managed identity, depending on the setup."

### ⭐ Remember

**ADF → Web Activity → REST API → Other ADF**

---

# 16. How can you automatically pause and resume an Azure Synapse Dedicated SQL Pool?

This question is about **saving compute cost when Synapse isn't being used**.

The PDF's approach is:

```text
ADF
 ↓
Web Activity
 ↓
Synapse REST API
 ↓
Pause / Resume
```

### Example

Suppose your data processing happens from:

```text
1 AM → 3 AM
```

You can:

```text
Before processing
       ↓
Resume Synapse
       ↓
Run workload
       ↓
Processing completed
       ↓
Pause Synapse
```

ADF Web Activity can call the Synapse REST API, assuming the required permissions are configured.

### Interview answer

> "We can automate the pause and resume of a Synapse Dedicated SQL Pool using ADF Web Activity and the Synapse REST API. For example, we can resume the pool before processing and pause it after the workload completes to avoid unnecessary compute usage."

### ⭐ Remember

**Web Activity + REST API = Control Synapse**

---

# 17. What is Azure Data Factory?

### Simple explanation

**Azure Data Factory, or ADF, is a cloud-based data integration and orchestration service.**

Think of ADF as a **manager**.

It doesn't necessarily do all the heavy data processing itself. Instead, it coordinates different activities and services.

### Example

```text
Source
  ↓
ADF
  ↓
Copy Data
  ↓
Databricks
  ↓
SQL
  ↓
Target
```

ADF can:

* Move data
* Schedule pipelines
* Orchestrate activities
* Call Databricks
* Call stored procedures
* Run data flows
* Monitor workflows

### Interview answer

> "Azure Data Factory is a cloud-based data integration and orchestration service. I use it to build pipelines that move data, schedule jobs, orchestrate different processing activities and integrate services such as ADLS, Azure SQL and Databricks."

### ⭐ Best way to remember

**ADF = Orchestrator / Manager**

---

# 18. How does Azure Data Factory work?

ADF works using several building blocks.

The PDF describes concepts such as:

```text
Pipeline
Activity
Dataset
Linked Service
Integration Runtime
Trigger
Control Flow
```

### Simple data journey

Suppose you want to move data from SQL Server to ADLS.

```text
Trigger
   ↓
Pipeline
   ↓
Copy Activity
   ↓
Linked Service → SQL Server
   ↓
Dataset
   ↓
Integration Runtime
   ↓
ADLS
```

### What happens?

**1. Trigger**

Decides when the pipeline starts.

**2. Pipeline**

Contains the workflow.

**3. Activity**

Performs a specific task.

**4. Linked Service**

Provides connection information.

**5. Dataset**

Points to the data.

**6. Integration Runtime**

Provides the compute/connection environment required for data movement or execution.

### Interview answer

> "ADF works by using pipelines to orchestrate activities. Activities interact with data through datasets and linked services, while Integration Runtime provides the execution or connectivity environment. Triggers determine when the pipeline starts, and control flow determines how activities execute."

---

# 19. What are the main components of Azure Data Factory?

This is an important question because interviewers often ask:

> "Explain ADF architecture/components."

The PDF lists these main components:

### 1. Pipeline

A container for your workflow.

```text
Pipeline
 ├── Lookup
 ├── Copy
 ├── Databricks
 └── Stored Procedure
```

### 2. Activity

One individual task.

Examples:

* Copy Activity
* Lookup
* Get Metadata
* Databricks
* Stored Procedure

### 3. Dataset

Represents/points to the data structure and location.

Example:

```text
customer.csv
```

### 4. Linked Service

Contains connection information.

Example:

```text
ADF → Azure SQL
ADF → ADLS
ADF → Databricks
```

### 5. Integration Runtime

Provides the environment for data movement/activity execution and connectivity.

### 6. Trigger

Determines when a pipeline starts.

### 7. Control Flow

Controls how activities are executed.

Examples:

```text
ForEach
If Condition
Switch
Execute Pipeline
Wait
```

### Interview answer

> "The main ADF components are pipelines, activities, datasets, linked services, Integration Runtime, triggers and control flow. A pipeline contains activities, datasets represent the data, linked services provide connection information, Integration Runtime provides the execution/connectivity environment, triggers start pipelines, and control flow defines the execution logic."

---

# 20. What is a Linked Service in ADF?

### Simple explanation

A **Linked Service is like a connection definition**.

It tells ADF:

> "How do I connect to this system?"

### Example

Suppose your source is Azure SQL:

```text
ADF
 ↓
Linked Service
 ↓
Azure SQL
```

The Linked Service contains the connection/authentication information required to connect.

Similarly:

```text
ADF → ADLS
ADF → Azure SQL
ADF → Databricks
ADF → Key Vault
```

can each have their own linked services.

### Why is it useful?

Because you can **reuse the connection** across multiple datasets and activities instead of defining the connection repeatedly.

### Interview answer

> "A Linked Service in ADF defines the connection and authentication information required to connect to an external data store or compute service. It is reusable, so multiple datasets or activities can use the same connection."

### ⭐ Easy memory trick

Think of it like your phone's saved Wi-Fi:

```text
Linked Service = How to connect
Dataset       = What data to access
Activity      = What to do
Pipeline      = Complete workflow
```

-------------------------------------------------------------------------------------------------------
Absolutely. Here are **Questions 21–30 from your PDF**, explained in a simpler and more interview-friendly way. I’m keeping the content based on the PDF rather than adding unrelated topics. 

---

# 21. What is a Dataset in Azure Data Factory?

### Simple explanation

A **Dataset tells ADF where the data is and what kind of data it is working with**.

Think of it like:

> **Linked Service = How to connect**
> **Dataset = Which data to access**

### Example

Suppose you have this file:

```text
ADLS
 └── raw
      └── customer
           └── customer.csv
```

You can create a Dataset that points to:

```text
ADLS → raw/customer/customer.csv
```

The Dataset uses a **Linked Service** to establish the connection to ADLS.

### Simple flow

```text
ADF
 ↓
Dataset
 ↓
Linked Service
 ↓
ADLS
 ↓
customer.csv
```

### Interview answer

> "A Dataset in ADF represents the structure and location of the data that an activity needs to work with. It uses a Linked Service to connect to the actual data store. For example, I can create an ADLS dataset pointing to a customer CSV file."

### ⭐ Remember

**Linked Service → Connection**

**Dataset → Data**

---

# 22. What is a Pipeline in Azure Data Factory?

### Simple explanation

A **Pipeline is a logical container that groups multiple activities together to perform a complete workflow.**

For example, suppose you need to load customer data:

```text
Pipeline
   ↓
Lookup
   ↓
Copy Data
   ↓
Databricks
   ↓
Stored Procedure
```

All these activities together form one pipeline.

### Real-world example

Imagine a manager gives instructions:

> 1. Get the file
> 2. Copy the file
> 3. Transform the data
> 4. Load the database
> 5. Send notification

The **Pipeline is the complete workflow**.

### Interview answer

> "An ADF Pipeline is a logical container that groups multiple activities to implement an end-to-end data workflow. For example, a pipeline can contain Lookup, Copy, Databricks and Stored Procedure activities."

### ⭐ Remember

**Pipeline = Complete workflow**

---

# 23. What is an Activity in ADF?

### Simple explanation

An **Activity is one individual task inside a pipeline**.

For example:

```text
Pipeline
   │
   ├── Lookup
   ├── Copy Data
   ├── Databricks
   └── Stored Procedure
```

Each one is an **Activity**.

### Common activities

The PDF gives examples such as:

* Copy Activity
* Stored Procedure
* Data Flow
* REST/Web activity
* Execute Pipeline

### Example

If you want to copy data from SQL Server to ADLS:

```text
Copy Activity
      ↓
SQL Server → ADLS
```

That's one activity.

### Interview answer

> "An Activity is an individual unit of work inside an ADF pipeline. Examples include Copy Activity, Lookup, Data Flow, Stored Procedure, REST and Execute Pipeline activities."

### ⭐ Remember

**Pipeline = Many tasks**

**Activity = One task**

---

# 24. What is the difference between ADF and SSIS?

This is a **very common interview question**.

### ADF

Azure Data Factory is a **cloud-based data integration and orchestration service**.

### SSIS

SQL Server Integration Services is traditionally used for **ETL, especially in SQL Server/on-premises environments**.

### Simple comparison

| ADF                                      | SSIS                          |
| ---------------------------------------- | ----------------------------- |
| Cloud-oriented                           | Traditionally on-premises     |
| Data integration + orchestration         | Mainly ETL                    |
| Azure service                            | SQL Server technology         |
| Supports many Azure services             | Strong SQL Server ecosystem   |
| Can integrate with Databricks, ADLS etc. | Commonly used with SQL Server |

ADF can also run existing SSIS packages using **Azure-SSIS Integration Runtime**.

### Interview answer

> "ADF is a cloud-based data integration and orchestration service, while SSIS is traditionally used for ETL workloads in SQL Server environments. If an organization has existing SSIS packages, ADF can also support running them using Azure-SSIS Integration Runtime."

### ⭐ Easy memory

**ADF = Cloud orchestration**

**SSIS = Traditional ETL**

---

# 25. How do you create and schedule a pipeline in ADF?

### Step-by-step

### Step 1 — Create Pipeline

Go to:

```text
Author
 ↓
Pipelines
 ↓
New Pipeline
```

### Step 2 — Add activities

For example:

```text
Lookup
 ↓
Copy
 ↓
Databricks
```

### Step 3 — Configure activities

Provide:

* Source
* Destination
* Dataset
* Parameters
* Settings

### Step 4 — Validate

Check whether there are configuration errors.

### Step 5 — Publish

Publish your changes.

### Step 6 — Create Trigger

For example:

```text
Every day at 2 AM
```

### Complete flow

```text
Create Pipeline
      ↓
Add Activities
      ↓
Configure
      ↓
Validate
      ↓
Publish
      ↓
Create Trigger
      ↓
Pipeline runs automatically
```

### Interview answer

> "I create the pipeline in the Author section, add and configure the required activities, validate the pipeline, publish the changes, and then create a trigger such as a schedule trigger to execute it automatically."

---

# 26. What is Copy Activity in ADF?

### Simple explanation

**Copy Activity is used to move data from a source to a destination.**

For example:

```text
SQL Server
    ↓
Copy Activity
    ↓
ADLS
```

Or:

```text
ADLS
 ↓
Copy Activity
 ↓
Azure SQL
```

### What can it do?

According to the PDF, Copy Activity supports things such as:

* Source-to-target data movement
* Column mapping
* Parallel copy
* Staging
* Retry
* Multiple source/destination systems

### Example

Suppose a partner sends:

```text
customer.csv
```

to ADLS.

You can use Copy Activity to move it into another storage/database location.

### Interview answer

> "Copy Activity is used for data movement between source and destination systems. I configure the source and sink datasets, mappings and performance settings such as parallel copy or staging when required."

### ⭐ Remember

**Copy Activity = Move data**

---

# 27. What is Mapping Data Flow in ADF?

### Simple explanation

**Mapping Data Flow is a visual, low-code way to transform data inside ADF.**

You don't have to write everything in SQL or Python.

You can visually create transformations such as:

* Filter
* Join
* Aggregate
* Derived Column
* Select

### Example

Suppose source data is:

| customer_id | age | city    |
| ----------- | --: | ------- |
| 101         |  25 | Delhi   |
| 102         |  17 | Gurgaon |
| 103         |  30 | Delhi   |

You want only customers above 18.

```text
Source
  ↓
Filter age > 18
  ↓
Target
```

Result:

| customer_id | age | city  |
| ----------- | --: | ----- |
| 101         |  25 | Delhi |
| 103         |  30 | Delhi |

### Important point

The PDF explains that Mapping Data Flow runs using **Spark behind the scenes**.

### Interview answer

> "Mapping Data Flow is a visual, low-code transformation capability in ADF. I can use it for transformations such as filtering, joining and aggregation without writing the complete transformation code. It uses Spark underneath for execution."

---

# 28. How do you debug a Mapping Data Flow?

### Simple explanation

ADF provides a **Data Flow Debug** option.

You can use it to test transformations and see the data flowing through the different steps.

### Example

Suppose your flow is:

```text
Source
  ↓
Filter
  ↓
Join
  ↓
Aggregate
  ↓
Sink
```

You can enable Debug and inspect the output at different transformation stages.

### Important point

The PDF says that Data Flow Debug starts a temporary Spark cluster for debugging. That cluster is shut down after inactivity.

### Interview answer

> "For Mapping Data Flow debugging, I enable Data Flow Debug and use data preview to inspect the output of different transformations. A temporary Spark environment is used for the debugging session and is shut down after inactivity."

### ⭐ Remember

**Data Flow Debug = Test transformation + Preview data**

---

# 29. What is Control Flow in ADF?

### Simple explanation

**Control Flow controls how activities execute inside a pipeline.**

It answers questions like:

> Should I repeat this task?

> Should I execute this activity only if a condition is true?

> Should I run another pipeline?

The PDF mentions examples such as:

* If Condition
* ForEach
* Switch
* Wait
* Execute Pipeline

### Example

```text
Check file exists
       ↓
     If Yes
       ↓
    ForEach
       ↓
 Process files
```

### Another example

```text
If country = India
      ↓
India Pipeline

If country = USA
      ↓
USA Pipeline
```

### Interview answer

> "Control Flow in ADF defines the execution logic and relationships between activities. For example, I can use If Condition for branching, ForEach for looping, Switch for multiple conditions, Wait for delays, and Execute Pipeline to call another pipeline."

### ⭐ Remember

**Control Flow = Controls the workflow**

---

# 30. What is ForEach activity in ADF?

### Simple explanation

**ForEach is used when you want to perform the same activity for multiple items.**

Think of it as a **loop**.

### Example

Suppose a folder contains:

```text
customer.csv
vehicle.csv
location.csv
```

You can get these files using Get Metadata and then pass them to ForEach.

```text
Get Metadata
     ↓
[customer.csv,
 vehicle.csv,
 location.csv]
     ↓
   ForEach
     ↓
Process each file
```

### Inside ForEach

```text
ForEach
   ↓
Copy Activity
```

The Copy Activity runs for each file.

### Sequential vs Parallel

ForEach can process items:

**Sequentially:**

```text
File1 → File2 → File3
```

or

**In parallel:**

```text
File1 ──→
File2 ──→
File3 ──→
```

Parallel execution can improve performance when the tasks are independent and the source/target can handle the load.

### Interview answer

> "ForEach activity is used to iterate over a collection of items and execute one or more activities for each item. The input can come from sources such as Lookup or Get Metadata. Depending on the requirement, I can configure it for sequential or parallel execution."

----------------------------------------------------------------------
Absolutely. Here are **Questions 31–40 from your PDF**, explained in a simple, interview-ready way. I’m keeping the same topics and order from the PDF. 

---

# 31. What is an Event-Based Trigger in ADF?

### Simple explanation

An **Event-Based Trigger** starts an ADF pipeline when a particular event happens.

The most common example is:

> **A new file arrives in Azure Blob Storage or ADLS → start the pipeline.**

### Example

Suppose your partner uploads:

```text
customer_20260920.csv
```

to ADLS.

The flow is:

```text
File uploaded
     ↓
Storage Event
     ↓
Event-Based Trigger
     ↓
ADF Pipeline
     ↓
Copy Activity
```

ADF uses **Azure Event Grid** for these storage events.

The event can also provide information such as the **file name and path**, which can be passed to the pipeline.

### Interview answer

> "An Event-Based Trigger starts an ADF pipeline when a specific storage event occurs, such as a file being created in Blob Storage or ADLS. ADF uses Event Grid to detect these events, and the event details can be passed to the pipeline."

### ⭐ Remember

**File arrives → Event → Pipeline starts**

---

# 32. How do you handle errors and retries in ADF?

This is a **very important interview question**.

ADF provides several ways to handle failures.

### 1. Retry

You can configure an activity with:

* Retry count
* Retry interval

For example:

```text
Databricks Activity
       ↓
     Failed
       ↓
    Retry 1
       ↓
    Retry 2
```

This is useful for temporary failures.

---

### 2. Failure Dependency

You can connect an activity using the **Failure** condition.

```text
Databricks
    ↓ Failure
Send Alert
```

---

### 3. Success Dependency

```text
Copy Data
    ↓ Success
Databricks
```

---

### 4. Completion Dependency

The next activity runs after the previous activity finishes, regardless of whether it succeeded or failed.

---

### 5. Alerts and Logging

You can use services such as:

* Logic Apps
* Azure Monitor
* Log Analytics

to monitor failures and send notifications.

### Interview answer

> "For error handling in ADF, I use activity retry settings for temporary failures and dependency conditions such as Success, Failure and Completion to control the workflow. For critical failures, I can trigger alerts using Logic Apps or monitoring services, and use logging to troubleshoot the issue."

### ⭐ Interview follow-up

**Interviewer:** "Would you retry every failure?"

**Good answer:**

> "No. I would mainly use retries for transient failures such as temporary connectivity or service issues. For permanent failures such as invalid data or incorrect configuration, retrying repeatedly would not solve the problem, so I would investigate the error and fix the root cause."

---

# 33. How do you secure an ADF pipeline?

The PDF focuses on several security mechanisms. 

### Main points

### 1. Managed Identity

Instead of storing passwords directly in the pipeline, ADF can use a **Managed Identity** to authenticate with Azure services.

```text
ADF
 ↓
Managed Identity
 ↓
Azure Service
```

---

### 2. Azure Key Vault

Secrets such as passwords, keys or connection secrets can be stored in **Key Vault** rather than hardcoded.

```text
ADF
 ↓
Key Vault
 ↓
Secret
```

---

### 3. RBAC

Use **Role-Based Access Control** to control who can access resources.

For example:

```text
Developer → Limited permissions
Admin     → More permissions
```

---

### 4. Encryption

Data should be protected during storage and transmission.

---

### 5. Private Endpoints

Private connectivity can be used to access Azure resources without exposing them publicly.

### Interview answer

> "For ADF security, I prefer managed identities instead of hardcoded credentials, store secrets in Azure Key Vault, use RBAC for access control, and use secure connectivity such as private endpoints where required."

### ⭐ Best interview point

Don't say:

> "I put passwords directly in the pipeline."

Instead:

> **"I use Managed Identity and Key Vault wherever possible."**

---

# 34. How do you control access to ADF resources?

### Simple explanation

ADF access can be controlled using **Role-Based Access Control (RBAC)**.

RBAC determines:

> **Who can access the resource and what they are allowed to do.**

### Example

Suppose you have:

```text
Developer
     ↓
Can develop/manage pipelines

Operator
     ↓
Can monitor/run pipelines

Admin
     ↓
More complete permissions
```

The exact roles/permissions depend on the organization's security setup.

For secrets, Azure Key Vault access can also be controlled through appropriate authorization mechanisms.

### Interview answer

> "I use Azure RBAC to control access to ADF and related Azure resources. Permissions are assigned according to the user's role and required access. For secrets, I also control access to Key Vault using the appropriate authorization model."

### ⭐ Remember

**RBAC = Who can do what?**

---

# 35. What is Integration Runtime in ADF?

This is one of the **most important ADF interview questions**.

### Simple explanation

**Integration Runtime, or IR, provides the environment required for ADF data movement and activity execution/connectivity.**

Think of it as the **bridge/engine that allows ADF to interact with data sources and destinations**.

For example:

```text
ADF
 ↓
Integration Runtime
 ↓
Source / Target
```

### Example

If your source is an on-premises SQL Server:

```text
On-Prem SQL Server
       ↑
       |
    SHIR
       |
       ↓
      ADF
```

The Self-Hosted Integration Runtime provides the bridge between ADF and the private/on-premises environment.

### Interview answer

> "Integration Runtime is the infrastructure used by ADF for data movement, connectivity and certain activity execution scenarios. It acts as the execution or connectivity bridge between ADF and the data sources or destinations."

---

# 36. What are the different types of Integration Runtime?

The PDF mentions **three main types**. 

## 1. Azure Integration Runtime

This is managed by Azure.

Used mainly for:

* Cloud data movement
* Cloud connectivity
* Data Flow execution

---

## 2. Self-Hosted Integration Runtime

This is installed and managed by you on your own infrastructure.

It is useful when ADF needs to connect to:

* On-premises systems
* Private network resources

Example:

```text
On-Prem SQL Server
       ↓
Self-Hosted IR
       ↓
ADF
```

---

## 3. Azure-SSIS Integration Runtime

Used to run **SSIS packages in Azure**.

Example:

```text
Existing SSIS Packages
        ↓
Azure-SSIS IR
        ↓
Azure
```

### Interview answer

> "ADF provides Azure Integration Runtime, Self-Hosted Integration Runtime and Azure-SSIS Integration Runtime. Azure IR is used for cloud-based connectivity and processing, Self-Hosted IR is useful for on-premises or private network connectivity, and Azure-SSIS IR is used to execute SSIS packages in Azure."

### ⭐ Easy memory

```text
Azure IR       → Cloud
Self-Hosted IR → On-prem/private
Azure-SSIS IR  → SSIS
```

---

# 37. How do you monitor ADF pipelines?

### Simple explanation

ADF provides a **Monitor** section where you can see pipeline and activity execution details.

You can check:

* Pipeline status
* Activity status
* Start/end time
* Duration
* Errors
* Parameters
* Run history

### Example

```text
ADF Monitor

Pipeline: Customer_Load
Status:   Failed
Activity: Copy_Data
Error:    Connection timeout
```

You can then investigate the failed activity.

### Additional monitoring

The PDF also mentions:

* Azure Monitor
* Log Analytics
* Alerts
* Dashboards

### Interview answer

> "I primarily use the ADF Monitor section to track pipeline and activity runs, execution time and errors. For centralized monitoring and alerting, I can integrate ADF with Azure Monitor and Log Analytics."

### ⭐ Real project answer

If an interviewer asks:

**"What do you check when a pipeline fails?"**

Say:

> "First, I check the pipeline run and identify the failed activity. Then I open the activity output and error message, check the input parameters and dependencies, fix the root cause, and rerun the pipeline."

---

# 38. What is Azure Data Lake Storage (ADLS), and how does ADF integrate with it?

### Simple explanation

**ADLS is Azure's storage service designed for large-scale data workloads.**

You can store:

```text
Raw data
Processed data
Historical data
Files
Parquet
CSV
JSON
```

### ADF + ADLS

ADF can move data between ADLS and other systems.

Example:

```text
Source
  ↓
ADF Copy Activity
  ↓
ADLS
  ↓
Databricks
  ↓
Processed data
```

### Typical architecture

```text
Source Systems
      ↓
     ADF
      ↓
ADLS Raw Layer
      ↓
 Databricks
      ↓
ADLS Processed Layer
```

### Interview answer

> "ADLS is Azure's scalable storage service for large data workloads. ADF can integrate with ADLS using linked services and datasets and can use Copy Activity to ingest or move data into different storage layers such as raw or processed data."

---

# 39. How can you perform transformations in ADF?

There are multiple approaches mentioned in the PDF. 

### Option 1 — Mapping Data Flow

Use ADF's visual transformation capability.

```text
Source
 ↓
Mapping Data Flow
 ↓
Filter
 ↓
Join
 ↓
Aggregate
 ↓
Target
```

---

### Option 2 — Stored Procedure

If transformation logic exists in a database:

```text
ADF
 ↓
Stored Procedure
 ↓
Database transformation
```

---

### Option 3 — Azure Databricks

For complex transformations:

```text
ADF
 ↓
Databricks
 ↓
PySpark
 ↓
Target
```

---

### Option 4 — Azure Functions

Useful for specific custom processing logic.

---

### Option 5 — Other compute services

The PDF also mentions services such as HDInsight.

### Interview answer

> "For transformations in ADF, I can use Mapping Data Flow for visual transformations, stored procedures for database-side logic, or external compute such as Azure Databricks when the transformation is complex or requires PySpark. Azure Functions and other processing services can also be integrated depending on the requirement."

### ⭐ Important for you

Since you work with Databricks, a strong answer is:

> **"I use ADF mainly for orchestration and use Databricks for complex transformations and PySpark processing."**

---

# 40. What services can ADF integrate with?

ADF can integrate with many Azure and external services.

The PDF specifically mentions examples such as:

### Storage

```text
Blob Storage
ADLS
```

### Databases / Analytics

```text
Azure SQL
Synapse
```

### Processing

```text
Databricks
HDInsight
Azure ML
```

### Other services

```text
Azure Functions
Logic Apps
Key Vault
Azure Monitor
Log Analytics
Azure DevOps
GitHub
```

### Example architecture

```text
             ADF
              |
      ┌───────┼────────┐
      ↓       ↓        ↓
     ADLS  Databricks  SQL
      ↓       ↓        ↓
    Storage Transform Database
              |
              ↓
           Monitor
```

### Interview answer

> "ADF integrates with a wide range of storage, database, compute and Azure services. For example, I can integrate ADF with ADLS, Blob Storage, Azure SQL, Synapse, Databricks, Functions, Logic Apps, Key Vault and monitoring services. This allows ADF to act as the orchestration layer for an end-to-end data pipeline."

---
Absolutely. Here are **Questions 41–50 from your PDF**, rewritten in a **simple, interview-friendly way** while keeping the same topics and order from the PDF. 

---

# 41. What are the best practices for designing an ADF pipeline?

### Simple explanation

When designing an ADF pipeline, the goal is to make it:

* Easy to understand
* Reusable
* Maintainable
* Reliable
* Easy to monitor

The PDF highlights these practices:

### 1. Make pipelines modular

Instead of creating one huge pipeline:

```text id="1zqf74"
One Huge Pipeline
 ├── Customer
 ├── Vehicle
 ├── Location
 ├── Payment
 └── ...
```

Break it into smaller pipelines:

```text id="z5b7xg"
Master Pipeline
    ↓
 ├── Customer Pipeline
 ├── Vehicle Pipeline
 ├── Location Pipeline
 └── Payment Pipeline
```

---

### 2. Use parameters

Don't hardcode values.

For example:

```text id="8l9k9j"
customer_20260920.csv
```

Instead, pass:

```text id="8q8a8w"
file_name
```

Then reuse the same pipeline for different files.

---

### 3. Add error handling and retries

Use:

* Retry settings
* Failure paths
* Alerts
* Logging

---

### 4. Use parallel processing where possible

For example, process independent files simultaneously using ForEach.

---

### 5. Use secure authentication

Prefer:

* Managed Identity
* Key Vault

instead of hardcoded passwords.

---

### 6. Monitor the pipeline

Use ADF Monitor and Azure monitoring services.

### Interview answer

> "My main ADF best practices are modular pipeline design, parameterization, proper error handling and retries, parallel processing where appropriate, secure authentication using managed identity or Key Vault, and proper monitoring and logging."

### ⭐ Remember

**Modular + Parameterized + Secure + Reliable + Monitorable**

---

# 42. How do you optimize ADF pipeline performance?

This is an important question for a **3+ year Data Engineer**.

The PDF focuses on avoiding unnecessary waiting and using parallel processing. 

### 1. Reduce unnecessary sequential dependencies

Instead of:

```text id="u3h0zy"
A → B → C → D
```

If B, C and D are independent:

```text id="g8n5iq"
       ┌→ B ─┐
A ─────┼→ C ─┼→ Next
       └→ D ─┘
```

This allows parallel processing.

---

### 2. Use ForEach parallelism

If you have 100 independent files, don't necessarily process them one by one.

```text id="3n2g0g"
File 1 ──→
File 2 ──→
File 3 ──→
File 4 ──→
```

---

### 3. Optimize Copy Activity

The PDF mentions:

* Parallel copy
* Partitioning
* Staging

---

### 4. Optimize Data Flow

Use appropriate partitioning and compute resources.

---

### 5. Use the appropriate Integration Runtime

The execution/connectivity environment should match the workload.

### Interview answer

> "I optimize ADF performance by reducing unnecessary sequential dependencies and using parallelism where possible. I also optimize Copy Activity using partitioning, parallel copy or staging, tune Data Flow partitioning, and use an appropriate Integration Runtime."

### ⭐ Important

Don't simply say:

> "I increase parallelism."

Say:

> "I increase parallelism where the source and target can handle the additional load."

---

# 43. What is a Tumbling Window Trigger?

### Simple explanation

A **Tumbling Window Trigger divides time into fixed, non-overlapping windows**.

For example, with a 1-hour window:

```text id="y7t4fr"
00:00 ─── 01:00
01:00 ─── 02:00
02:00 ─── 03:00
03:00 ─── 04:00
```

Each window represents one processing period.

### Why is it useful?

According to the PDF, it is useful when you need:

* Fixed time intervals
* Stateful processing
* Retry
* Backfill
* Dependencies between windows

### Example

Suppose you process hourly transaction data:

```text id="msf9jh"
01:00 → Process 00:00–01:00
02:00 → Process 01:00–02:00
03:00 → Process 02:00–03:00
```

Each time window can be tracked separately.

### Interview answer

> "A Tumbling Window Trigger divides processing into fixed, non-overlapping time intervals. It maintains state for those windows and is useful when we need time-based processing with retry, dependencies or backfill."

### ⭐ Remember

**Tumbling Window = Fixed + Non-overlapping + Stateful**

---

# 44. What is the difference between ADF and Azure Databricks?

This is **very important for your Databricks interviews**.

### ADF

ADF is mainly used for:

* Orchestration
* Scheduling
* Data movement
* Workflow management

### Databricks

Databricks is mainly used for:

* Complex transformations
* PySpark/Spark processing
* Big-data processing
* Analytics
* Machine learning

### Typical architecture

```text id="8kq3d7"
              ADF
               ↓
         Orchestration
               ↓
          Databricks
               ↓
          PySpark/Spark
               ↓
        Transformation
               ↓
         ADLS / Database
```

### Example

ADF says:

> "Run the customer transformation job."

Databricks does:

> "Read 100 million records and transform them using PySpark."

### Interview answer

> "ADF is primarily an orchestration and data integration tool, while Databricks is a compute and data processing platform. In a project, I can use ADF to schedule and orchestrate the workflow and Databricks to perform complex transformations using PySpark."

### ⭐ Easy memory

**ADF = Manager**

**Databricks = Worker**

---

# 45. How do you implement ETL using ADF?

### ETL means

```text id="w8g8u4"
E = Extract
T = Transform
L = Load
```

### Step 1 — Extract

Get data from the source using Copy Activity.

```text id="x9k3ht"
SQL / API / File
      ↓
     ADF
```

### Step 2 — Stage

Store raw data in ADLS/Blob if required.

```text id="0gr5tg"
Source
 ↓
ADF
 ↓
ADLS Raw
```

### Step 3 — Transform

You can use:

* Mapping Data Flow
* Databricks
* Stored Procedure
* Functions

For your Databricks-based project:

```text id="3q8v6a"
ADLS Raw
    ↓
Databricks
    ↓
PySpark
    ↓
Transformed Data
```

### Step 4 — Load

Load the final data into the target.

```text id="pj0n4h"
Transformed Data
       ↓
Target Database / ADLS
```

### Complete flow

```text id="7k0m7h"
Source
  ↓
Extract
  ↓
ADLS Raw
  ↓
Transform
  ↓
Load
  ↓
Target
```

### Interview answer

> "For ETL, I use ADF to orchestrate the workflow. I extract data using Copy Activity, optionally stage it in ADLS, transform it using Mapping Data Flow or Databricks depending on the complexity, and then load the transformed data into the target system."

---

# 46. What is the ADF REST API used for?

### Simple explanation

The **ADF REST API allows us to interact with ADF programmatically instead of doing everything manually through the ADF UI.**

You can use it to:

* Start pipeline runs
* Monitor pipeline runs
* Manage triggers
* Manage ADF resources such as datasets and linked services

### Example

Instead of manually clicking:

```text id="1m9q4c"
ADF
 ↓
Trigger Now
```

another application can call an API:

```text id="0h5n0d"
Application
     ↓
ADF REST API
     ↓
Start Pipeline
```

### Interview answer

> "ADF REST API allows us to programmatically manage and interact with ADF resources. For example, we can trigger a pipeline, monitor pipeline runs, and manage resources such as triggers, datasets and linked services."

### ⭐ Remember

**ADF UI = Manual**

**ADF REST API = Programmatic**

---

# 47. How do you implement version control in ADF?

This is important when working in a **team environment**.

ADF supports integration with Git-based source control.

### Typical workflow

```text id="5pygxy"
Developer
    ↓
Feature Branch
    ↓
Make ADF Changes
    ↓
Commit
    ↓
Pull Request
    ↓
Review
    ↓
Merge
```

ADF pipeline definitions are stored as configuration/JSON files in the repository.

### Example

Suppose you need to modify:

```text id="0y3x55"
Customer Pipeline
```

Instead of directly changing production:

```text id="psx9h5"
develop
   ↓
feature/customer-pipeline
   ↓
Changes
   ↓
PR
   ↓
Review
   ↓
Merge
```

### Interview answer

> "I use Git integration for ADF version control. Developers work on feature branches, commit pipeline changes, raise pull requests for review, and merge the approved changes into the appropriate branch. This also supports CI/CD across environments."

### ⭐ Good interview point

Mention:

**Branch → Commit → PR → Review → Merge → Deploy**

---

# 48. What is Self-Hosted Integration Runtime and when do you use it?

### Simple explanation

**Self-Hosted Integration Runtime (SHIR)** is an Integration Runtime that you install and manage on your own machine or server.

It is mainly useful when ADF needs to access:

* On-premises databases
* Private network resources
* Systems that aren't directly accessible from Azure

### Example

Suppose your company has an on-premises SQL Server:

```text id="x0my8q"
On-Prem SQL Server
        ↑
        |
      SHIR
        |
        ↓
       ADF
```

SHIR acts as the bridge.

### Interview answer

> "Self-Hosted Integration Runtime is used when ADF needs connectivity to on-premises or private network data sources. We install the runtime on infrastructure that can access the source system, allowing ADF to securely perform data movement or connectivity operations."

### ⭐ Easy memory

**Cloud source → Azure IR**

**On-prem/private source → Self-Hosted IR**

---

# 49. What is Schema Drift in ADF?

### Simple explanation

**Schema drift means the structure of incoming data can change.**

For example, today's file contains:

```text id="l0x0g8"
customer_id
name
email
```

Tomorrow the source sends:

```text id="t3r0vf"
customer_id
name
email
phone
```

The `phone` column is new.

Or a source might remove/rename a column.

### Why can this be a problem?

A pipeline that expects a fixed schema may fail or ignore the new column depending on its configuration.

### ADF approach

The PDF mentions features such as:

* Allow Schema Drift
* Wildcard mappings
* Parameterized datasets
* Schema projection

### Example

```text id="n0m3t2"
Source
 ↓
Schema Drift enabled
 ↓
Transformation
 ↓
Target
```

This allows the pipeline/data flow to handle certain schema changes more flexibly.

### Interview answer

> "Schema drift occurs when the source schema changes over time, such as a new column being added. In ADF Mapping Data Flow, I can enable schema drift and use appropriate mappings so the pipeline can handle changing schemas without requiring manual changes for every new column."

### ⭐ Real interview example

**Interviewer:** "A partner adds a new column to the CSV. What will you do?"

You can say:

> "First, I would confirm whether the new column needs to be propagated to the target. If the requirement is to support dynamic schema changes, I can use schema-drift capabilities in Mapping Data Flow. If the target has a fixed schema, I would validate the change and update the target mapping/schema as required."

---

# 50. What is Azure Monitor and how is it used with ADF?

### Simple explanation

**Azure Monitor helps monitor the health and execution of Azure resources, including ADF.**

With ADF, you can monitor things such as:

* Pipeline runs
* Activity runs
* Execution duration
* Failures
* Logs
* Metrics

### Example

```text id="w0g8ds"
ADF Pipeline
     ↓
Azure Monitor
     ↓
Logs / Metrics
     ↓
Alerts
```

You can also use **Log Analytics** for centralized log analysis.

### Example failure

```text id="1z9n0f"
Customer Pipeline
       ↓
     FAILED
       ↓
Azure Monitor
       ↓
Alert
       ↓
Support Team
```

### Interview answer

> "Azure Monitor can be used to monitor ADF pipeline and activity execution, collect logs and metrics, and configure alerts. For centralized log analysis, ADF monitoring information can also be integrated with Log Analytics."

---


