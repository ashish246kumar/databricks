What is the name of the platform that enables the execution of Databricks applications

The platform that actually executes Databricks applications is Apache Spark.
Azure Databricks is built on top of Spark, which is a powerful open-source distributed computing engine. 
Spark processes large amounts of data in parallel across a cluster of machines, which makes it very fast and scalable
Databricks enhances Spark by adding features like Delta Lake integration, and easy connectivity with Azure services. But underneath all that, Spark is the core engine that executes all the code
_____________________________________________________________________________
43. How do you mount Azure Data Lake Storage to Databricks?
    To connect Azure Data Lake to Databricks, I mount the storage using the dbutils.fs.mount() command. This links ADLS to the Databricks File System (DBFS), making the data available like a local directory

Here's how I usually do it: 
Get the storage credentials — this includes the storage account name, container name, and authentication details (like service principal credentials or managed identity).
Use the dbutils.fs.mount() command to mount ADLS to a mount point in DBFS. For example:
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<application-id>",
  "fs.azure.account.oauth2.client.secret": "<application-secret>",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

Access the files just like local files, for example:
spark.read.csv("/mnt/<mount-name>/data.csv")

______________________________________________________

4. What are the various optimization techniques in Spark?

Partitioning Optimization:
 There are several techniques I use to optimize Spark jobs for better performance and lower resource usage
   Use repartition() or coalesce() to control the number of partitions, and partition data by key columns to reduce shuffling

Caching and Persistence:
Cache intermediate DataFrames that are reused and unpersist() them when no longer needed   
Broadcast Joins:
Broadcast smaller DataFrames to avoid large shuffles in joins.
Adaptive Query Execution (AQE):
Enable AQE (spark.sql.adaptive.enabled=true) so Spark can optimize execution plans at runtime

Filter and select only the required data early on. File formats like Parquet and Delta support this efficiently

Delta Lake Optimization:
Delta format improves performance through indexing, caching, and ACID capabilities.

Use Efficient File Formats:
Prefer Parquet or Delta over CSV/JSON for better compression and faster read/write.



__________________________________________________________________________________________
When referring to Azure Databricks, what exactly does it mean to "auto-scale" a cluster

Auto-scaling in Azure Databricks means that the cluster can automatically increase or decrease the number of worker nodes based on the workload. For example, if I run a heavy job that needs more processing power, Databricks will automatically add more nodes. When the workload reduces, it will remove the extra nodes to save cost.

So basically, I don’t have to manually adjust the cluster size — Databricks takes care of it for me. This helps improve performance when needed and also keeps costs under control. For instance, if my cluster is set to auto-scale between 2 and 8 workers, it might start with 2, scale up to 8 during peak load, and then go back down to 2 when the job finishes


______________________________________________________________________________________________________________
Why is it necessary for us to use the DBU Framework? D

DBU stands for Databricks Unit. It’s basically a unit that measures the processing power used per hour in Azure Databricks.
Using the DBU framework is important because it helps track and control the cost of running workloads. 
Different workloads and cluster types consume different amounts of DBUs, so it gives a clear idea of how much a job will cost before running it.
It also separates the Databricks cost from the underlying VM cost, which makes budgeting and cost management much easier and more transparent


_______________________________________________________________________________________________________
What is the function of the Databricks filesystem? 

The Databricks File System, or DBFS, is a distributed file system built on top of cloud storage.
It acts like a bridge between Databricks and the underlying cloud storage, such as Azure Blob Storage.
For example, I can upload a CSV file to DBFS and read it using a path like /dbfs/FileStore/mydata.csv.
The main functions of DBFS are:
Storing input and output data used in Spark jobs or ML models.
Serving as a temporary workspace for intermediate files.
____________________________________________________________________
 What is a delta table in Databricks? 
 
A Delta table is a Databricks table built on Delta Lake. 
It adds features like ACID transactions, version control, and schema enforcement to data lakes — so I can manage big data with the reliability of a traditional database
think of it as a smart version of a data lake table that supports both batch and streaming data efficiently.

ACID Transactions: Ensures data consistency even with concurrent reads and writes.
Schema Evolution: Allows adding or modifying columns easily.
Efficient Updates & Deletes: Supports SQL operations like UPDATE, DELETE, and MERGE.
High Performance: Uses indexing and caching for faster queries.
_______________________________________________________________________________________________________________________________________
1. What is Azure Databricks and advantages

Azure Databricks is a cloud-based big data and machine learning platform built on Apache Spark and integrated with Microsoft Azure.
It provides an interactive and collaborative environment where I can use Python, SQL, or Scala to process large datasets efficiently.
What I like most is that it handles cluster setup and scaling automatically, integrates well with services like Azure Data Lake, Synapse, and Power BI, 
and provides strong security using Azure’s RBAC.
For example, I can read raw customer data from Azure Data Lake, clean and transform it in Databricks, train a machine learning model, and then visualize 
the results in Power BI — all within one environment.

4. Built-in Security and Integration
  You can connect Databricks securely to:
Azure Data Lake (for raw data)
Azure Key Vault (for secrets)

Azure Synapse (for analytics)
without exposing passwords or tokens.

5. Supports Multiple Languages
6. Job Orchestration
Why: You can schedule, monitor, and automate data workflows directly in Databricks


___________________________________________________________________________















___________________________________________________________________________________________________________________________________

Data lake ek digital talab hai, jahan saara data – raw, kaccha-pakka, structured, unstructured – store hota hai
Kaise Kaam Karta Hai?
Alag-alag sources se data aata hai (website, apps, sensors, social media), aur jaisa hai waisa hi dump ho jata hai.
Baad mein tools (Hadoop, Spark, AWS) se process karke insights nikaalte hain.
Data Lake vs Data Warehouse?
Data lake = Kaccha-pakka mix, flexible, raw data ke liye.
Data warehouse = Fully pakka, structured, business reporting ke liye.

Fayde Kya Hain?
Flexible – koi bhi data store karo (text, images, videos).
Scalable – cloud mein easily bada karo.
Sasta – raw data store karna costly nahi.
Advanced analytics – AI, machine learning ke liye best.
Challenges Kya Hain?
Data swamp – agar manage nahi kiya toh ganda talab ban jata hai.
Security – sensitive data leak ka risk.
Skills chahiye – big data tools, programming ka knowledge zaroori.
Use Kahan Hota Hai?
E-commerce (recommendations), healthcare (AI models), finance (fraud detection), social media (ads targeting).
Future Kya Hai?
Bright hai, kyunki data badh raha hai, aur cloud + AI ke saath powerful ho raha hai.
*********
What is Control Plane and Data Plane in Databricks?
Control Plane:
Ek manager ya control room jo Databricks ke cloud account mein hota hai.
Backend services manage karta hai – UI, notebooks, clusters, security, user management.
Data processing nahi karta, sirf instructions deta hai.
Data Plane:
Ek kaam ka maidan jo customer ke cloud account mein hota hai.
Yahan data processing hota hai – clusters banate hain, data analyze karte hain.
Do types:
Classic Compute: Customer ke account mein, customer manage karta hai.
Serverless Compute: Databricks ke account mein, Databricks manage karta hai.
Connection:
Control plane aur data plane secure network (HTTPS, private tunnels) se baat karte hain.
******
2. What is High-Level Architecture of Databricks?

Control Plane (Databricks ka Area):
Databricks ke cloud account mein hota hai.
Services: Web UI, cluster manager, workflow manager, Unity Catalog, notebook storage.
Customer ka data touch nahi karta, sirf manage karta hai.
Data Plane (Customer ka Area):
Customer ke cloud account mein hota hai.
Parts:
Clusters: Data processing ke liye compute resources (classic ya serverless).
Storage: Customer ka data (S3, ADLS, Google Cloud Storage) store hota hai.
Networking aur Security:
Secure communication HTTPS ya private tunnels (AWS PrivateLink, Azure Service Endpoints) ke through.
Security features: Encryption, access control (RBAC), network isolation.
Example: Control plane = Restaurant ka manager, data plane = Kitchen jahan kaam hota hai.
*********
3. What are the Different Roles Available in Databricks?
Account-Level Roles:
Account Owner: Super boss – poore account ko control karta hai (workspaces, users, billing, security).
Account Admin: Owner ka helper – users, workspaces, settings manage karta hai.
Workspace-Level Roles:
Workspace Admin: Ek workspace ka manager – users, clusters, permissions manage karta hai.
Workspace User: Normal kaamgaar – notebooks, queries, data analysis ka kaam karta hai (limited permissions).
Workspace Contributor: Limited role – specific objects (notebooks, jobs) pe kaam karta hai.
Data-Level Roles (Unity Catalog):
Owner: Data objects (tables, schemas) ka poora control.
Read/Write: Data ko read ya modify kar sakta hai.
Execute: Jobs ya queries run kar sakta hai.
Service Principals:
Automated tools, scripts ke liye role – humans ke liye nahi.
Example: Account owner = School principal, workspace admin = Class teacher, workspace user = Student.

*****
4. What is Databricks Workspace?
Workspace Kya Hai?:
Ek shared office jahan teams kaam karte hain – data analysis, ML, ETL, dashboards.
Workspace Mein Kya Hota Hai?:
Notebooks: Code likhne aur run karne ke liye (Python, SQL, Scala, R).
Clusters: Data processing ke liye compute resources.
Jobs: Scheduled ya automated tasks (daily data processing).
Libraries: External code ya packages.
Dashboards: Data visualizations aur reports.
Experiments: ML models track karne ke liye.
Features:
Folders mein assets organize kar sakte hain.
Access control – admin decide karta hai kaun kya kar sakta hai.
Apna storage bucket – system data (logs, revisions) store karne ke liye.
Example: Workspace = Office, jahan desks (notebooks, clusters) hote hain, aur manager (admin) decide karta hai kaun kya karega.
5. What is the Relation Between Databricks Account and Workspace?
Databricks Account:
Ek top-level entity – company ya organization ka poora setup.
Multiple workspaces manage karta hai.
Billing, user management, security, governance account level pe hota hai.
Workspace:
Account ke andar ek specific environment jahan teams kaam karte hain.
Apna URL, storage bucket, aur resources hota hai.
Relation:
Ek account mein multiple workspaces ho sakte hain (jaise production, development, testing).
Ek workspace sirf ek account ka part hota hai.
Unity Catalog account level pe data governance deta hai – saare workspaces ke data ka metadata ek jagah.
Example: Account = School, workspaces = Classrooms. Principal (account owner) decide karta hai kitne classrooms honge, aur har classroom ka apna teacher (workspace admin) hota hai.
******************


why is data relability and performance important in data enginerring

Why Data Reliability is Important?
Sahi Decisions: Unreliable data se galat business decisions hote hain, aur loss hota hai.
Consistency: Alag-alag systems ka data match karna zaroori hai, warna confusion hota hai.
Trustworthy Analytics: Galat data se reports aur ML models galat hote hain.
Compliance: Laws (GDPR, HIPAA) ke liye reliable data zaroori hai, warna legal trouble hota hai.
Customer Trust: Galat data se customer experience kharab hota hai, aur trust toot jata hai.
***********
Why Performance is Important?
Real-Time Insights: Slow pipelines se data late milta hai, aur decisions delay hote hain.
Big Data Handling: Bade data volumes ko jaldi process karna zaroori hai, warna bottlenecks bante hain.
Cost Efficiency: Slow systems se zyada cloud resources use hote hain, aur bill badhta hai.
User Experience: Slow queries ya dashboards se productivity kam hoti hai.
Scalability: Performant systems easily scale karte hain jab data volume badhta hai
******************
Problems Encountered When Using Data Lake
Data Swamp: Data lake messy ho jata hai – no metadata, duplicates, inconsistent formats – use karna mushkil hota hai.
Security Risks: Sensitive data leak ho sakta hai agar access control aur encryption strong nahi hai.
Poor Data Quality: Raw data mein errors, missing values, ya inconsistent formats se analytics galat hota hai.
Slow Performance: Bade data volumes aur small files se queries slow ho jati hain, real-time insights nahi milte.
Business Misalignment: Business goals ke saath align nahi hota, toh data lake sirf storage ban jata hai.
Complexity in Data Processing
Problem: Data lake mein raw data process karna complex hota hai, especially streaming data aur batch data ko combine karna
Kyun Hota Hai: Traditional data lakes ACID transactions (atomicity, consistency, isolation, durability) support nahi karte, toh data processing mein errors aate hain.
Lack of Schema Enforcement

*******

Main Features of Delta Lake
ACID Transactions: Data writes, updates, deletes reliable hote hain, multiple users safely kaam kar sakte hain.
Schema Enforcement: Data store karte time schema enforce karta hai, data quality improve hoti hai.
Schema Evolution: Schema mein changes (jaise new columns) add kar sakte ho bina data break kiye.
Time Travel: Data ke purane versions access ya restore kar sakte ho, recovery easy hota hai.
Performance Optimization: Data skipping, Z-Order indexing, caching, file compaction se queries fast hoti hain.
Unified Processing: Batch aur streaming data ek hi table mein handle kar sakta hai.
******
data science, data enginerring data scence  (Ek field hai jo data se insights nikaalne ke liye math, stats, aur programming ka use karta hai)
|
data intelligence engine  (Business teams ko real-time decisions lene mein help karta hai, jaise customer behavior samajhna ya sales improve karna.)
|
unity catalog(governance)(Databricks ka data governance tool hai jo metadata manage karta hai, taaki data discoverable aur secure rahe)
|
delta lake(core dl)  (Data lakes ke upar ek storage layer hai jo reliability (ACID transactions) aur performance (indexing, caching) deta hai.)

|
clouddatalake  (Cloud mein bada data storage hai jahan raw, structured, aur unstructured data store hota hai)
*********

delta lake integrate with all the major analalytic tool
A. Apache Spark (Core Integration)
B. SQL-Based Query Engines(Apache Hive)
C. Cloud-Native Analytic Tools(AWS Athena,Snowflake,Google BigQuery)
Business Intelligence (BI) Tools
**
Photon ka main kaam hai SQL queries aur data processing tasks ko optimize karna

Vectorized Execution:ek baar mein ek row process karne ke bajaye, ye ek saath thousands of rows process karta hai
Compatibility: Photon Spark SQL aur DataFrame APIs ke saath compatible hai.bas Photon enable karo aur speed boost milta hai.
Cost Efficiency: Kam compute resources use hote hain, toh cloud costs kam ho jate hain.
Photon Ka Use Kahan Hota Hai?
E-commerce: Real-time inventory management, sales analytics, customer recommendations.
IoT: Sensor data processing, predictive maintenance, anomaly detection.
*******
Data lakes mein governance ek badi problem hai kyunki:
(alag-alag systems mein bikhra hua), toh discoverability aur access control mushkil hota hai.
AI models ke liye governance nahi hota, toh model versioning, auditing, aur compliance track karna mushkil hota hai.
(data kahan se aaya, kaise use hua) track karna hard hota hai.
****
2. How Databricks Lakehouse Solves Governance Challenges
Unity Catalog: Ek centralized governance layer jo data (tables, files) aur AI assets (models, notebooks) ko ek hi jagah manage karta hai, discoverability, access control, lineage, aur auditing deta hai.
Data Quality: Delta Lake ke schema enforcement, ACID transactions, aur time travel se data reliable hota hai
Databricks Notebooks, SQL, aur Delta Sharing se teams ek hi platform pe kaam kar sakte hain,
***********
Databricks ek unified data analytics platform hai jo data engineering, data science, machine learning (ML), aur business intelligence (BI) ko ek hi jagah pe laata hai.
Databricks ka main idea hai Lakehouse Architecture
data lake aur data warehouse ke benefits ko combine karna
– Databricks dono ko ek platform pe laata hai.

Databricks Ke Main Features
Unified Platform
Lakehouse Architecture:
Scalability:
Cloud-based hone ki wajah se Databricks petabyte-scale data handle kar sakta hai, aur auto-scaling clusters ke saath resources adjust ho jate hain
Governance:
High Performance:
Photon engine
***********************************************************************************************************************
dbutils.fs.ls()
What is Serverless Compute in Databricks?
Serverless Compute ek feature hai jahan Databricks compute resources ko automatically manage karta hai – no manual cluster setup ya maintenance.

Benefits of Serverless Compute
No Management: Databricks sab handle karta hai, tension-free.
Fast: Instant startup (seconds mein), Photon se high performance.
Cost-Saving: Pay-as-you-go, no idle costs, 20-40% savings.
******
SQL warehouse
Scaling happens on warehouse level, allows you to add or remove clusters. It means number of cluster are increased or decreased as per definition of Warehouse size.
******
Databricks SQL (DBSQL) ek tool hai jo Databricks Lakehouse mein SQL-based analytics aur reporting ke liye banaya gaya hai.
******
Query Profile ek tool hai Databricks SQL mein jo query performance ko analyze karne mein help karta hai. Ye batata hai ki query kyun slow hai aur kaise improve ho sakti hai.
***
5. How to Monitor SQL Query Performance?
Query History:
DBSQL ke “Query History” section mein har query ka record hota hai – execution time, status (success/failed), aur resource usage

*******
In Apache Spark, yeh cluster has one boss called "master" and kai saare workers called "nodes" that do kaam together
********
 Spark ka Core Architecture
Spark ka architecture Master-Slave model pe based hota hai. Matlab ek Driver Program hota hai jo Executors ko kaam assign karta hai.

🖥 Driver Program:

Ye main process hota hai jo job ko initiate karta hai.
Iske andar ek SparkContext hota hai jo pura execution manage karta hai.
⚡ Cluster Manager:

Ye resources allocate karta hai.
Spark ke cluster manager YARN, Mesos ya Standalone mode me ho sakte hain.
🖥 Executors:

Ye worker nodes hote hain jo actual computation karte hain.
Driver inko kaam assign karta hai aur ye tasks execute karte hain.


Spark ke Major Components
🚀 Spark Core:

Isme RDDs (Resilient Distributed Datasets) hote hain jo distributed processing ka main part hain.
Task scheduling, Memory Management, Fault tolerance yahi handle karta hai.
📊 Spark SQL:

Ye structured data ke liye hota hai.
Isme DataFrames and Datasets hote hain jo SQL queries ko optimize karte hain.
🔥 Spark Streaming:

Ye real-time data processing ke liye hota hai.
Kafka, Flume, socket streams se data le sakta hai.


Apache Spark Workflow
1️⃣ Data Load: Data kisi bhi source se aa sakta hai (HDFS, S3, DB).
2️⃣ RDD Creation: Spark Core RDDs create karta hai.
3️⃣ Transformations: Operations perform hote hain jaise map(), filter(), reduce().
4️⃣ Actions: Jab hum result chahiye toh actions like collect(), count() use hote hain.
5️⃣ Execution & Optimization: Spark DAG (Directed Acyclic Graph) banata hai jo efficient execution karta hai.
6️⃣ Results: Data ko visualize kar sakte hain ya kisi DB me store kar sakte hain.


Spark Execution Model
Transformations → Lazy execution hoti hai, jab tak action call na ho.
Actions → Actual computation hoti hai.
DAG Scheduler → Task ko optimize karta hai.
Task Execution → Executors tasks execute karte hain aur results bhejte hain.



*******

Delta Table Merge Kya Hai?
Merge is like ek smart operation jo Delta Table mein data ko update, insert, ya delete karne ka kaam ek saath karta hai. Matlab, agar tumhare paas ek purani Delta Table hai aur ek nayi data ka source hai (jaise koi file ya table), toh tum dono ko "merge" kar sakte ho – jahan chahiye wahan update karo, jahan naya data hai wahan add karo, aur jahan remove karna hai wahan delete karo

MERGE INTO students AS purani_table
USING nayi_data AS nayi_table
ON purani_table.id = nayi_table.id
WHEN MATCHED THEN 
  UPDATE SET purani_table.marks = nayi_table.marks
WHEN NOT MATCHED THEN 
  INSERT (id, marks) VALUES (nayi_table.id, nayi_table.marks);
*******
How to do soft delete of Incremental data using Merge Statement?

MERGE INTO target_table AS tgt
USING staging_table AS src
ON tgt.id = src.id  -- Matching condition based on primary key
WHEN MATCHED AND src.id IS NOT NULL THEN
    UPDATE SET 
        tgt.name = src.name,
        tgt.updated_at = CURRENT_TIMESTAMP,
        tgt.is_deleted = 0  -- Reactivate if it was marked deleted
WHEN NOT MATCHED THEN
    INSERT (id, name, created_at, is_deleted)
    VALUES (src.id, src.name, CURRENT_TIMESTAMP, 0)
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET 
        tgt.is_deleted = 1,  -- Soft delete records not in incremental data
        tgt.updated_at = CURRENT_TIMESTAMP;


If the record is in target_table but not in staging_table (NOT MATCHED BY SOURCE case):
Mark is_deleted = 1 (Soft delete).

**********
Slowly Changing Dimension Type 1 (SCD1) using MERGE in Delta Tables

SCD Type 1 (SCD1) overwrites existing records with new data, without maintaining historical records. Delta Tables in Databricks or Apache Spark support MERGE operations efficiently.
**********




