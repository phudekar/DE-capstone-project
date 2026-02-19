# Data Engineering Capstone Project

We want to build a monorepo to demonstrate the data engineering skills via Data Engineering Capstone Project

I want to impelment data pipeline in such a way that it has following phases in its workflow:

## 1. Data Sources & Ingestion:
I will be using a local console application which will generate dummy stock market trades. These trades / orders will be published on websockets.

I want to setup local Kafka cluster where this real time streaming data will be pushed after reading from websockets.

## 2. Processing Paradigms
Raw data will be pushed to Kafka from them it will be processed by Apache Flink and published on different Kafka topics based on the type of event for real time analysis

## 3. Compute & Execution Frameworks
In flight raw data will be processed via Apache Flink. DuckDB can be used for in memory processing. 

## 4. Storage & Data Modeling
Processed data should be stored on local iceburg cluster with parquet foramt for furhter consumption. The data will be modeled according to stock trade analysis with SCD 1, 2 and 3

## 5. Orchestration & Workflow Management
We will be using Dagster to manage orchastration of tasks to load and transform data with:
    - Dependency management
    - Retry logic and failure handling
    - Backfilling historical data
    - Idempotency
    - Scheduling strategies

## 6. Data Quality & Validation
Suggest a good framework to validate data quality and validation strategies using dagster. eg Great Expectations or Apache Deequ
Our object is to check following properties of stored data:
    - Completeness (null checks, missing values)
    - Accuracy (referential integrity, business rules)
    - Consistency (cross-source validation)
    - Timeliness (freshness SLAs)
    - Uniqueness (deduplication)

## 7. Data Governance & Metadata
Suggest good framework for data governance. eg. Data Catalog
Apache Atlas. OpenMetadata for tracking
    - Data Lineage 
    - Column-level lineage tracking
    - mpact analysis (upstream/downstream dependencies)
    - OpenLineage standard
    - Access Control
    - Role-based access (RBAC)
    - Row-level and column-level security
    - Data masking and anonymization
    - PII handling

## 8. Data Observability & Monitoring
Build data monitoring toolchain with Prometheus/Grafana to observe
    - Freshness — Is data arriving on time?
    - Volume — Are row counts within expected range?
    - Schema — Have columns changed unexpectedly?
    - Distribution — Are value distributions anomalous?
    - Lineage — Where did breakages originate?

## 9. Data Access & Serving Layer
Build API layer to access data and insights with GraphQL for flexible querying


## 10. Visualization & Consumption
Build sharable reports for business leadership based on open source reportin infrastructure similar to Tableu with some prebuild reports.

## 11. Infrastructure & DevOps for Data
Follow Infrastructure as Code pattern. Everything in our infrasturcutre should be build from ground up using version controlled scripts at any moment without loosing any data.
We can use:
    - Terraform, Pulumi for cloud resources
    - Containerization with Colima
    - Kubernetes for orchestration
    - Local CI/CD for Data Pipelines
    - Version control for pipeline code
    - Automated testing (unit, integration)
    - Blue-green deployments for pipelines
    - Environment promotion (dev → staging → prod)

## 12. Scalability & Performance
Consider Partitioning strategies (time-based, hash-based) suitable for vaious kinds of trade data, including Bucketing and clustering, Caching layers
Query optimization (predicate pushdown, partition pruning) and Cost-based optimization

## 13. Technical Considerations
All code will be tracked under a monorepo with proper categorization assuming different team focusing on different modules.

## Rubric
Dimension
Weight
Multiple source types & ingestion patterns
10%
Processing patterns (batch + streaming)
15%
Storage architecture & data modeling
10%
Orchestration & workflow design
10%
Data quality implementation
10%
Catalog, lineage & governance
10%
Observability & monitoring
10%
API/serving layer
10%
Documentation & data product thinking
10%
Code quality & CI/CD practices
5%


