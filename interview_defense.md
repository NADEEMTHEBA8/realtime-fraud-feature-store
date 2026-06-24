# Interview Defense: Real-Time Fraud Feature Store

As a Principal Staff Engineer, you must be prepared to defend the architecture, address trade-offs, and justify the specific technologies chosen for this portfolio project. Below are the 3 hardest technical questions a hiring manager might ask, along with pragmatic, senior-level answers.

---

### Question 1: "Why did you choose Spark Structured Streaming for ingestion instead of a lightweight alternative like Faust, Benthos, or a native Kafka Connector? Isn't Spark overkill for writing JSON to Parquet?"

**The Pragmatic Defense:**
"Yes, if the only requirement was dumping JSON strings to S3, Spark is heavyweight. A Kafka Connect S3 Sink or an AWS Firehose would be the standard, zero-code way to do that. However, I chose Spark Structured Streaming because it provides a scalable compute layer at the edge. By using Spark, I was able to implement real-time PII masking (SHA-256 hashing) *before* the data lands in the data lake, and gracefully split out a dead-letter queue for malformed JSON using Spark's `filter` API. While a Kafka Connect SMT (Single Message Transform) could technically hash the data, Spark provides a much richer programming model for adding complex validation or enrichment logic down the line without changing the architecture."

---

### Question 2: "You are using dbt against PostgreSQL to model the data, but then you are pulling the gold features out of Postgres and loading them into Redis for serving. If latency is the goal, why use a relational database at all instead of computing features directly in a stream processor like Flink?"

**The Pragmatic Defense:**
"It’s a trade-off between engineering velocity and ultra-low latency. Flink is the gold standard for real-time stateful aggregations (e.g., sliding windows), but it requires managing complex state, watermarks, and JVM deployments. For this architecture, I wanted to leverage dbt because it democratizes feature engineering. Data scientists and analysts can write SQL to define complex fraud features (like 7-day velocity or average ticket sizes) and rely on standard dbt testing for data quality and reconciliation. We run dbt in batch/micro-batch, materializing the features into Postgres, and then cache the final results in Redis for single-digit millisecond lookup latency at the API layer. If a specific feature requires sub-second freshness, we would introduce Flink for that specific feature, but the dbt-to-Redis batch pipeline is more than sufficient and highly maintainable for the vast majority of our historical aggregation features."

---

### Question 3: "You're capturing CDC from Postgres with Debezium and writing to Kafka, but your dbt models are reading the Postgres tables directly. Why implement CDC if you aren't using the streams to update your warehouse?"

**The Pragmatic Defense:**
"In this local prototype, dbt reads directly from Postgres because the operational database and the analytical warehouse share the same Postgres instance, making direct reads trivial. However, the Debezium CDC pipeline is implemented to prove out the enterprise architecture. In a real-world scenario, the operational DB (e.g., RDS) is completely decoupled from the data warehouse (e.g., Snowflake or BigQuery). By capturing changes in the WAL and publishing them to Kafka, we decouple the systems and prevent expensive analytical queries from impacting operational DB performance. The next step in this architecture would be wiring a Kafka Sink Connector to land those CDC topics into the target data warehouse, but the foundation—capturing zero-downtime updates at the source—is already solved and observable in the Kafka topics."
