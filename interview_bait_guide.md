# The Interview Bait Guide (Cheat Sheet)

Use this guide to perfectly defend the 3 architectural baits we planted in the repository.

---

## Bait 1: The README Trade-off (Kafka Partitioning)
**Location:** `README.md` (Under "Architectural Trade-offs & Known Limitations")
**The Trap:** You deliberately pointed out that your Kafka topic only has 1 partition, which prevents consumer scaling.
**The Guaranteed Question:** *"I see you only have one Kafka partition for `transactions.raw`. If our traffic suddenly spiked by 10x, how would you redesign this pipeline to scale while still maintaining the event ordering you mentioned?"*

**The Senior-Level Defense:**
> "Right now, a single partition ensures total global ordering, which is great for a prototype. To scale for a 10x spike, we absolutely have to increase the partition count. The trade-off is we lose *global* ordering. To fix this, I would ensure the Kafka Producer uses the `user_id` as the partition key. This guarantees that all transactions for a specific user land in the same partition, providing *partial* ordering (per-user ordering). Since our fraud features are calculated per user (e.g., user velocity over 7 days), we don't care about the global order of events between User A and User B. We only care that User A's events are processed sequentially. This allows us to scale out Spark consumer tasks horizontally across partitions without breaking the fraud logic."

---

## Bait 2: The ADR (Batch vs. Real-Time Features)
**Location:** `docs/ADRs/001-batch-dbt-vs-streaming-flink-for-feature-aggregation.md`
**The Trap:** You documented your choice to use dbt (batch) over Flink (streaming), explicitly calling out that you sacrificed feature freshness.
**The Guaranteed Question:** *"You mentioned in your ADR that you sacrificed sub-second feature freshness to use dbt. What happens if a fraudster makes 5 rapid transactions in 2 minutes? Because your pipeline runs every 4 hours, wouldn't the model miss that velocity spike and let the fraud through?"*

**The Senior-Level Defense:**
> "That’s the exact trade-off. For historical features (like average ticket size over 30 days), a 4-hour batch delay is perfectly fine. But you're right—for real-time velocity attacks, batch is too slow. To solve this without ripping out dbt, I would implement a Lambda Architecture or a 'Dual Write' feature pattern. We keep the dbt pipeline for the heavy historical aggregations. But for the ultra-low-latency 'velocity' counters, we would either add a Flink job specifically just for those real-time counters, or we would increment a Redis counter directly from our fast-path FastAPI service when a transaction hits. The model then queries Redis and combines the real-time velocity counter with the 4-hour stale historical features. It gives us the best of both worlds: low operational complexity for most features, and low latency where it actually matters."

---

## Bait 3: The Inline TODO (The S3 Small File Problem)
**Location:** `streaming/spark/src/bronze_ingest.py` (Inside `write_bronze_to_minio`)
**The Trap:** You highlighted that 30-second micro-batches create a "Small File Problem" in S3/MinIO.
**The Guaranteed Question:** *"I noticed your TODO about the S3 small file problem degrading downstream dbt performance. Why does having lots of small Parquet files hurt Postgres/dbt, and how exactly would you build that compaction job you mentioned?"*

**The Senior-Level Defense:**
> "Having thousands of tiny files destroys read performance because the overhead of opening files and reading Parquet metadata footers starts to take longer than reading the actual data. If we load that into Postgres, the I/O bottleneck happens at the MinIO/S3 API layer. To fix it, we have two options. The simple option is to dynamically increase the Spark trigger interval (e.g., to 5 minutes) so it naturally writes larger files. The enterprise option is a Compaction Job. I would write a nightly PySpark or AWS Glue job that reads the previous day's partition (e.g., `event_date=2023-10-01`), uses `.coalesce()` to shuffle the data into a few large files, overwrites the partition, and cleans up the tiny files. This keeps ingestion fast but optimizes the storage for heavy analytical reads."
