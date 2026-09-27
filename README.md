# Apache Fluss as the streaming layer of a lakehouse

<table align="center">
  <tr>
    <td align="center" width="110"><img src="docs/images/postgresql.svg" height="56" alt="PostgreSQL"><br><sub>PostgreSQL</sub></td>
    <td align="center" width="110"><img src="docs/images/flink.png" height="56" alt="Apache Flink"><br><sub>Flink</sub></td>
    <td align="center" width="110"><img src="docs/images/fluss.svg" height="56" alt="Apache Fluss"><br><sub>Fluss</sub></td>
    <td align="center" width="110"><img src="docs/images/iceberg.png" height="56" alt="Apache Iceberg"><br><sub>Iceberg</sub></td>
    <td align="center" width="110"><img src="docs/images/hive.svg" height="56" alt="Apache Hive"><br><sub>Hive</sub></td>
    <td align="center" width="110"><img src="docs/images/s3.svg" height="56" alt="Amazon S3"><br><sub>S3 (RustFS)</sub></td>
  </tr>
</table>

A demo of the use case [Apache Fluss](https://fluss.apache.org) was built for, which is keeping a
lakehouse table fresh while data keeps changing at the source. It runs on a laptop with Docker
Compose.

## The problem

Getting a stream of changes into an open table format such as Iceberg forces an uncomfortable
choice. Every Iceberg commit writes new data files, delete files and metadata, so committing
every few seconds leaves the table full of tiny files, piles up metadata and turns compaction
into a job of its own. With a CDC source the effect is worse, because every update and every
delete adds delete files that readers have to merge. Committing every few minutes keeps the table
healthy, but then the lake is minutes behind the source, and anything that needs fresh data
ends up reading somewhere else.

## How Fluss solves it

Fluss sits in front of the lake as a streaming storage layer. Changes land in a Fluss table,
which applies them in milliseconds and keeps the changelog. A tiering service copies them into
Iceberg in larger batches, with one commit per table every few minutes (30 seconds in this demo,
so there is something to watch). Readers that want fresh data query the table through Fluss,
and Fluss serves the latest Iceberg snapshot together with the changes that have not been tiered
yet, as if they were a single table. Fluss calls this a **union read**. The lake gets few and
large commits, and queries still see the source within seconds.

## What the demo does

A Python script keeps inserting, updating and deleting rows in PostgreSQL. Flink CDC captures
those changes and writes them into a Fluss primary-key table, and Fluss tiers the table into
Iceberg on S3-compatible storage, with a Hive Metastore as the catalog. The same query then runs
against the source, through Fluss and on Iceberg alone, so you can see how far behind the lake is
and how the union read closes the gap.

The setup favors being easy to read over being realistic, and it is not meant for production.
[This is not a production setup](#this-is-not-a-production-setup) explains what would change.

## Contents

- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Walkthrough](#walkthrough)
- [The data](#the-data)
- [Versions, and why these ones](#versions-and-why-these-ones)
- [Known limitations](#known-limitations)
- [This is not a production setup](#this-is-not-a-production-setup)
- [Repository layout](#repository-layout)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [References](#references)
- [License](#license)

## Architecture

```
  ┌───────────────┐   INSERT / UPDATE / DELETE    ┌──────────────────────────┐
  │ generator.py  │ ────────────────────────────▶ │ PostgreSQL 17            │
  │ ~5 ops/s      │                               │ demo.downloads           │
  └───────────────┘                               │ wal_level = logical      │
                                                  └────────────┬─────────────┘
                                                               │ logical replication
                                                               │ (pgoutput, publication flink_pub)
                                                               ▼
                                                  ┌──────────────────────────┐
                                                  │ Flink job "cdc"          │
                                                  │ Flink CDC source         │
                                                  │   ─▶ Fluss sink          │
                                                  └────────────┬─────────────┘
                                                               │ upserts and deletes
                                                               ▼
  ┌───────────────┐                               ┌──────────────────────────┐
  │ Flink SQL     │ ◀──────── union read ──────── │ Fluss 1.0                │
  │ client        │                               │ bronze.downloads         │
  └───────┬───────┘                               │ primary-key table        │
          │                                       └────────────┬─────────────┘
          │                                                    │ Flink job "fluss-tiering"
          │                                                    │ one commit every 30 s
          │                                                    ▼
          │                                       ┌──────────────────────────┐
          └────────────── union read ───────────▶ │ Iceberg bronze.downloads │
                                                  │ Parquet on RustFS (S3)   │
                                                  └────────────┬─────────────┘
                                                               │ table → metadata.json
                                                               ▼
                                                  ┌──────────────────────────┐
                                                  │ Hive Metastore 3.1       │
                                                  │ Iceberg catalog          │
                                                  └──────────────────────────┘
```

Each piece has a single job.

| Component | Role |
|---|---|
| **PostgreSQL** | The operational database we want to replicate. |
| **Flink CDC** | Reads PostgreSQL's logical replication stream. It embeds Debezium, so there is no Kafka or Kafka Connect in between. |
| **Fluss** | The hot storage layer. A primary-key table that applies each change in milliseconds and keeps the changelog. |
| **Fluss tiering service** | A Flink job that periodically copies what arrived in Fluss into Iceberg, one commit per table and round. |
| **Iceberg on RustFS** | The cold layer, in an open format on cheap storage, readable by any Iceberg engine. |
| **Hive Metastore** | The Iceberg catalog. It only stores, per table, a pointer to the current `metadata.json`. |
| **Flink SQL** | Where the queries run: union read through Fluss, or Iceberg only. |

### How the union read works

Every Iceberg commit made by the tiering service records how far it got in the Fluss log of each
bucket. A union read uses that point as the seam between the two layers.

```
  Fluss log of a bucket     ███████████████████████████████████████░░░░░░░░░░░░░░░░░░
                            └──────── already tiered to Iceberg ──┘└─ only in Fluss ┘
                                                                   ▲
                                                                   tiering frontier

  union read  =  read the latest Iceberg snapshot
               + replay the Fluss log from the frontier onwards
```

Because the seam is an exact offset, nothing is lost or counted twice. On a primary-key table, a
row that changed after the last commit is replaced by its newer version coming from Fluss.

### Where Kafka would fit

In a typical company, the change stream of a database would go through Kafka. Debezium or Flink
CDC would publish the changes to a topic, and Kafka would be the place where they meet the rest
of the company's streaming events, with any number of consumers reading them at their own pace.
Fluss would then be one more consumer.

This demo has a single source and a single consumer, so, to keep it small, Flink CDC writes
straight into Fluss. That is a shortcut for the demo and nothing more. Neither Flink CDC nor Fluss
can take over the work Kafka does as the backbone for events across a company.

## Quick start

You need Docker with Compose v2 and about **8 GB of memory for Docker**. The whole stack uses
around 6 GB once it is running, and the first build downloads a few GB of images and jars.

```bash
git clone <this-repo> && cd fluss-demo
docker compose up -d --build
```

The `jobs-init` container submits the two Flink jobs and exits. It checks what is already running
first, so running `docker compose up` again will not duplicate them. After a minute or so, run
the comparison script.

```bash
./scripts/compare.sh
```

```
tool        Postgres              Fluss union read      Iceberg only
Airflow       388  19:17:32.489    388  19:17:32.489    386  19:17:24.132
Cassandra     189  19:17:39.983    189  19:17:39.983    189  19:17:39.983
Flink         483  19:17:40.424    481  19:17:40.424    477  19:17:36.152
Kafka         664  19:17:42.038    662  19:17:42.038    660  19:17:36.354
Spark         746  19:17:43.247    746  19:17:43.247    744  19:17:33.300
...
```

That is the number of completed downloads per tool and the time of the last one, from the source,
through Fluss and from Iceberg alone. The union read stays within a few seconds of PostgreSQL
(the script queries PostgreSQL right after it), and Iceberg trails behind until the next commit.
If you stop the generator with `docker compose stop generator`, all three columns match in under
a minute.

Only the ports that are useful for poking around are published, always on the same number
inside and outside Docker.

| What | Where | Credentials |
|---|---|---|
| Flink web UI | http://localhost:8081 | none |
| RustFS console | http://localhost:9001/rustfs/console/ | `rustfsadmin` / `rustfsadmin` |
| RustFS S3 API | http://localhost:9000 | same, for any S3 client |
| PostgreSQL | `localhost:5432`, database `demo` | `demo` / `demo` |

All credentials live in [`.env`](.env). Fluss, ZooKeeper and the Hive Metastore are only reachable
from inside the Docker network. Fluss advertises its servers by container name, so a client on
the host could not follow those addresses anyway.

## Walkthrough

The steps below follow the data from the source to the queries, and all of them run against the
stack started above.

### 1. The source

The generator simulates people downloading Apache big data tools. Every 30 seconds it prints
the result of the same per-tool query that the rest of the demo uses.

```bash
docker compose logs -f generator
```

```
ops so far {'start': 64, 'finish': 69, 'purge': 8} | completed downloads per tool:
  Spark         783  last 2026-09-27 19:19:28
  Kafka         698  last 2026-09-27 19:19:26
  ...
```

[`postgres/init.sql`](postgres/init.sql) prepares PostgreSQL for CDC with a publication for the
table and `REPLICA IDENTITY FULL`, and the server runs with `wal_level=logical`. Once the CDC job
is running, it holds a replication slot.

```bash
docker compose exec source-db psql -U demo -d demo \
  -c "SELECT slot_name, plugin, active, confirmed_flush_lsn FROM pg_replication_slots"
```

`confirmed_flush_lsn` moves forward every 10 seconds, with each Flink checkpoint.

### 2. Into Fluss

Open http://localhost:8081 and you will find two jobs.

- **`cdc`** reads PostgreSQL with the CDC source and writes into Fluss. It is defined in
  [`flink/sql/pipeline.sql`](flink/sql/pipeline.sql).
- **`fluss-tiering`** is the tiering service, launched from
  [`flink/submit-jobs.sh`](flink/submit-jobs.sh).

Then open a SQL session. [`flink/sql/init.sql`](flink/sql/init.sql) has already registered the
Fluss catalog, the `bronze` database and an extra Iceberg catalog.

```bash
./scripts/flink-sql.sh
```

```sql
SHOW TABLES;   -- downloads
```

### 3. Into Iceberg

About every 30 seconds the tiering service commits to Iceberg, and you can watch it happen from
three places. Flink shows one row per commit.

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT committed_at, snapshot_id, operation FROM downloads$lake$snapshots;
```

The Hive Metastore only keeps a pointer to the current `metadata.json` of the table. Run this
twice, 30 seconds apart, and the pointer moves.

```bash
./scripts/hive.sh 'SHOW TBLPROPERTIES bronze.downloads("metadata_location")'
```

And in the [RustFS console](http://localhost:9001/rustfs/console/), the `fluss-demo` bucket holds
two folders.

```
fluss-demo/
├── datalakehouse/bronze.db/downloads/    Iceberg: data/ (Parquet and delete files), metadata/
└── fluss_remote_data/                    Fluss' own remote storage (log segments, KV snapshots)
```

### 4. The union read, live

Still in the SQL client, switch to the interactive result view and run the query in streaming
mode.

```sql
SET 'sql-client.execution.result-mode' = 'table';
SET 'execution.runtime-mode' = 'streaming';

SELECT tool, count(*) AS downloads, max(created_at) AS last_download
FROM downloads
WHERE status = 'completed'
GROUP BY tool;
```

You get one row per tool that updates in place (press `Q` to leave the view). Flink reads the
latest Iceberg snapshot first and then follows the Fluss changelog, so updates and deletes coming
from PostgreSQL show up here within seconds.

Compare it with the same query on Iceberg alone, where the numbers only change when a commit
lands.

```sql
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

-- Iceberg, through Fluss
SELECT tool, count(*) AS downloads, max(created_at) AS last_download
FROM downloads$lake WHERE status = 'completed' GROUP BY tool;

-- Iceberg, straight through the Hive Metastore, without Fluss
SELECT tool, count(*) AS downloads, max(created_at) AS last_download
FROM iceberg_catalog.bronze.downloads WHERE status = 'completed' GROUP BY tool;
```

The second query uses a plain Iceberg catalog that knows nothing about Fluss. It gives the same
result, which shows the lake is standard Iceberg that any engine connected to the same metastore
can read.

### 5. Two experiments

**Stop the tiering.** Cancel the `fluss-tiering` job from the Flink UI. Iceberg freezes, but the
union read keeps matching PostgreSQL, because it simply reads more from Fluss. Start it again with
`docker compose start jobs-init`, and on its next round a single commit brings Iceberg up to date.

**Stop the source.** Run `docker compose stop generator`. Within one tiering round, the three
columns of `./scripts/compare.sh` become identical. Bring it back with
`docker compose start generator`.

## The data

The source table is `demo.downloads`, with one row per download.

| Column | Type | Notes |
|---|---|---|
| `download_id` | `UUID` | Primary key. |
| `user_id` | `UUID` | 500 users who come back for more. |
| `ip`, `country` | `VARCHAR`, `CHAR(2)` | Fixed per user. |
| `tool`, `version` | `VARCHAR` | Spark, Kafka, Flink, Airflow, Hadoop, Iceberg, Hive, Cassandra, Druid, Pinot, Paimon or Fluss, with real version numbers. Weighted by popularity. |
| `os`, `channel` | `VARCHAR` | `linux`/`macos`/`windows`; `website`/`maven`/`docker`/`pypi`. |
| `status` | `VARCHAR` | `started`, `completed` or `failed`. |
| `size_bytes` | `BIGINT` | Close to the real artifact size of each tool. |
| `duration_ms` | `INT` | Empty while the download is running. |
| `created_at`, `updated_at` | `TIMESTAMP(3)` | |

Each download goes through a small lifecycle, which gives the CDC pipeline all three kinds of
change.

1. A user starts a download, which is an INSERT with status `started`.
2. The download completes (85% of the time) or fails, which is an UPDATE.
3. Failed downloads are cleaned up, which is a DELETE.

On first start, the generator seeds 1,000 finished downloads spread over the last seven days, so
the per-tool numbers mean something from the beginning. The rate, the number of users and the mix
of operations are set in [`generator/generator.env`](generator/generator.env).

## Versions, and why these ones

| Component | Version | Image |
|---|---|---|
| Apache Fluss | 1.0.0 | `apache/fluss:1.0.0` + Iceberg and Hive client jars |
| Apache Flink | 1.20.0 | `apache/fluss-quickstart-flink:1.20-1.0.0` + Hive client and Flink CDC |
| Flink CDC (Postgres connector) | 3.6.0 for Flink 1.20 | jar from Maven Central |
| Apache Iceberg | 1.10.1 | bundled with Fluss 1.0 |
| Hive Metastore | 3.1.3 (Derby) | `apache/hive:3.1.3`, amd64 only |
| Hive client used by Iceberg | 2.3.10, with Hadoop client 3.3.6 | jars from Maven Central |
| PostgreSQL | 17 | `postgres:17` |
| RustFS | 1.0.0 | `rustfs/rustfs:1.0.0` |
| ZooKeeper | 3.9.2 | `zookeeper:3.9.2` |

Most of these versions are not arbitrary. Some come from what Fluss 1.0 supports, and some from
problems I ran into while building the demo.

**Fluss 1.0 and Iceberg 1.10.1.** Fluss 1.0 tiers primary-key tables into Iceberg with
merge-on-read (equality and position delete files), so CDC updates and deletes work end to end.
Its Iceberg integration is built against Iceberg 1.10.1, and every Iceberg jar added on the Fluss
and Flink side has to match that version.

**Flink 1.20.0.** It is the Flink version inside the Flink image published by the Fluss project,
which already contains the Fluss connector and the Iceberg jars. The Flink CDC connector is the
3.6.0 build for Flink 1.20.

**Hive Metastore 3.1.3, not 4.x.** Iceberg 1.10 talks to the metastore with a Hive 2.3 client,
which works with HMS 2.x and 3.x. HMS 4.x changed the Thrift API and fails with
`Invalid method name: 'get_table'`, as the
[Fluss Hive guide](https://fluss.apache.org/docs/streaming-lakehouse/datalake-catalogs/hive/)
warns. The official `apache/hive:3.1.3` image is only published for amd64, so on Apple Silicon it
runs emulated. That is fine here, since the metastore only gets a couple of calls per commit.

The same guide lists the jars that Fluss and Flink need to use a Hive catalog
(`iceberg-hive-metastore`, `hive-exec` 2.3.10, `hadoop-client-api`/`-runtime` 3.3.6 and a few
helpers). Both Dockerfiles download exactly that list.

**The metastore needs S3A.** The metastore creates the database and table directories itself, so
it has to reach RustFS. The image already ships `hadoop-aws`, which only needs to be added to the
classpath, and a `core-site.xml` with the endpoint. Hadoop 3.1 has no filesystem registered for
the `s3://` scheme that Iceberg uses, hence the `fs.s3.impl` mapping to S3A.

**RustFS 1.0.0, not the alpha from the Fluss quickstart.** The official Fluss quickstart pins
`rustfs/rustfs:1.0.0-alpha.83`. That build answers `400 Bad Request` instead of `404` to a
`HEAD` on a key that ends in `/`. Modern S3 clients never do that, but Hadoop's S3A does it to
detect directories, so creating the Iceberg database through the metastore failed. RustFS 1.0.0
returns `404` and works. The console also moved to `/rustfs/console/` in that release.

**PostgreSQL 17 with `pgoutput`.** Logical decoding needs `wal_level=logical`. The Flink CDC
connector defaults to the `decoderbufs` plugin, which does not ship with PostgreSQL, so the
pipeline sets `pgoutput`, which is built in. `pgoutput` only sends tables that belong to a
publication, which is why `init.sql` creates `flink_pub`.

**No Trino.** Trino can read the Iceberg tables, but Fluss 1.0 has no Trino connector, so Trino
cannot do a union read. Only Flink and Spark can. Since the demo is about the union read,
everything is queried from Flink. The reason there is no Kafka is explained in
[Where Kafka would fit](#where-kafka-would-fit).

## Known limitations

These are limits of the versions above, not of the demo setup. I checked each of them against the
code or reproduced it.

- **Union read only works from Flink and Spark.** Trino and StarRocks can only read the Iceberg
  side through their own connectors
  ([engine support](https://fluss.apache.org/docs/streaming-lakehouse/union-read/)).
- **Batch union read fails on primary-key tables stored in Iceberg**, with
  `UnsupportedOperationException: lake records must instance of sorted view`. To merge the lake
  data with the Fluss changelog in batch mode, the lake reader has to return rows sorted by key.
  The Paimon and Hudi readers do that, and the Iceberg reader does not
  ([`LakeSnapshotAndLogSplitScanner`](https://github.com/apache/fluss/blob/v1.0.0/fluss-client/src/main/java/org/apache/fluss/client/table/scanner/batch/LakeSnapshotAndLogSplitScanner.java)).
  Streaming mode works, because it reads the snapshot and then replays the log, and that is what
  the demo uses. Batch mode works for log (append-only) tables, and `$lake` queries work in batch.
- **No compaction from Fluss for the Iceberg tables it creates.** `table.datalake.auto-compaction`
  only applies to tables created by older Fluss versions. Each commit on a primary-key table adds
  data files and delete files, so for a long-running setup you need an external engine to
  compact them (Spark's `rewrite_data_files`, Trino's `optimize` or Iceberg's Java actions).

## This is not a production setup

Everything here is tuned so that a laptop can run it and a person can follow it, which is the
opposite of what a real deployment needs.

- **Buckets and tiering interval.** The table has a single bucket (`bucket.num = 1`) and is tiered
  every 30 seconds, so that there is one Iceberg partition to look at and a commit to watch every
  half minute. In a real environment the number of buckets depends on the write throughput and the
  parallelism you need, and the tiering interval is a trade-off between lake freshness and file
  size that usually lands in minutes (Fluss defaults to 3). The 10-second Flink checkpoint
  interval was picked for the same reason.
- **Credentials.** [`.env`](.env) is committed so that the demo works right after cloning. Real
  credentials never go into a repository, and they would come from a secret manager instead.
- **Single instances everywhere.** There is one Fluss tablet server with a replication factor of 1,
  one ZooKeeper node, one Flink JobManager without high availability, and Flink checkpoints are
  kept in the JobManager's memory.
- **Throwaway state.** The Hive Metastore keeps its catalog in an embedded Derby database inside
  its container, and Fluss keeps its local data inside the tablet server's container. Only RustFS
  has a volume, and `docker compose down -v` removes it too.
- **No security.** There is no TLS and no authentication between the services.

## Repository layout

Each folder holds everything its containers need. [`scripts/`](scripts) holds what you run from
your own machine.

```
.
├── docker-compose.yml     services, ports, volumes, dependencies
├── .env                   every credential of the demo
├── postgres/              source database: table, replica identity, publication
├── generator/             the Python data generator and its settings
├── rustfs/                S3 storage settings and the script that creates the bucket
├── metastore/             Hive Metastore image with S3A, and its core-site.xml
├── fluss/                 Fluss image (Iceberg + Hive client jars) and server.yaml
├── flink/                 Flink image, per-role settings, job submission, SQL files
│   └── sql/
│       ├── init.sql       catalogs registered in every SQL session
│       └── pipeline.sql   Fluss table, CDC source and the INSERT that runs as job "cdc"
├── scripts/
│   ├── flink-sql.sh       Flink SQL client: interactive, or pass statements to run
│   ├── hive.sh            Hive CLI against the metastore
│   └── compare.sh         the per-tool query in PostgreSQL, union read and Iceberg only
└── docs/images/           logos used in this README
```

Configuration is split so that `docker-compose.yml` only describes the structure.

- **[`.env`](.env)** holds the credentials, and Compose loads it automatically.
- **`<folder>/<service>.env`** holds the settings of each service. It references the credentials
  it needs with `${...}`, so every container only gets the ones it uses.
- **Configuration files** pick credentials up from the environment. `fluss/server.yaml` uses
  `${VAR}`, which the Fluss image replaces with `envsubst` at startup. `metastore/core-site.xml`
  uses `${env.VAR}`, which Hadoop resolves. The `.sql` files use `${VAR}`, and
  `flink/sql-client.sh` fills them in before starting the client, because Flink SQL does not read
  environment variables.

These are the settings you are most likely to play with.

| Setting | Where | Default |
|---|---|---|
| Iceberg commit interval | `table.datalake.freshness` in [`flink/sql/pipeline.sql`](flink/sql/pipeline.sql) | `30s` |
| Flink checkpoint interval | `execution.checkpointing.interval`, same file | `10s` |
| Generator rate and operation mix | [`generator/generator.env`](generator/generator.env) | 5 ops/s |

## Troubleshooting

**A port is already in use.** The demo needs 5432, 8081, 9000 and 9001 on the host. A local
PostgreSQL on 5432 is the usual suspect.

**Something is off and you want a clean slate.** Remove every container and volume, then start
again.

```bash
docker compose --profile client down -v
docker compose up -d
```

**A job is missing from the Flink UI.** Look at what `jobs-init` did, and run it again.

```bash
docker compose logs jobs-init
docker compose start jobs-init
```

**The metastore is slow to start on a Mac.** It runs under amd64 emulation. Make sure Rosetta is
enabled in Docker Desktop (Settings → General → "Use Rosetta for x86_64/amd64 emulation").
`./scripts/hive.sh` also takes a few seconds per call for the same reason.

## Stopping

```bash
docker compose stop                        # pause, keeping the state (resume with: docker compose start)
docker compose --profile client down -v    # remove everything
```

## Contributing

Pull requests that improve the repo are welcome, whether they fix something, make an explanation
clearer or move the demo to newer versions. For bigger changes, opening an issue first to talk it
through saves everyone time.

## References

- [Apache Fluss documentation](https://fluss.apache.org/docs/)
- [Fluss lakehouse quickstart](https://fluss.apache.org/docs/quickstart/lakehouse/)
- [Fluss union read](https://fluss.apache.org/docs/streaming-lakehouse/union-read/)
- [Fluss tiering service](https://fluss.apache.org/docs/streaming-lakehouse/tiering-service/)
- [Fluss with Iceberg](https://fluss.apache.org/docs/streaming-lakehouse/datalake-formats/iceberg/)
- [Fluss with a Hive Metastore](https://fluss.apache.org/docs/streaming-lakehouse/datalake-catalogs/hive/)
- [Flink CDC Postgres connector](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/flink-sources/postgres-cdc/)
- [PostgreSQL publications](https://www.postgresql.org/docs/17/logical-replication-publication.html)
- [RustFS](https://github.com/rustfs/rustfs)

## License

This project is licensed under the [Apache License 2.0](LICENSE).

---

<sub>Apache Fluss, Apache Flink, Apache Iceberg, Apache Hive and their logos are trademarks of the
[Apache Software Foundation](https://www.apache.org/foundation/marks/). Amazon S3 is a trademark of
Amazon.com, Inc., and the PostgreSQL logo belongs to its owners. They are used here only to identify each
project. The demo uses RustFS, an S3-compatible store, not Amazon S3.</sub>
