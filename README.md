# Zero-Copy Migration to Iceberg Demo

This repository contains a small Apache Beam demo for showing how an existing
Parquet lake can be registered as an Apache Iceberg table without copying the
data files into a new table-owned data directory.

The core idea is simple:

1. Write Parquet files into a lake location.
2. Use Beam's Iceberg `AddFiles` transform to register those files in an
   Iceberg table.
3. Show that Iceberg metadata is created while the original Parquet files stay
   where they already are.

This makes the demo useful for explaining a "zero-copy" migration path: Iceberg
gets table metadata, snapshots, partition information, and catalog visibility,
but the data files are not rewritten or relocated as part of registration.

## Repository Contents

```text
src/main/java/org/apache/beam/demo/
  WriteParquet.java       Writes sample partitioned Parquet files.
  AddFilesBatch.java      Registers local Parquet files into a local Iceberg table.
  AddFilesStreaming.java  Watches GCS for Parquet files and registers them on Dataflow.

batch_registration.yaml      YAML version of a batch-style registration pipeline.
streaming_registration.yaml  YAML version of a streaming registration pipeline.
discover.sql                 Helper SQL for inspecting the Iceberg tables.
build.gradle                 Beam, Iceberg, Dataflow, and Hadoop dependencies.
```

## Demo Shape

There are two related demos in this repo:

1. **Batch demo:** everything runs locally.
2. **Streaming demo:** the registration pipeline runs on Dataflow and watches a
   GCS Parquet lake.

Both demos are meant to make the same point: the Parquet lake remains the source
of the data files, and Iceberg adds metadata on top of those files.

## Batch Demo

The batch demo is the simplest way to show zero-copy registration.

### 1. Write local Parquet files

Run `org.apache.beam.demo.WriteParquet`.

By default, `WriteParquet` writes 10 sample rows to:

```text
parquet_dir/
```

The files are partitioned by `age`, so the output should look conceptually like:

```text
parquet_dir/
  age=30/
    ...
  age=31/
    ...
```

### 2. Look at the local Parquet lake

At this point, the repo contains data files but no Iceberg table metadata yet.
This is the "before migration" view: a regular Parquet lake on disk.

### 3. Register the files with Iceberg

Run `org.apache.beam.demo.AddFilesBatch`.

This pipeline:

1. Matches files under the local `parquet_dir/`.
2. Converts the matched file metadata into file paths.
3. Passes those paths to Beam's Iceberg `AddFiles` transform.
4. Creates or updates the Iceberg table:

```text
my_namespace.my_table
```

The local demo uses a Hadoop Iceberg catalog with a warehouse at:

```text
iceberg_table/
```

### 4. Look at the Iceberg table metadata

After registration, inspect the Iceberg warehouse directory.

The key observation is that Iceberg metadata exists, but the original Parquet
files still live in `parquet_dir/`. The Iceberg table points at those files
instead of copying them into a new table `data/` directory.

That is the essential idea behind "zero-copy".

## Streaming Demo

The streaming demo shows the same registration idea with a continuously running
pipeline.

### 1. Start the streaming registration pipeline

Run `org.apache.beam.demo.AddFilesStreaming`.

This starts a Dataflow streaming job named:

```text
addfiles-streaming
```

The job continuously watches:

```text
gs://zero-copy-parquet-dir/**/*.parquet
```

and registers matching files into:

```text
my_namespace.my_table
```

using a BigLake REST Iceberg catalog backed by:

```text
gs://zero-copy-warehouse
```

The demo code currently uses:

```text
project: apache-beam-testing
region:  us-central1
```

Adjust those values in `AddFilesStreaming.java` if you are running the demo in a
different Google Cloud project or region.

### 2. Write Parquet files to GCS

Run `org.apache.beam.demo.WriteParquet` with the GCS destination enabled.

In the source file, the local destination is enabled by default:

```java
write(LOCAL, 10);
// write(GCS, 1000);
```

For the streaming demo, switch this so the pipeline writes to GCS:

```java
// write(LOCAL, 10);
write(GCS, 1000);
```

The GCS destination is:

```text
gs://zero-copy-parquet-dir/
```

### 3. Look at the GCS Parquet lake

Once `WriteParquet` finishes, inspect the GCS bucket and see the newly written
Parquet files. This is the same "plain Parquet lake" view as the local batch
demo, but stored in Cloud Storage.

### 4. Look at that Dataflow registers the files

The running `AddFilesStreaming` job should pick up the new Parquet files and
register them in the Iceberg table.

The important thing to see is the order of operations:

1. The streaming registration pipeline is already running.
2. New Parquet files land in GCS.
3. The pipeline discovers those files.
4. Iceberg metadata is updated to reference them.

Again, the data files remain in the Parquet lake. Iceberg registration adds table
metadata around the files rather than moving or rewriting them.

## YAML Pipeline Files

The repo also includes YAML versions of the registration workflows:

- `batch_registration.yaml`
- `streaming_registration.yaml`

These are useful for demonstrating the same idea through Beam YAML pipelines
instead of Java source. They both feed matched Parquet file paths into
`IcebergAddFiles`.

## Discovering the Tables

`discover.sql` contains example SQL snippets for creating Iceberg catalogs and
querying the registered tables.

Before using it, update any local paths, project IDs, warehouses, table names,
or catalog properties so they match the environment where you are running the
demo. In particular, the Java batch demo writes local Iceberg metadata under
`iceberg_table/`, so the local SQL catalog warehouse should point at that
location if you use the file as-is for the batch demo.

## Demo Takeaway

This repo is not trying to be a production migration tool. It is a presentation
demo that makes one behavior easy to see:

> Existing Parquet files can be registered into Iceberg metadata without copying
> the files into a new location.
