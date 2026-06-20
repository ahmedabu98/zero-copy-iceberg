package org.apache.beam.demo;

import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.iceberg.AddFiles;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Watch;
import org.joda.time.Duration;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class AddFilesStreaming {
    static final String PROJECT = "apache-beam-testing";
    static final String PARQUET_PATTERN = "gs://zero-copy-parquet-dir/**/*.parquet";
    static final String WAREHOUSE = "gs://zero-copy-warehouse";
    static final Map<String, String> catalogProps = Map.of(
            "type", "rest",
            "uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog",
            "warehouse", WAREHOUSE,
            "header.x-goog-user-project", PROJECT,
            "rest.auth.type", "google",
            "io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO",
            "header.X-Iceberg-Access-Delegation", "vended-credentials");
    static final String destinationTable = "my_namespace.my_table";
    static final List<String> partitionFields = Collections.singletonList("age");

    public static void main(String[] args) {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject(PROJECT);
        options.setRegion("us-central1");
        options.setStreaming(true);
        options.setJobName("addfiles-streaming");

        Pipeline p = Pipeline.create(options);
        p
                .apply(Create.of(PARQUET_PATTERN))
                .apply(FileIO.matchAll().continuously(Duration.standardSeconds(1), Watch.Growth.never()))
                .apply(MapElements.into(strings()).via(metadata -> metadata.resourceId().toString()))
                .apply(new AddFiles(
                        IcebergCatalogConfig.builder().setCatalogProperties(catalogProps).build(),
                        destinationTable,
                        null,
                        partitionFields,
                        null,
                        null,
                        null,
                        Duration.standardSeconds(2)));
        DataflowPipelineJob job = (DataflowPipelineJob) p.run();
        System.out.println(job.getJobId());
    }
}
