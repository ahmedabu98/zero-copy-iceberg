package org.apache.beam.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.iceberg.AddFiles;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class AddFilesBatch {
    static final String PROJECT_DIR = format("file://%s/github/zero-copy-iceberg/", System.getProperty("user.home"));
    static final String WAREHOUSE = PROJECT_DIR + "iceberg_table";
    static final Map<String, String> catalogProps = Map.of(
            "type", "hadoop",
            "warehouse", WAREHOUSE
    );
    static final String destinationTable = "my_namespace.my_table";
    static final List<String> partitionFields = Collections.singletonList("age");

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        p.apply(Create.of(PROJECT_DIR + "parquet_dir/**/*"))
                .apply(FileIO.matchAll())
                .apply(MapElements.into(strings()).via(metadata -> metadata.resourceId().toString()))
                .apply(new AddFiles(
                        IcebergCatalogConfig.builder().setCatalogProperties(catalogProps).build(),
                        destinationTable,
                        null,
                        partitionFields,
                        null,
                        null,
                        null,
                        null));
        p.run().waitUntilFinish();
    }
}
