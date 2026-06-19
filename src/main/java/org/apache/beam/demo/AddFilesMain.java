package org.apache.beam.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.iceberg.AddFiles;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.LogElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.joda.time.Duration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class AddFilesMain {
    static final String PROJECT_DIR = format("file://%s/github/zero-copy-iceberg/", System.getProperty("user.home"));
    static final String WAREHOUSE = PROJECT_DIR + "warehouse";
    static final Map<String, String> catalogProps = Map.of(
            "type", "hadoop",
            "warehouse", WAREHOUSE
    );
    static final String destinationTable = "my_namespace.my_table_3";
    static final List<String> partitionFields = Collections.singletonList("age");

    public static void main(String[] args) {
        Catalog catalog = IcebergCatalogConfig.builder().setCatalogProperties(Map.of("type", "hadoop",
                "warehouse", WAREHOUSE)).build().catalog();

        TableIdentifier identifier = TableIdentifier.parse("my_namespace.my_table_4");
        Schema schema = new Schema(Types.NestedField.optional(1, "name", Types.StringType.get()));

        Table table = catalog
                .createTable(identifier, schema);
//                .buildTable(identifier, schema)
//                .create();

        System.out.println(table.name());
    }

    public static void main2(String[] args) {
        Pipeline p = Pipeline.create();
        System.out.println("starting: ");
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
