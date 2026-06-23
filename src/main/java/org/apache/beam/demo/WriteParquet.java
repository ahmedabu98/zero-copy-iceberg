package org.apache.beam.demo;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.UUID;

import static java.lang.String.format;
import static org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.toAvroSchema;
import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;

public class WriteParquet {
    static final Schema BEAM_SCHEMA =
            Schema.builder().addInt64Field("id").addStringField("name").addInt64Field("age").build();
    static final String LOCAL = "parquet_dir/";
    static final String GCS = "gs://zero-copy-parquet-dir/";


    public static void main(String[] args) {
        write(LOCAL, 10);
//        write(GCS, 1000);
    }

    public static void write(String destination, long to) {
        Pipeline p = Pipeline.create();
        String prefix = UUID.randomUUID().toString();

        p.apply(GenerateSequence.from(0).to(to))
                .apply(MapElements.into(TypeDescriptors.rows())
                        .via(l -> Row.withSchema(BEAM_SCHEMA)
                                .addValues(l, "name_" + l, (l % 10) + 30)
                                .build()))
                .setRowSchema(BEAM_SCHEMA)
                .apply(Convert.to(GenericRecord.class))
                .apply(
                        FileIO.<Long, GenericRecord>writeDynamic()
                                .by(record -> (long) record.get("age"))
                                .withDestinationCoder(VarLongCoder.of())
                                .via(ParquetIO.sink(toAvroSchema(BEAM_SCHEMA)))
                                .to(destination)
                                .withNaming(age -> defaultNaming(format("age=%s/%s", age, prefix), ".parquet"))
                                .withNumShards(2));

        p.run().waitUntilFinish();
    }
}