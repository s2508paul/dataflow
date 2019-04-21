package com.proj.sample;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.schemas.Schema;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import java.util.stream.Collectors;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.*;


public class JoinGrp {


    public static final Schema SCHEMA = Schema.builder()
            .addInt64Field("id")
            .addStringField("name")
            .addDoubleField("marks")
            .build();

    public static class RowToString extends DoFn<Row, String>{
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element().getValues()
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(","));


         /*List l = c.element().getValues();
            System.out.println("size="+l.size());
           */ 
            c.output(line);
        }

    }

    public static class StringToRow extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] vals = c.element().split(",");
            Row appRow = Row
                    .withSchema(SCHEMA)
                    .addValues(Long.valueOf(vals[0]),vals[1],Double.valueOf(vals[2]))
                    .build();
            c.output(appRow);

        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);

        String input = "gs://sumit-test-bucket-2508/input/a2.txt";
        //String outputPrefix = "/Users/tkmacgf/Desktop/to_tar/output/hello";
        PCollection<String> lines = p.apply("readRow", TextIO.read().from(input));
        PCollection<Row> rw = lines.apply("transform_to_row", ParDo.of(new StringToRow())).setRowSchema(SCHEMA);

	PCollectionTuple my_tpl = PCollectionTuple.of(new TupleTag<>("tbl"), rw);

        PCollection<Row> new_rw = my_tpl.apply("transform_sql", SqlTransform.query(
                "SELECT id,count(*) FROM tbl group by id"));

        PCollection<String> z = new_rw.apply("transform_to_string", ParDo.of(new RowToString()));

        z.apply("write_to_gcs", TextIO.write().to("gs://sumit-test-bucket-2508/output/tbl").withSuffix(".txt").withoutSharding());
        p.run();



    }
}
