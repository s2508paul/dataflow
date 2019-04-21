package com.proj.sample;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import java.util.stream.Collectors;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.*;

public class MaxRev {

    public static final Schema ORDER = Schema.builder()
            .addStringField("order_id")
            .addStringField("cust_id")
            .addStringField("prod_id")
            .addInt64Field("qty")
            .build();

    public static final Schema PRODUCT = Schema.builder()
            .addStringField("prod_id")
            .addStringField("prod_name")
            .addDoubleField("price")
            .addStringField("catagory")
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

    public static class OrderStringToRow extends DoFn<String, Row> {


        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] o_vals = c.element().split(",");
            Row orderRow = Row
                    .withSchema(ORDER)
                    .addValues(o_vals[0],o_vals[1],o_vals[2],Long.valueOf(o_vals[3]))
                    .build();
            c.output(orderRow);

        }
    }

    public static class ProductStringToRow extends DoFn<String, Row> {


        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] p_vals = c.element().split(",");
            Row productRow = Row
                    .withSchema(PRODUCT)
                    .addValues(p_vals[0],p_vals[1],Double.valueOf(p_vals[2]),p_vals[3])
                    .build();
            c.output(productRow);

        }
    }



    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);


        String out_path = "gs://sumit-test-bucket-2508/output/tbl";

        PCollection<String> order_lines = p.apply("readOrder", TextIO.read().from("gs://sumit-test-bucket-2508/input/order.csv"));
        PCollection<String> prod_lines = p.apply("readProduct", TextIO.read().from("gs://sumit-test-bucket-2508/input/product.csv"));

        PCollection<Row> order_row = order_lines.apply("order_str_to_row", ParDo.of(new MaxRev.OrderStringToRow())).setRowSchema(ORDER);
        PCollection<Row> product_row = prod_lines.apply("prod_str_to_row", ParDo.of(new MaxRev.ProductStringToRow())).setRowSchema(PRODUCT);


        PCollectionTuple my_tpl = PCollectionTuple.of(new TupleTag<>("order_tbl"),order_row )
                .and(new TupleTag<>("prod_tbl"), product_row);

        PCollection<Row> join_rw = my_tpl.apply("transform_sql", SqlTransform.query(
                "SELECT order_tbl.cust_id as id,SUM(order_tbl.qty * prod_tbl.price) as rev FROM order_tbl INNER JOIN prod_tbl ON order_tbl.prod_id = prod_tbl.prod_id group by order_tbl.cust_id order by rev desc LIMIT 100"));

        //PCollection<Row> grp_rw = join_rw.apply("transform_sql", SqlTransform.query(
          //      "SELECT id,SUM(rev) as tol_rev FROM PCOLLECTION group by id order by tol_rev dec"));

        PCollection<String> z = join_rw.apply("transform_to_string", ParDo.of(new MaxRev.RowToString()));

        z.apply("write_to_gcs", TextIO.write().to("gs://sumit-test-bucket-2508/output/jn").withSuffix(".txt").withoutSharding());
        p.run();



    }



}
