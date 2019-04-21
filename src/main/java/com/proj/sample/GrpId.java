package com.proj.sample;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class GrpId {

    public static final void main(String args[]) throws Exception {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);

        PCollection<String> lines = p.apply("readInput", TextIO.read().from("gs://sumit-test-bucket-2508/input/order.csv"));

        PCollection<KV<String, String>> stringToKv =
                lines.apply(
                        "StringToKV",
                        MapElements.via(
                                new SimpleFunction<String, KV<String, String>>() {

                                    @Override
                                    public KV<String, String> apply(String str) {
                                        String[] split = str.split(",");

                                        String key = split[2];
                                        String value = split[1];
                                        return KV.of(key, value);
                                    }
                                }));

        PCollection<KV<String, Iterable<String>>> collection_id = stringToKv.apply(GroupByKey.<String, String>create());

        PCollection<String> sumUpValuesByKey =
                collection_id.apply(
                        "GroupByItem_Id",
                        ParDo.of(
                                new DoFn<KV<String, Iterable<String>>, String>() {

                                    @ProcessElement
                                    public void processElement(ProcessContext context) {
                                        String collect_id = "{";
                                        String itm_id = context.element().getKey();
                                        Iterable<String> cust_id_list = context.element().getValue();
                                        for (String cust_id : cust_id_list) {
                                            collect_id+=cust_id + ",";
                                        }
                                        collect_id = collect_id.replace(collect_id.substring(collect_id.length()-1), "}");

                                        context.output(itm_id + "," + collect_id);
                                    }
                                }));

        sumUpValuesByKey.apply(TextIO.write().to("gs://sumit-test-bucket-2508/output/collect_id").withSuffix(".txt").withoutSharding());

        p.run();
    }
}
