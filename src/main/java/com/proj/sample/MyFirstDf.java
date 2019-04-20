package com.proj.sample;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


public class MyFirstDf {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline p = Pipeline.create(options);

        String input = "gs://sumit-test-bucket-2508/input/a1.txt";
        String outputPrefix = "gs://sumit-test-bucket-2508/output/b";
        final String searchTerm = "hello";


        PCollection<String> lines = p.apply("GetJava", TextIO.read().from(input)); //1st arg is the name of transform,2nd op
        PCollection<String> filtered_lines = lines.apply("Grep", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {
                String line = c.element();
                if (line.contains(searchTerm)) {
                    c.output(line);
                }
            }
        }));
        filtered_lines.apply(TextIO.write().to(outputPrefix).withSuffix(".txt").withoutSharding());

        p.run();
    }

}
