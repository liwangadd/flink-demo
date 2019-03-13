package cn.windylee.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {

    private static final Logger log = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<String> text;
        env.getConfig().setGlobalJobParameters(params);
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            log.warn("Executing WordCount example with default input data set.");
            log.warn("Use --input to specify file input.");
            text = env.fromElements(WordCountData.WORDS);
        }
        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0).sum(1);
        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            log.warn("Printing result to stdout. Use --output to specify output path.");
            counts.print().setParallelism(1);
        }
        env.execute("Streaming WordCount");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.toLowerCase().split("\\W+");
            for (String word : words) {
                if (word.length() > 0)
                    collector.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
