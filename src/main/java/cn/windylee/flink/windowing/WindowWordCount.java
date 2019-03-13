package cn.windylee.flink.windowing;

import cn.windylee.flink.wordcount.WordCount;
import cn.windylee.flink.wordcount.WordCountData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowWordCount {

    private static final Logger log = LoggerFactory.getLogger(WindowWordCount.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        DataStream<String> text;
        if(params.has("input")){
            text = env.readTextFile(params.get("input"));
        }else{
            log.warn("Executing WindowWordCount example with default input data set.");
            log.warn("Use --input to specify file input.");
            text = env.fromElements(WordCountData.WORDS);
        }
        int windowSize = params.getInt("window", 10);
        int slideSize = params.getInt("slide", 5);
        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new WordCount.Tokenizer())
                .keyBy(0)
                .countWindow(windowSize, slideSize)
                .sum(1);
        if(params.has("output")){
            counts.writeAsText(params.get("output"));
        }else{
            log.warn("Printing result to stdout. Use --output to specify output path.");
            counts.print().setParallelism(1);
        }
        env.execute("WindowWordCount");
    }

}
