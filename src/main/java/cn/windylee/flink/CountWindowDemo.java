package cn.windylee.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountWindowDemo {

    private final static Logger LOG = LoggerFactory.getLogger(CountWindowDemo.class);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> textStream;
        if (params.has("input")) {
            textStream = env.readTextFile(params.get("input"));
        } else {
            LOG.info("Executing CountWindowDemo example with default input data set.");
            LOG.info("Use --input to specify file input.");
            textStream = env.fromElements(IntegerData.WORDS);
        }

        env.getConfig().setGlobalJobParameters(params);
        // 设置并行度为1，使每次运行结果相同，便于观察
        env.setParallelism(1);
        DataStream<Long> resStream = textStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                // 将文本信息按空白字符进行切分
                String[] tokens = s.toLowerCase().split("\\W+");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(token);
                    }
                }
            }

        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                // 过滤非数字类型
                for (int i = s.length(); --i >= 0; ) {
                    if (!Character.isDigit(s.charAt(i))) {
                        return false;
                    }
                }
                return true;
            }
        }).map(new MapFunction<String, Long>() {
            @Override
            public Long map(String s) throws Exception {
                // 将字符串类型转化为长整形
                return Long.parseLong(s);
            }
        }).windowAll(GlobalWindows.create()) // 设置windowAssigner为GlobalWindows类型，自定义trigger进行触发
                .trigger(MyCountTrigger.of(100, 500)) // 窗口内元素大于100或者窗口内元素和大于500，触发窗口计算
                .sum(0);

        if (params.has("output")) {
            // 将结果信息输出到指定文件
            LOG.info("Printing result to stdout. Use --output to specify output path.");
            resStream.writeAsText(params.get("output"));
        } else {
            // 若没有指定输出文件，打印到控制台
            resStream.print();
        }
        env.execute();

    }


}
