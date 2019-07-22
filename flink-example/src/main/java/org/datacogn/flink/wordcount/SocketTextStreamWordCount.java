package org.datacogn.flink.wordcount;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.datacogn.flink.trigger.CustomTrigger;

/**
 * <pre>
 * flink run -c org.datacogn.flink.wordcount.SocketTextStreamWordCount flink-example-1.0-SNAPSHOT.jar 127.0.0.1 9000
 *
 *
 * nc -l 9000
 *
 * </pre>
 */
public class SocketTextStreamWordCount {

    public static void main(String[] args) throws Exception {
        //参数检查
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }

        String hostname = args[0];
        Integer port = Integer.parseInt(args[1]);

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);
        stream = stream.setParallelism(1);
        //计数
        stream.flatMap(new LineSplitter())
                .assignTimestampsAndWatermarks((new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Integer>>(Time.milliseconds(0)) {
                    public long extractTimestamp(Tuple2<String, Integer> element) {
                        return System.currentTimeMillis();
                    }
                })).keyBy(0)
                .timeWindow(Time.minutes(20))
                .trigger(new CustomTrigger())
                .aggregate(new WordAgg())
                .process(new ProcessFunction<Result, Object>() {
                    @Override
                    public void processElement(Result value, Context ctx, Collector<Object> out) throws Exception {
                        out.collect(JSONObject.toJSON(value));
                    }
                })
                .addSink(new PrintSinkFunction<>());

        env.execute("Java WordCount from SocketTextStream Example");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
