package org.datacogn.flink.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class KeyByJson {


    public static void main(String[] args) throws Exception {
        test3(null);

    }


    public static void test1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(2L, 32L), Tuple2.of(2L, 4L), Tuple2.of(3L, 6L), Tuple2.of(5L, 9L))
                .keyBy(0)//// 以数组的第一个元素作为key
                .map((MapFunction<Tuple2<Long, Long>, String>) t -> "key:" + t.f0 + ",value:" + t.f1)
                .print();

        env.execute("eef");

    }


    /**
     * reduce表示将数据合并成一个新的数据，返回单个的结果值，并且 reduce 操作每处理一个元素总是创建一个新值。而且reduce方法不能直接应用于SingleOutputStreamOperator对象，也好理解，因为这个对象是个无限的流，对无限的数据做合并，没有任何意义哈！
     * 所以reduce需要针对分组或者一个window(窗口)来执行，也就是分别对应于keyBy、window/timeWindow 处理后的数据，根据ReduceFunction将元素与上一个reduce后的结果合并，产出合并之后的结果。
     *
     * @param args
     * @throws Exception
     */
    public static void test2(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0) // 以数组的第一个元素作为key
                .reduce((ReduceFunction<Tuple2<Long, Long>>) (t2, t1) -> new Tuple2<>(t1.f0 + t2.f0, t2.f1 + t1.f1)) // value做累加
                .print();

        env.execute("execute");
    }


    public static void test3(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream keyedStream = env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L), Tuple2.of(2L, 9L))
                .keyBy(0);// 以数组的第一个元素作为key


//        SingleOutputStreamOperator<Tuple2> sumStream = keyedStream.sum(0);
        SingleOutputStreamOperator<Tuple2> sumStream = keyedStream.minBy(1);
        sumStream.addSink(new PrintSinkFunction());

        env.execute("Test Job");
    }
}
