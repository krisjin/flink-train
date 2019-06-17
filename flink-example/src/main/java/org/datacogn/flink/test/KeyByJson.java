package org.datacogn.flink.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByJson {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(2L, 32L), Tuple2.of(2L, 4L), Tuple2.of(3L, 6L), Tuple2.of(5L, 9L))
                .keyBy(0)//// 以数组的第一个元素作为key
                .map((MapFunction<Tuple2<Long, Long>, String>) t -> "key:" + t.f0 + ",value:" + t.f1)
                .print();

        env.execute("eef");
    }

}
