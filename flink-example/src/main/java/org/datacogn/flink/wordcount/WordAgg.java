package org.datacogn.flink.wordcount;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User:shijingui
 * Date:2019-07-22
 */
public class WordAgg implements AggregateFunction<Tuple2<String, Integer>, Acc, Result> {

    private volatile AtomicInteger cout = new AtomicInteger(0);

    @Override
    public Acc createAccumulator() {
        return new Acc();
    }

    @Override
    public Acc add(Tuple2<String, Integer> value, Acc accumulator) {


        Map<String, Integer> countMap = accumulator.getCount();
        Integer oldVal = countMap.get(value.f0);

        if (oldVal == null) countMap.put(value.f0, 1);
        else countMap.put(value.f0, oldVal + 1);

        return accumulator;
    }

    @Override
    public Result getResult(Acc accumulator) {
        if (accumulator != null) {
            Result result = new Result();

            result.setCount(accumulator.getCount());

            return result;
        }
        System.out.println(cout.getAndIncrement());

        return null;
    }

    @Override
    public Acc merge(Acc a, Acc b) {
        return null;
    }
}
