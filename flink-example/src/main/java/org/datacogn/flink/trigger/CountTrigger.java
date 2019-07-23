package org.datacogn.flink.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * User:shijingui
 * Date:2019-07-22
 */
public class CountTrigger extends Trigger<Object, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private CountTrigger(int count) {
        this.threshold = count;
    }

    private int count = 0;
    private int threshold;
    private final ReducingStateDescriptor<Long> stateDesc = new ReducingStateDescriptor<>("ptcls-time", new Min(), LongSerializer.INSTANCE);

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {

//        long watermark = ctx.getCurrentWatermark();
//        ctx.registerEventTimeTimer(window.maxTimestamp());
        ReducingState<Long> f = ctx.getPartitionedState(stateDesc);


        System.err.println(ctx.getCurrentWatermark() + "--" + window.getStart() + "--" + window.getEnd() + "--" + timestamp);
        if (count > 4) {
            count = 0;
            ctx.registerEventTimeTimer(System.currentTimeMillis() + 2000);
            try {
                f.add(System.currentTimeMillis() + 2000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return TriggerResult.CONTINUE;
        } else {
            count++;
        }
        System.out.println("onElement: " + element);
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        System.out.println(1111);
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }

    @Override
    public String toString() {
        return "CountTrigger";
    }

    public static CountTrigger of(int threshold) {
        return new CountTrigger(threshold);
    }


    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 2L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }
}
