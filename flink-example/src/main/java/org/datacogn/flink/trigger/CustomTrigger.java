package org.datacogn.flink.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * User:shijingui
 * Date:2019-07-22
 */
public class CustomTrigger extends Trigger<Object, TimeWindow> {

    private static final long serialVersionUID = 1L;

    public CustomTrigger() {
    }

    private static int flag = 0;

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, Trigger.TriggerContext ctx) {
        ctx.registerEventTimeTimer(window.maxTimestamp());
        if (flag > 4) {
            flag = 0;
            return TriggerResult.FIRE;
        } else {
            flag++;
        }
        System.out.println("onElement: " + element);
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
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
        return "CustomTrigger";
    }


}
