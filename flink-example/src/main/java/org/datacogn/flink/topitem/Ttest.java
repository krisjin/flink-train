package org.datacogn.flink.topitem;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * User:shijingui
 * Date:2019-07-22
 */
public class Ttest {


    public static void main(String[] args) throws Exception {

        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，改变并发对结果正确性没有影响
        env.setParallelism(1);

        // UserBehavior.csv 的本地文件路径, 在 resources 目录下
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<HotItems.UserBehavior> pojoType = (PojoTypeInfo<HotItems.UserBehavior>) TypeExtractor.createTypeInfo(HotItems.UserBehavior.class);

        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};

        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<HotItems.UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);


        // 创建数据源，得到 UserBehavior 类型的 DataStream
        env.createInput(csvInput, pojoType)
                // 抽取出时间和生成 watermark
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<HotItems.UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(HotItems.UserBehavior userBehavior) {
                        // 原始数据单位秒，将其转成毫秒
                        return userBehavior.timestamp * 1000;
                    }
                })
                .filter(new FilterFunction<HotItems.UserBehavior>() {
                    @Override
                    public boolean filter(HotItems.UserBehavior userBehavior) throws Exception {
                        // 过滤出只有点击的数据
                        return userBehavior.behavior.equals("pv");
                    }
                })
                .keyBy("itemId")
                .window(TumblingEventTimeWindows.of(Time.hours(12)))//滑动窗口，窗口大小1小时，滑动步长5分钟
                .trigger(new HotItems.CustomTrigger())
                .aggregate(new HotItems.CountAgg(), new HotItems.WindowResultFunction()).process(new ProcessFunction<HotItems.ItemViewCount, Object>() {
            @Override
            public void processElement(HotItems.ItemViewCount data, Context ctx, Collector<Object> out) throws Exception {
                out.collect("item:" + data.itemId + ", count:" + data.viewCount+","+ data.windowEnd);

            }
        })
//                .keyBy("windowEnd")
//                .process(new TopNHotItems(3))
                .print();

        env.execute("Hot Items Job");
    }

    /**
     * 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
     */
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, HotItems.ItemViewCount, String> {

        private final int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private ListState<HotItems.ItemViewCount> itemState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<HotItems.ItemViewCount> itemsStateDesc = new ListStateDescriptor<>(
                    "itemState-state",
                    HotItems.ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(
                HotItems.ItemViewCount input,
                Context context,
                Collector<String> collector) throws Exception {

            // 每条数据都保存到状态中
            itemState.add(input);
            // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            context.timerService().registerEventTimeTimer(input.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取收到的所有商品点击量
            List<HotItems.ItemViewCount> allItems = new ArrayList<>();
            for (HotItems.ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort(new Comparator<HotItems.ItemViewCount>() {
                @Override
                public int compare(HotItems.ItemViewCount o1, HotItems.ItemViewCount o2) {
                    return (int) (o2.viewCount - o1.viewCount);
                }
            });
            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < allItems.size() && i < topSize; i++) {
                HotItems.ItemViewCount currentItem = allItems.get(i);
                // No1:  商品ID=12224  浏览量=2413
                result.append("No").append(i).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  浏览量=").append(currentItem.viewCount)
                        .append("\n");
            }
            result.append("====================================\n\n");

            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000);

            out.collect(result.toString());
        }
    }

    /**
     * 用于输出窗口的结果
     */
    public static class WindowResultFunction implements WindowFunction<Long, HotItems.ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(
                Tuple key,  // 窗口的主键，即 itemId
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
                Collector<HotItems.ItemViewCount> collector  // 输出类型为 ItemViewCount
        ) throws Exception {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count = aggregateResult.iterator().next();
            collector.collect(HotItems.ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }

    /**
     * COUNT 统计的聚合函数实现，每出现一条记录加一
     */
    public static class CountAgg implements AggregateFunction<HotItems.UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(HotItems.UserBehavior userBehavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    /**
     * 商品点击量(窗口操作的输出类型)
     */
    public static class ItemViewCount {
        public long itemId;     // 商品ID
        public long windowEnd;  // 窗口结束时间戳
        public long viewCount;  // 商品的点击量

        public static HotItems.ItemViewCount of(long itemId, long windowEnd, long viewCount) {
            HotItems.ItemViewCount result = new HotItems.ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }


        public String toString() {

            return "itemId:" + itemId + ", count:" + viewCount;
        }
    }

    /**
     * 用户行为数据结构
     **/
    public static class UserBehavior {
        public long userId;         // 用户ID
        public long itemId;         // 商品ID
        public int categoryId;      // 商品类目ID
        public String behavior;     // 用户行为, 包括("pv", "buy", "cart", "fav")
        public long timestamp;      // 行为发生的时间戳，单位秒
    }


    static class CustomTrigger extends Trigger<Object, TimeWindow> {
        private static final long serialVersionUID = 1L;

        public CustomTrigger() {
        }

        private static int flag = 0;

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            if (flag > 30) {
                flag = 0;
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                flag++;
            }
//            System.out.println("onElement: " + element);
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteProcessingTimeTimer(window.maxTimestamp());
        }

        @Override
        public String toString() {
            return "CountTrigger";
        }

//    @Override
//    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
//        long windowMaxTimestamp = window.maxTimestamp();
//        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
//            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
//        }
//    }

        public static HotItems.CustomTrigger create() {
            return new HotItems.CustomTrigger();
        }
    }
}
