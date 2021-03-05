package com.zky.chapter06;

import com.zky.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author Dawn
 * @Date 2021/1/4 11:29
 * @Desc 水位线的设置 全窗口函数
 */
public class Flink03_TimeCharacteristic_EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 1:指定使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> sensorDS = env.socketTextStream("zky01", 9999);
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/sensor-data.log");

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = stringDataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] fields = value.split(",");
                return new WaterSensor(fields[0], Long.valueOf(fields[1]), Integer.valueOf(fields[2]));
            }
        });

        // TODO 2.指定如何 从数据中 抽取出 事件时间，时间单位是 ms
        SingleOutputStreamOperator<WaterSensor> waterSensorAndAssignTsDS = waterSensorDS
                .assignTimestampsAndWatermarks(
                        //分配水位线有2中方式，一种是周期性插入，一种是Punctual插入，就是来一个就插入一次水位线
                        //这里使用的是周期性插入的实现类之一 升序插入，
                        // 如果涉及到乱序和迟到数据就可以使用另一种实现类型，BoundedOutOfOrdernessTimestampExtractor
                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000;
                            }
                        }
                );

        //分组、开窗、聚合
        waterSensorAndAssignTsDS
                .keyBy(r -> r.getId())
                .process(new MyKeyedProcessFunc())
   /*             .process(new KeyedProcessFunction<String, WaterSensor, String>() {


                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //注册定时器
                        ctx.timerService().registerEventTimeTimer(value.getTs() * 1000 + 5 * 1000);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println(ctx.getCurrentKey() + "触发了==>" + timestamp);
                    }
                })*/
                .print();

        env.execute();
    }

    public static class MyKeyedProcessFunc extends KeyedProcessFunction<String,WaterSensor,String>{
//        private var fasterRepeatList: ListState[String] = _
        private ListState<String> fasterList;
        private ValueState<String> trigger;

        @Override
        public void open(Configuration parameters) throws Exception {
            fasterList = getRuntimeContext().getListState(new ListStateDescriptor<String>("test",String.class));
            trigger = getRuntimeContext().getState(new ValueStateDescriptor<String>("ss",String.class));
        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
            trigger.update("aa");
            trigger.clear();
            System.out.println(trigger.value() == null);
        }
    }

}
