package com.zky.chapter05;

import com.zky.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author：Dawn
 * @date：2020/11/26 15:49
 * @Desc：
 */
public class Flink07_Transform_RichMapFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> mapDS = inputDS.map(new MyRichMapFunction());

        mapDS.print();


        env.execute();
    }


    /**
     * 继承 RichMapFunction，指定输入的类型，返回的类型
     * 提供了 open()和 close() 生命周期管理方法
     * 能够获取 运行时上下文对象 =》 可以获取 状态、任务信息 等环境信息
     */
    public static class MyRichMapFunction extends RichMapFunction<String, WaterSensor> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");

        }

        @Override
        public WaterSensor map(String s) throws Exception {
            String[] fields = s.split(",");

            //还可以获取运行时的环境、状态等信息
            String taskName = getRuntimeContext().getTaskName();
            return new WaterSensor(taskName + fields[0], Long.valueOf(fields[1]), Integer.valueOf(fields[2]));
        }

        @Override
        public void close() throws Exception {
            System.out.println("close");
        }
    }
}
