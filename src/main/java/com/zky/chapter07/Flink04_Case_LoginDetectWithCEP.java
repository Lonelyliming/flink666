package com.zky.chapter07;

import com.zky.bean.LoginEvent;
import com.zky.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @Author Dawn
 * @Date 2021/3/8 14:44
 * @Desc 2s内连续登录失败2次的用户
 */
public class Flink04_Case_LoginDetectWithCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<LoginEvent> sensorDS = env
                .readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new LoginEvent(Long.valueOf(datas[0]),datas[1],datas[2], Long.valueOf(datas[3]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<LoginEvent>() {
                            @Override
                            public long extractAscendingTimestamp(LoginEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );

        //按照用户进行分组
        KeyedStream<LoginEvent, Long> keyedDS = sensorDS.keyBy(r -> r.getUserId());

        //TODO CEP处理
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("firstFail")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .next("secondFail")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .within(Time.seconds(2));

        //应用规则
        PatternStream<LoginEvent> loginDS = CEP.pattern(keyedDS, pattern);

        //获取匹配的结果
        SingleOutputStreamOperator<String> resultDS = loginDS.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent firstFail = map.get("firstFail").iterator().next();
                LoginEvent secondFail = map.get("secondFail").iterator().next();
                return "用户" + firstFail.getUserId() + "在2s内连续登陆失败2次，可能为恶意登陆！！！";
            }
        });

        resultDS.print();
        env.execute("login fail");
    }
}
