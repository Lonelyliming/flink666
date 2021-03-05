package com.zky.chapter05;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author：Dawn
 * @date：2020/11/26 14:45
 * @Desc：
 */
public class Flink01_Environment {
    public static void main(String[] args) {
        //创建批处理环境
        ExecutionEnvironment env1 = ExecutionEnvironment.getExecutionEnvironment();
        //创建流处理环境
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
