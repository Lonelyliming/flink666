package com.zky.chapter05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author：Dawn
 * @date：2020/11/27 11:39
 * @Desc：
 */
public class Flink22_Sink_MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.readTextFile("input/sensor-data.log");

        inputDS.addSink(new RichSinkFunction<String>() {
            private Connection connection = null;
            private PreparedStatement pstm = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_report", "root", "199902");
                pstm = connection.prepareStatement("INSERT INTO sensor values (?,?,?)");
            }

            @Override
            public void close() throws Exception {
                pstm.close();
                connection.close();
            }

            @Override
            public void invoke(String value, Context context) throws Exception {
                String[] datas = value.split(",");
                pstm.setString(1, datas[0]);
                pstm.setLong(2, Long.valueOf(datas[1]));
                pstm.setLong(3, Integer.valueOf(datas[2]));
                pstm.execute();
            }
        });

        env.execute();
    }
}
