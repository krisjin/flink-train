package org.datacogn.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;

/**
 * User:shijingui
 * Date:2019-08-12
 */
public class T1 {


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);


    }
}
