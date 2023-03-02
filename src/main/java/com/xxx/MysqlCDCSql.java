package com.xxx;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author xqh
 * @date 2023-03-02  10:40:22
 * @apiNote flinkSql 获取mysql cdc数据
 */
public class MysqlCDCSql {
    public static void main(String[] args) throws Exception {
        // TODO 1. 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);
        env.enableCheckpointing(3000);

        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(1);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        // TODO 2. 创建cdc的动态表
        String sourceDDL = "CREATE TABLE mysql_binlog (" +
                " id INT," +
                " name STRING," +
                " PRIMARY KEY(id) NOT ENFORCED"
                + ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'localhost'," +
                " 'port' = '3307'," +
                " 'username' = 'flink'," +
                " 'password' = 'flink'," +
                " 'database-name' = 'cdc'," +
                " 'table-name' = 'flink_cdc'" +
//                " 'scan.startup.mode' = 'latest-offset'" +
                ")";

//        tableEnv.executeSql(sourceDDL);
//        tableEnv.executeSql("SELECT * FROM mysql_binlog").print();

        // 输出目标表
/*        String sinkDDL =
                "CREATE TABLE test_cdc_sink (\n" +
                        " id INT," +
                        " name STRING," +
                        " PRIMARY KEY(id) NOT ENFORCED" +
                        ") WITH (\n" +
                        " 'connector' = 'jdbc',\n" +
                        " 'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                        " 'url' = 'jdbc:mysql://localhost:3307/cdc?serverTimezone=UTC&useSSL=false',\n" +
                        " 'username' = 'flink',\n" +
                        " 'password' = 'flink',\n" +
                        " 'table-name' = 'flink_cdc_sink'\n" +
                        ")";*/
        String sinkDDL =
                "CREATE TABLE test_cdc_sink (\n" +
                        " id INT," +
                        " name STRING," +
                        " dt STRING," +
                        " PRIMARY KEY(id) NOT ENFORCED" +
                        ") WITH (\n" +
                        " 'connector' = 'jdbc',\n" +
                        " 'driver' = 'org.postgresql.Driver',\n" +
                        " 'url' = 'jdbc:postgresql://localhost:5432/postgres',\n" +
                        " 'username' = 'postgres',\n" +
                        " 'password' = '123456',\n" +
                        " 'table-name' = 'public.flink_cdc_sink'\n" +
                        ")";
//        定义主键，连接器 upsert模式：会根据主键判断插入新行或者更新已存在的行，确保幂等性
        // 同步数据到sink表 ：insert update delete
//        String transformDmlSQL = "insert into test_cdc_sink select * from mysql_binlog";
        String transformDmlSQL = "insert into test_cdc_sink select id,name,'20230302' from mysql_binlog";

        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(sinkDDL);
        tableEnv.executeSql(transformDmlSQL);

    }
}