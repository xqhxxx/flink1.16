package com.xxx;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @author xqh
 * @date 2023-02-24  16:19:51
 * @apiNote
 * dataStream 获取mysql cdc数据
 * jsom格式输出
 */

public class MysqlCDCDs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);

        //todo 获取cdc 数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3307)
                .databaseList("cdc")
                .tableList("cdc.flink_cdc")//
                .username("flink")
                .password("flink")
                .deserializer(new JsonDebeziumDeserializationSchema())// converts SourceRecord to JSON String
                .startupOptions(StartupOptions.latest())
                .build();

//        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                .print();



        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flink_cdc_topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();



        // todo  写到kafka
//         env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source").sinkTo(kafkaSink);
//                //todo 处理数据
//



//todo  写到mysql



        env.execute("Print MySQL Snapshot + Binlog");
    }
}