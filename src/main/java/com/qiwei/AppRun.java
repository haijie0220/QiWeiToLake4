package com.qiwei;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.HashMap;
import java.util.Map;

import utils.FlinkEnvUtil;

public class AppRun {
    public static void main(String[] args) throws Exception {

        String fsStateBackendCheckPath = "hdfs://jf-iceberg/user/sloth/flink/checkpoints";
        String metastoreUrl = "thrift://jfdkhadoop007:9083,thrift://jfdkhadoop006:9083";
        String hiveWarehouse = "hdfs://jf-iceberg/iceberg/jf_hive_catalog/warehouse/";
        String catalog = "hive_catalog";
        String icebergDatabase = "oth_kj_qwzj";
        String icebergTable = "con_msg_ext_test22";
        String mysqlHostname = "134.108.39.138";
        int mysqlPort = 3306;
        String mysqlDatabase = "company_wechat";
        String mysqlTable = "company_wechat.con_msg";
        String mysqlUsername = "qdecoder";
        String mysqlPasswd = "7eEM71KQSxV";


        //构建流式环境
        StreamExecutionEnvironment env = FlinkEnvUtil.creatEnv5(fsStateBackendCheckPath);

        //加载iceberg表
        Configuration configuration = new Configuration();
        Map<String,String> ConfigMap = new HashMap<>();
        ConfigMap.put("uri", metastoreUrl);
        ConfigMap.put("warehouse", hiveWarehouse);
        CatalogLoader catalogLoader = CatalogLoader.hive(catalog, configuration, ConfigMap);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader,TableIdentifier.of(icebergDatabase,icebergTable));

        //source->map->sink

        //mysql source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(mysqlHostname)
                .port(mysqlPort)
                .databaseList(mysqlDatabase) // set captured database
                .tableList(mysqlTable) // set captured table
                .username(mysqlUsername)
                .password(mysqlPasswd)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> source = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Qiwei MySQL Source")
                .setParallelism(1);


        //map->做字段解析
        SingleOutputStreamOperator<RowData> rowData = source.flatMap(new ContMap(tableLoader));

        //iceberg-sink
        FlinkSink
                .forRowData(rowData)
                .upsert(true)
                .distributionMode(DistributionMode.HASH)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .append();

        env.execute("Qiwei Iceberg DataStream");
    }

}
