package com.qiwei;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.table.data.*;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.types.logical.RowType;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import java.io.IOException;
import java.text.ParseException;

import java.util.ArrayList;
import java.util.List;



public class ContMap extends RichFlatMapFunction<String,RowData>{
    private List<String> goodList;
    private List<String> badList;
    private TableLoader tableLoader;
    JsonRowDataDeserializationSchema jrd;

    public ContMap() {}
    public ContMap(TableLoader tableLoader) {
        this.tableLoader = tableLoader;
    }

    /*
    1. title op在cdur时，进行不同的操作

     r   初始
     i   插入
     d   删除
     c   插入
     u   update


    2. 将msg_cont包含的List取出，给到新的字段
    */

    @Override
    public void flatMap(String s, Collector<RowData> collector) throws Exception {

        if (s != null) {

            JSONObject jsonObject = JSONObject.parseObject(s);

            String op = jsonObject.getString("op");
            switch (op) {
                case "r":
                case "c":
                case "i":
                    //如果操作类型是Insert
                    String data = jsonObject.getString("after");
                    JSONObject datajson = JSONObject.parseObject(data);
                    RowData rowData = jsonToRowData(datajson, jrd);
                    rowData.setRowKind(RowKind.INSERT);

                    collector.collect(rowData);
                    break;

                case "u": { //如果操作类型是Update

                    //update前的数据
                    String dataBefore = jsonObject.getString("before");
                    JSONObject datajsonBefore = JSONObject.parseObject(dataBefore);
                    RowData rowDataBefore = jsonToRowData(datajsonBefore, jrd);
                    rowDataBefore.setRowKind(RowKind.UPDATE_BEFORE);

                    //update后的数据
                    String dataAfter = jsonObject.getString("after");
                    JSONObject datajsonAfter = JSONObject.parseObject(dataAfter);
                    RowData rowDataAfter = jsonToRowData(datajsonAfter, jrd);
                    rowDataAfter.setRowKind(RowKind.UPDATE_AFTER);

                    collector.collect(rowDataBefore);
                    collector.collect(rowDataAfter);
                    break;
                }
                case "d": { //如果操作类型是Delete

                    String dataBefore = jsonObject.getString("before");
                    JSONObject datajsonBefore = JSONObject.parseObject(dataBefore);
                    RowData rowDataBefore = jsonToRowData(datajsonBefore, jrd);
                    rowDataBefore.setRowKind(RowKind.DELETE);

                    collector.collect(rowDataBefore);
                    break;
                }
                default:
                    //其他类型
                    System.out.println(s);
                    break;
            }

        }
    }


    public RowData jsonToRowData (JSONObject jsonObject,JsonRowDataDeserializationSchema jrd) throws IOException, ParseException {
        String msg_cont = jsonObject.getString("msg_cont").replace("CXXZ","");

        StringBuilder goodMessage = new StringBuilder("");
        StringBuilder badMessage = new StringBuilder("");

        for (String goodString : goodList) {
            if (msg_cont.contains(goodString)) {
                goodMessage.append(goodString).append(" ");
            }
        }
        for (String badString : badList) {
            if (msg_cont.contains(badString)) {
                 badMessage.append(badString).append(" ");
            }
        }

        jsonObject.put("good_message", goodMessage.toString());
        jsonObject.put("bad_message", badMessage.toString());


        GenericRowData rowData = new GenericRowData(7);
        rowData.setField(0, Integer.parseInt(jsonObject.getString("cm_id")));
        rowData.setField(1, StringData.fromString(jsonObject.getString("msg_id")));
        rowData.setField(2, StringData.fromString(jsonObject.getString("msg_cont")));
        rowData.setField(3, StringData.fromString(jsonObject.getString("msg_type")));
        rowData.setField(4, TimestampData.fromEpochMillis(jsonObject.getLong("msg_time")- 28800000L));
        rowData.setField(5, StringData.fromString(jsonObject.getString("good_message")));
        rowData.setField(6, StringData.fromString(jsonObject.getString("bad_message")));


        return rowData;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        goodList = new ArrayList<>();
        badList = new ArrayList<>();
        super.open(parameters);
        goodList.add("非常抱歉");
        goodList.add("祝您周末愉快");
        goodList.add("很高兴为您服务");
        goodList.add("请问有什么可以帮您的吗？");
        goodList.add("关注“浙江电信”公众号查询");
        goodList.add("第一时间回复您");
        goodList.add("工作时间随时在线");
        goodList.add("记得领取上方您的新人礼包");
        goodList.add("您的问题已收到");
        goodList.add("请对我的服务做一下满意度评价哦");
        goodList.add("有事情随时微我");
        goodList.add("您还可以参加话费充值的活动");

        badList.add("我不知道");
        badList.add("这个不是我办理的");
        badList.add("我没有上班");
        badList.add("积分到期了");
        badList.add("这个我办理不了。");
        badList.add("我没时间回信息");
        badList.add("我不是营业厅的");
        badList.add("去营业厅吧");
        badList.add("我不是跟你说过了吗");
        badList.add("银行卡密码");
        badList.add("电信就是这么规定的");
        badList.add("我也没办法");
        badList.add("您可以打10000号投诉");
        badList.add("电信是比移动贵");
        badList.add("电信信号确实不好");
        badList.add("移动是便宜点");
        badList.add("！！");
        badList.add("？？");
        badList.add("看到消息请回复");
        badList.add("不归我管");
        badList.add("无语");
        badList.add("不是这样的");
        badList.add("喂！");
        badList.add("你怎么这样");
        badList.add("我没有必要骗你");
        badList.add("你要投诉，我也没办法");
        badList.add("XX");

        tableLoader.open();
        Table table = tableLoader.loadTable();
        RowType rowType = FlinkSchemaUtil.convert(table.schema());

        jrd  = new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
                true, false, TimestampFormat.SQL);

    }


}
