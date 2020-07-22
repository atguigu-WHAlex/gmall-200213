package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {

    public static void main(String[] args) {

        //1.创建Canal连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");

        //2.连接Canal,订阅表,抓取数据
        while (true) {

            //a.连接Canal
            canalConnector.connect();

            //b.订阅表
            canalConnector.subscribe("gmall200213.*");

            //c.抓取数据
            Message message = canalConnector.get(100);

            //d.解析message
            if (message.getEntries().size() <= 0) {
                System.out.println("当次抓取数据无结果，休息一会！！！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                for (CanalEntry.Entry entry : message.getEntries()) {

                    //判断当前操作是否为数据操作
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        try {
                            //1.获取表名
                            String tableName = entry.getHeader().getTableName();

                            //2.获取StoreValue
                            ByteString storeValue = entry.getStoreValue();

                            //3.反序列化
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                            //4.获取事件类型
                            CanalEntry.EventType eventType = rowChange.getEventType();

                            //5.获取行数据集
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            //6.根据表名以及eventType,处理rowDatasList
                            handler(tableName, eventType, rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    //根据表名以及eventType,处理rowDatasList
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //订单表，新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            sendToKafka(GmallConstants.ORDER_INFO, rowDatasList);

            //订单详情表,新增数据
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            sendToKafka(GmallConstants.ORDER_DETAIL, rowDatasList);

            //用户表,新增及变化数据
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {

            sendToKafka(GmallConstants.USER_INFO, rowDatasList);

        }
    }

    private static void sendToKafka(String topic, List<CanalEntry.RowData> rowDatasList) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //创建JSONObject用于存放单条数据
            JSONObject result = new JSONObject();
            //将列名和列值放入JSONObject
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                result.put(column.getName(), column.getValue());
            }
            //单条数据打印并发送至Kafka
            String msg = result.toString();
            try {
                Thread.sleep(new Random().nextInt(5) * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(msg);
            MyKafkaSender.send(topic, msg);
        }
    }


}
