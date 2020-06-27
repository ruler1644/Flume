package com.my03.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;

/**
 * @Auther wu
 * @Date 2019/7/12  8:46
 */
public class MySink extends AbstractSink implements Configurable {


    //获取logger对象
    private static final Logger logger = LoggerFactory.getLogger(MySink.class);


    //定义前缀，后缀
    private String prefix;
    private String suffix;

    //读取配置文件，并为前后缀赋值
    public void configure(Context context) {
        prefix = context.getString("prefix");
        suffix = context.getString("suffix", ".log");
    }

    public Status process() throws EventDeliveryException {

        //获取channel，并从channel获取事务
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {

            //从channel获取数据
            Event event = channel.take();

            //处理事件数据
            if (event != null) {
                String body = new String(event.getBody());
                logger.info(prefix + body + suffix);
            }

            //提交事务
            transaction.commit();
            return Status.READY;

        } catch (ChannelException e) {
            e.printStackTrace();
            transaction.rollback();
            return Status.BACKOFF;
        } finally {
            transaction.close();
        }
    }
}