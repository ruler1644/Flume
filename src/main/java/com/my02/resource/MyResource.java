package com.my02.resource;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.Map;

/**
 * @Auther wu
 * @Date 2019/7/12  8:17
 */
public class MyResource extends AbstractSource implements Configurable, PollableSource {

    //定义全局的前缀和后缀
    private String prefix;
    private String suffix;


    //读取配置文件内容，初始化前后缀
    public void configure(Context context) {

        prefix = context.getString("prefix");
        suffix = context.getString("suffix", ".log");
    }

    //获取数据，封装成event并写入channel
    //这个方法会被循环调用
    public Status process() throws EventDeliveryException {

        try {

            //创建事件
            Event event = new SimpleEvent();

            //创建事件头
            Map<String, String> headerMap = new HashMap<String, String>();

            //循环封装事件
            for (int i = 0; i < 5; i++) {

                //设置事件头
                event.setHeaders(headerMap);

                //设置body
                event.setBody((prefix + "--" + i + "--" + suffix).getBytes());

                //将事件写入channel
                getChannelProcessor().processEvent(event);

                //休眠2秒
                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}