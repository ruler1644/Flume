package com.my01.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Auther wu
 * @Date 2019/7/11  19:22
 */

//根据是否含有单词hello，将数据发送到不同目的地
public class MyInterceptor implements Interceptor {

    private List<Event> addHeaderEvents;

    public void initialize() {
        addHeaderEvents = new ArrayList<Event>();
    }

    //单个事件拦截
    public Event intercept(Event event) {

        //获取事件中的头信息
        Map<String, String> headers = event.getHeaders();

        //获取事件中的body信息
        byte[] body = event.getBody();
        String str = new String(body);

        //据body中是否含有单词hello，添加不同的头信息
        if (str.contains("hello")) {
            headers.put("type", "true");
        } else {
            headers.put("type", "false");
        }
        return event;
    }

    //批量事件拦截
    public List<Event> intercept(List<Event> events) {

        //清空集合
        addHeaderEvents.clear();

        for (Event event : events) {

            //调用interceptor方法，给每一个事件添加头信息，之后将事件添加到集合
            addHeaderEvents.add(intercept(event));
        }
        return addHeaderEvents;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new MyInterceptor();
        }

        public void configure(Context context) {

        }
    }
}