package com.spirit.mq.task;

import com.spirit.commons.common.StringUtils;
import com.spirit.mq.MqMsgProcessor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 为什么会有这个类, 其实很奇怪, 有些任务只是需要定时的去跑一下就好, 这里就暂时的当作一个
 * 定时的 task 去处理吧, 会用 schedule 去处理, 原则上暴露的只是业务代码, 以及需要的时间
 * 配置信息。
 *
 */
public class MqMsgTask implements MqMsgProcessor {

    /** empty: inf-loop, others: execute at fix frequency */
    private String scheduleTime = "";

    /** schedule executor */
    private ScheduledExecutorService scheduledExecutorService = null;

    private String processorName;

    /**
     * if anything need to do
     */
    private void init(){
        if(StringUtils.isNullOrEmpty(scheduleTime)){
            return;
        }

        scheduledExecutorService = Executors.newScheduledThreadPool(1);
    }


    /**
     * main deal function
     */
    @Override
    public void process() {
        this.processSchedule();
    }

    /**
     * schedule
     */
    private void processSchedule(){
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                deal();
            }
        }, 0L, 0L, TimeUnit.SECONDS);
    }

    /**
     *
     */
    public void deal(){

    }
}
