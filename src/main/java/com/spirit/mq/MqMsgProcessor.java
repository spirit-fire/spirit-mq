package com.spirit.mq;

/**
 * handler, task 都实现这个接口
 */
public interface MqMsgProcessor {

    /** main deal function */
    void process();

}
