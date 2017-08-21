package com.spirit.mq.handler;

/**
 * All mq processor need to implements this interface
 */
public interface MqMsgHandler {

    /** main deal function */
    void process();

}
