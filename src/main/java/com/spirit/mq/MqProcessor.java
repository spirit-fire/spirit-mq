package com.spirit.mq;

import com.spirit.commons.common.ToolUtils;
import com.spirit.mq.handler.MqMsgHandler;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * All mq processor need to implements this interface
 */
@Service("mqProcessor")
public class MqProcessor {

    /** mq processor names parse from .properties, */
    private String mqProcessorName;

    /** if mq processor need to do */
    private boolean mqProcessor = false;

    /**
     * default constructor
     */
    public MqProcessor(){

    }

    /**
     * start mq msg handler
     * @param handler
     */
    private void start(final MqMsgHandler handler){
        Thread t = new Thread(new Runnable() {
            public void run() {
                handler.process();
            }
        });
        t.start();
    }

    /** main deal function */
    public void process(){
        if(!mqProcessor){
            // log info, do not start mq processor
            return;
        }

        List<String> mqList = ToolUtils.explode(mqProcessorName, ",");
        ApplicationContext applicationContext = null;
        for(String mq : mqList){
            MqMsgHandler handler = (MqMsgHandler) applicationContext.getBean(mq);
            start(handler);
            // record handler
        }
    }

}
