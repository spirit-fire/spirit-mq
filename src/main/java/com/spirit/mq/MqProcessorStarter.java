package com.spirit.mq;

import com.spirit.commons.common.ApiLogger;
import com.spirit.commons.common.ToolUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.*;

import java.util.List;

/**
 * mq processor/task 启动类
 */
public class MqProcessorStarter implements InitializingBean, ApplicationContextAware {

    /** mq processor names parse from .properties, */
    private String mqProcessorName;

    /** if mq processor need to do */
    private boolean mqStart = false;

    /** ApplicationContext */
    private ApplicationContext applicationContext;

    /**
     * default constructor
     */
    public MqProcessorStarter(){

    }

    /**
     * start mq msg handler
     * @param processor
     */
    private void start(final MqMsgProcessor processor){
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                processor.process();
            }
        });
        t.start();
    }

    /** main deal function, add init-method="process" in .xml */
    public void process(){
        if(!mqStart){
            // log info, do not start mq processor
            ApiLogger.info(String.format("[MqProcessorStarter] MqProcessorStarter process dose not start! mqProcessorName: %s, mqStart: %b", mqProcessorName, mqStart));
            return;
        }

        List<String> mqList = ToolUtils.explode(mqProcessorName, ",");
        for(String mq : mqList){
            MqMsgProcessor processor = (MqMsgProcessor) applicationContext.getBean(mq);
            start(processor);
            // record handler
            ApiLogger.info(String.format("[MqProcessorStarter] MqProcessorStarter process start MqMsgProcessor %s", mq));
        }
    }

    public void setMqProcessorName(String mqProcessorName) {
        this.mqProcessorName = mqProcessorName;
    }

    public String getMqProcessorName() {
        return mqProcessorName;
    }

    public void setMqStart(boolean mqStart) {
        this.mqStart = mqStart;
    }

    public boolean isMqStart() {
        return mqStart;
    }

    /**
     * Invoked by a BeanFactory after it has set all bean properties supplied
     * (and satisfied BeanFactoryAware and ApplicationContextAware).
     * <p>This method allows the bean instance to perform initialization only
     * possible when all bean properties have been set and to throw an
     * exception in the event of misconfiguration.
     *
     * @throws Exception in the event of misconfiguration (such
     *                   as failure to set an essential property) or if initialization fails.
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        ApiLogger.info(String.format("[MqProcessorStarter] MqProcessorStarter afterPropertiesSet, mqProcessorName: %s, mqStart: %b", mqProcessorName, mqStart));
        process();
    }

    /**
     * Set the ApplicationContext that this object runs in.
     * Normally this call will be used to initialize the object.
     * <p>Invoked after population of normal bean properties but before an init callback such
     * as {@link InitializingBean#afterPropertiesSet()}
     * or a custom init-method. Invoked after {@link ResourceLoaderAware#setResourceLoader},
     * {@link ApplicationEventPublisherAware#setApplicationEventPublisher} and
     * {@link MessageSourceAware}, if applicable.
     *
     * @param applicationContext the ApplicationContext object to be used by this object
     * @throws ApplicationContextException in case of context initialization errors
     * @throws BeansException              if thrown by application context methods
     * @see BeanInitializationException
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
