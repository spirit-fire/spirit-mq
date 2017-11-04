package com.spirit.mq.handler;

import com.spirit.commons.common.ApiLogger;
import com.spirit.commons.common.StringUtils;
import com.spirit.commons.common.ToolUtils;
import com.spirit.commons.common.trace.RequestContext;
import com.spirit.commons.storage.mc.GkMemcacheClient;
import com.spirit.commons.storage.mc.GkMemcacheClientShardingSupport;
import com.spirit.mq.MqMsgProcessor;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * readability mq processor
 */
public class MqMsgHandler implements MqMsgProcessor {

    /** mc client writer */
    private GkMemcacheClientShardingSupport mcqWriter;

    /** mc clent reader */
    private List<GkMemcacheClient> mcqReader;

    /** mcq reader serverports */
    private String serverports;

    /** read key */
    private String readerKey;

    /** write key */
    private String writerKey;

    /** processor name */
    private String processorName;

    private Handler handler;

    /**
     * default constructor
     */
    public MqMsgHandler(){

    }

    /**
     * if anything need to do
     */
    public void init(){
        processorName = "[MqMsgHandler] " + this.handler.getClass().getName();
        List<String> serverPortList = ToolUtils.explode(serverports, ",");
        mcqReader = new ArrayList<GkMemcacheClient>(serverPortList.size());
        for(int index = 0; index < serverPortList.size(); index++){
            GkMemcacheClient gkMemcacheClient = new GkMemcacheClient();
            gkMemcacheClient.setServerPort(serverPortList.get(index));
            gkMemcacheClient.init();
            mcqReader.add(gkMemcacheClient);
        }
    }

    @Override
    public void process() {
        init();
        this.processMcq();
    }

    /**
     * deal with data, finish business here
     * @param data
     */
    public void deal(String data){
        handler.deal(data);
    }

    /**
     * each mc client used one thread
     * @param client
     */
    private void startMcqReader(final GkMemcacheClient client){
        String data;
        while(true){
            try{
                RequestContext.init();
                data = (String) client.get(readerKey);
                if(StringUtils.isNullOrEmpty(data)){
                    Thread.sleep(10);
                    continue;
                }
                ApiLogger.info(" receive readKey " + readerKey + ", msg: " + data);
                deal(data);
            } catch (Exception e){
                // record err msg, then continue
            } finally {
                RequestContext.finish();
            }
        }
    }
    
    /**
     * loop
     */
    private void processMcq(){
        if(mcqReader == null || CollectionUtils.isEmpty(mcqReader)){
            // log error msg, mcqReader should not be empty
            System.out.println("[MqMsgHandler] " + processorName + " processMcq stop! mcqReader is null or empty!"  + " serverports: " + serverports);
            ApiLogger.error("[MqMsgHandler] " + processorName + " processMcq stop! mcqReader is null or empty!"  + " serverports: " + serverports);
            return;
        }

        for(int index = 0; index < mcqReader.size(); index++){
            final GkMemcacheClient client = mcqReader.get(index);
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    startMcqReader(client);
                }
            });
            thread.start();
        }
    }

    public String getServerports() {
        return serverports;
    }

    public void setServerports(String serverports) {
        this.serverports = serverports;
    }

    public String getReaderKey() {
        return readerKey;
    }

    public void setReaderKey(String readerKey) {
        this.readerKey = readerKey;
    }

    public String getWriterKey() {
        return writerKey;
    }

    public void setWriterKey(String writerKey) {
        this.writerKey = writerKey;
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
