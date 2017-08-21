package com.spirit.mq.handler;

import com.spirit.commons.common.StringUtils;
import com.spirit.commons.storage.mc.GkMemcacheClient;
import com.spirit.commons.storage.mc.GkMemcacheClientShardingSupport;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * readability mq processor
 */
@Service("readabilityMqMsgHandler")
public class ReadabilityMqMsgHandler implements MqMsgHandler {

    /** true: start mcq reader, false: execute at scheduleTime */
    private boolean startMcqReader = false;

    /** empty: inf-loop, others: execute at fix frequency */
    private String scheduleTime = "";

    /** mc client writer */
    private GkMemcacheClientShardingSupport mcqWriter;

    /** mc clent reader */
    private List<GkMemcacheClient> mcqReader;

    private String readerKey;

    private String writerKey;

    /**
     * default constructor
     */
    public ReadabilityMqMsgHandler(){

    }

    /**
     * if anything need to do
     */
    private void init(){

    }

    public void process() {
        if(startMcqReader){
            this.processMcq();
        }else{
            this.processSchedule();
        }
    }

    /**
     * deal with data, finish business here
     * @param data
     */
    private void deal(String data){

    }

    /**
     * each mc client used one thread
     * @param client
     */
    private void startMcqReader(final GkMemcacheClient client){
        String data;
        while(true){
            try{
                data = (String) client.get(readerKey);
                if(StringUtils.isNullOrEmpty(data)){
                    Thread.sleep(10);
                    continue;
                }
                deal(data);
            } catch (Exception e){
                // record err msg, then continue
            }
        }
    }

    /**
     * loop
     */
    private void processMcq(){
        if(mcqReader == null || CollectionUtils.isEmpty(mcqReader)){
            // log error msg, mcqReader should not be empty
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

    /**
     * schedule
     */
    private void processSchedule(){

    }
}
