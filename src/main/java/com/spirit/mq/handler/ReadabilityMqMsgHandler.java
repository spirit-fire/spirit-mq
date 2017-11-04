package com.spirit.mq.handler;

import com.qiniu.util.StringUtils;
import com.spirit.commons.common.ApacheHttpClient;
import com.spirit.commons.common.ApiLogger;
import com.spirit.commons.common.HttpRequest;
import com.spirit.commons.storage.mc.GkMemcacheClient;
import com.spirit.commons.storage.mc.GkMemcacheClientShardingSupport;
import com.spirit.engine.readability.PageReadException;
import com.spirit.engine.readability.Readability;

import com.spirit.engine.readability.model.ReadabilityCallBackModel;
import com.spirit.engine.readability.service.ReadabilityService;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;

/**
 * readability mq processor
 */
@Service("readabilityMqMsgHandler")
public class ReadabilityMqMsgHandler implements Handler {

    private static ApacheHttpClient apacheHttpClient;
    static{
        apacheHttpClient = new ApacheHttpClient(50, 1000, 2000, 1, 10, 50);
    }

    /** callback url */
    private static final String GEEKBOOK_CALLBACK_INTERFACE = "https://uapi.geekbook.cc/article/update";

    @Resource
    private ReadabilityService readabilityService;

    /**
     * if anything need to do
     */
    public void init(){

    }

    /**
     * deal with data, finish business here
     * @param data
     */
    @Override
    public void deal(String data){
        JSONObject root = null;
        String info = "";
        String url = "";
        String uid = "";
        String aid = "";
        try {
            root = new JSONObject(data);
            url = root.has("url") ? root.getString("url") : "";
            uid = root.has("uid") ? root.getString("uid") : "";
            aid = root.has("aid") ? root.getString("aid") : "";
            if(StringUtils.isNullOrEmpty(url)
                    || StringUtils.isNullOrEmpty(uid)
                    || StringUtils.isNullOrEmpty(aid)){
                ApiLogger.info(this.getClass().getName() + " data get from mcq is invalid, data: " + data);
                return;
            }
            ReadabilityCallBackModel callBackModel = readabilityService.getCallBackModerlFromUrAndSavePicsToQiniu(url);
            callBackModel.setAid(aid);
            callBackModel.setUid(uid);
            callBackModel.setCtime(System.currentTimeMillis()/1000);
            String response = HttpRequest.doPost(GEEKBOOK_CALLBACK_INTERFACE, callBackModel.toString());
//            String response = apacheHttpClient.post(GEEKBOOK_CALLBACK_INTERFACE, callBackModel.toParams(), "utf-8");
            ApiLogger.info(this.getClass().getName() + " callback response: " + response);
        } catch (JSONException e) {
            ApiLogger.error(this.getClass().getName() + " JSONException, err_msg: " + e.getMessage());
        } catch (PageReadException e) {
            ApiLogger.error(this.getClass().getName() + " PageReadException, err_msg: " + e.getMessage());
        } catch (Exception e) {
            ApiLogger.error(this.getClass().getName() + " Exception, err_msg: " + e.getMessage());
        }
    }

    public static void main(String[] args){
        String response = apacheHttpClient.post(GEEKBOOK_CALLBACK_INTERFACE, new HashMap<String, String>(), "utf-8");
        System.out.println(response);
    }

}
