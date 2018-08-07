package com.ydzq.hq.custbase.kafka.mkt;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Resource;

import org.redisson.Redisson;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import com.ydzq.hq.custbase.global.AppConstrants;
import com.ydzq.hq.custbase.kafka.thread.HqBaseHanlerRunnable;
import com.ydzq.hq.custbase.kafka.thread.HqDealHandlerRunnable;
import com.ydzq.hq.custbase.kafka.thread.HqJjsHandlerRunnable;
import com.ydzq.hq.custbase.kafka.thread.HqMinHandlerRunnable;
import com.ydzq.hq.custbase.kafka.thread.HqMonitorRunnable;
import com.ydzq.hq.custbase.kafka.thread.HqSnapHandlerRunnable;
import com.ydzq.hq.custbase.kafka.thread.HqTenHandlerRunnable;

@Component
public class HkConsumerRunnable {
	@Resource
	@Qualifier("hkQuote")
	public Redisson redisson;

	@SuppressWarnings("rawtypes")
	public ConcurrentHashMap<String, HqBaseHanlerRunnable> initPool() throws Exception {
		if(redisson==null)
			throw new Exception(" redisson is null");
		
		ConcurrentHashMap<String, HqBaseHanlerRunnable> handlers = new ConcurrentHashMap<String, HqBaseHanlerRunnable>();
		handlers.put(AppConstrants.TOPIC.HK.DEAL, new HqDealHandlerRunnable(redisson, AppConstrants.TOPIC.HK.DEAL));
		handlers.put(AppConstrants.TOPIC.HK.JJS, new HqJjsHandlerRunnable(redisson, AppConstrants.TOPIC.HK.JJS));
		handlers.put(AppConstrants.TOPIC.HK.MIN, new HqMinHandlerRunnable(redisson, AppConstrants.TOPIC.HK.MIN));
		handlers.put(AppConstrants.TOPIC.HK.SNAP, new HqSnapHandlerRunnable(redisson, AppConstrants.TOPIC.HK.SNAP));
		handlers.put(AppConstrants.TOPIC.HK.TEN, new HqTenHandlerRunnable(redisson, AppConstrants.TOPIC.HK.TEN));
		handlers.put(AppConstrants.TOPIC.HK.MONITOR, new HqMonitorRunnable(new ConcurrentHashMap<String, HqBaseHanlerRunnable>(handlers), AppConstrants.TOPIC.HK.MONITOR));
		return handlers;
	}
	
	public void clearRedisson(){
		redisson.getKeys().flushdb();
	}

	// @Resource(name = AppConstrants.TOPIC.HK.SNAP)
	// private HqSnapHandlerRunable hqSnapDecoder;
	//
	// @Resource(name = AppConstrants.TOPIC.HK.TEN)
	// private HqTenHandlerRunable hqTenDecoder;
	//
	// @Resource(name = AppConstrants.TOPIC.HK.MIN)
	// private HqMinHandlerRunable hqMinDecoder;
	//
	// @Resource(name = AppConstrants.TOPIC.HK.DEAL)
	// private HqDealHandlerRunable hqDealDecoder;
	//
	// @Resource(name = AppConstrants.TOPIC.HK.JJS)
	// private HqJjsHandlerRunable hqJjsDecoder;

}
