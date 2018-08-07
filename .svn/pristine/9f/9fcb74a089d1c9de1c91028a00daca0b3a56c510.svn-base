package com.ydzq.hq.custbase.kafka.client;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import com.ydzq.hq.custbase.kafka.thread.HqBaseHanlerRunnable;
import com.ydzq.hq.custbase.kafka.vo.KafkaVo;

/**
 * kafka工厂
 * 
 * @author f.w
 *
 */
public class KafkaClientPoolFactory implements Callable<KafkaClientPoolProxy> {

	private KafkaVo vo;
	@SuppressWarnings("rawtypes")
	private ConcurrentHashMap<String, HqBaseHanlerRunnable> handlers;

	@SuppressWarnings("rawtypes")
	public KafkaClientPoolFactory(KafkaVo vo, ConcurrentHashMap<String, HqBaseHanlerRunnable> handlers) {
		this.vo = vo;
		this.handlers = handlers;
	}

	@Override
	public KafkaClientPoolProxy call() throws Exception {
		KafkaClientPoolProxy proxy = new KafkaClientPoolProxy(this.vo, this.handlers);
		proxy.createProxy();
		proxy.start();
		return proxy;
	}

}
