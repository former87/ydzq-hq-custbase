package com.ydzq.hq.custbase.kafka.thread;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ydzq.hq.custbase.global.AppConstrants;

/**
 * 快照
 * 
 * @author f.w
 *
 */
@SuppressWarnings("rawtypes")
public class HqMonitorRunnable extends HqBaseHanlerRunnable {

	private ConcurrentHashMap<String, HqBaseHanlerRunnable> handlers;
	private Logger logger = null;

	public HqMonitorRunnable(ConcurrentHashMap<String, HqBaseHanlerRunnable> handlers, String topic) {
		logger = LoggerFactory.getLogger(topic);
		this.handlers = handlers;
	}

	@Override
	public void run() {

		while (true) {
			logger.info("-------------------------------------");
			for (HqBaseHanlerRunnable r : handlers.values()) {

				logger.info(r.countBb());
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				logger.error(AppConstrants.TOPIC.HK.MONITOR + " is stop");
				break;
			}
		}
		logger.info("hkMonitor is close ");
	}

	@Override
	protected void save(List l) throws Exception {
		// TODO Auto-generated method stub
		
	}
 
}
