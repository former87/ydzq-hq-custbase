package com.ydzq.hq.custbase;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.ydzq.hq.custbase.config.mkt.HkKafkaParamsConfig;
import com.ydzq.hq.custbase.kafka.client.KafkaClientPool;
import com.ydzq.hq.custbase.kafka.client.KafkaClientPoolProxy;
import com.ydzq.hq.custbase.kafka.mkt.HkConsumerRunnable;

@Component
public class Entrance {
	private Logger logger = LoggerFactory.getLogger(Entrance.class);
	@Resource
	private HkConsumerRunnable hkConsumerRunnale;
	@Resource
	private HkKafkaParamsConfig hkKafkaParamsConfig;

	@PostConstruct
	private void init() throws Exception {
		if (hkKafkaParamsConfig != null) {
			logger.info("=================kafka:[" + hkKafkaParamsConfig + "] is load=================");
			KafkaClientPool.createPool(hkKafkaParamsConfig, hkConsumerRunnale.initPool());
		}
	}

	@Scheduled(cron = "0 0 9 ? * *")
	public void updateUsStockInfo() {
		if (hkKafkaParamsConfig != null) {
			try {

				logger.info("=================kafka:[" + hkKafkaParamsConfig + "] is reset=================");
				KafkaClientPoolProxy proxy = KafkaClientPool.getPool(hkKafkaParamsConfig);
				proxy.reset();

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			hkConsumerRunnale.clearRedisson();
		}
	}
}
