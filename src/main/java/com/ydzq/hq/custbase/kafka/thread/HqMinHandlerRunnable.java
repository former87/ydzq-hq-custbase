package com.ydzq.hq.custbase.kafka.thread;
 
import java.util.List;
 

import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.api.RMapAsync;
import org.slf4j.LoggerFactory; 

import com.fasterxml.jackson.databind.ObjectMapper; 
import com.ydzq.hq.global.AppConstrants;
import com.ydzq.hq.vo.hq.HqMinVo; 

/**
 * 快照
 * 
 * @author f.w
 *
 */
public class HqMinHandlerRunnable extends HqBaseHanlerRunnable<HqMinVo> {

	public Redisson redisson;

	public HqMinHandlerRunnable(Redisson redisson, String topic) throws Exception {
		super(HqMinVo.class, new ObjectMapper(), topic, LoggerFactory.getLogger(topic)); 
		this.redisson = redisson;
	}

	@Override
	protected void save(List<HqMinVo> l) throws Exception {
		if (l != null && l.size() > 0) {
			RBatch ba = redisson.createBatch();
			for (HqMinVo vo : l) {
				String key = String.format(AppConstrants.REDIS.HK.MIN, vo.getId());
				RMapAsync<String, HqMinVo> hmvs = ba.getMap(key);
				hmvs.putAsync(vo.getTime(), vo); 
			}
			ba.execute();
		}
		
	}
 
}
