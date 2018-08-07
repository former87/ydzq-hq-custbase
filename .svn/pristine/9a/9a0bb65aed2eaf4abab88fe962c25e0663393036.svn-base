package com.ydzq.hq.custbase.kafka.thread;

import java.util.List;
 

import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.api.RMapAsync;
import org.slf4j.LoggerFactory; 

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ydzq.hq.global.AppConstrants;
import com.ydzq.hq.vo.hq.HqSnapVo;

/**
 * 快照
 * 
 * @author f.w
 *
 */
public class HqSnapHandlerRunnable extends HqBaseHanlerRunnable<HqSnapVo> {

	public Redisson redisson;

	public HqSnapHandlerRunnable(Redisson redisson,String topic) throws Exception {
		super(HqSnapVo.class, new ObjectMapper(), topic, LoggerFactory.getLogger(topic));
		this.redisson = redisson;
	}

	@Override
	protected void save(List<HqSnapVo> l) throws Exception {

		if (l != null && l.size() > 0) {
			String key = AppConstrants.REDIS.HK.SNAP;
			RBatch ba = redisson.createBatch();
			RMapAsync<String, HqSnapVo> map = ba.getMap(key);
			for (HqSnapVo vo : l) {
				map.putAsync(vo.getId(), vo);
			}
			ba.execute();

		}
	}
}
