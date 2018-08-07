package com.ydzq.hq.custbase.kafka.thread;

import java.util.List;
 

import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.api.RMapAsync;
import org.slf4j.LoggerFactory; 

import com.fasterxml.jackson.databind.ObjectMapper; 
import com.ydzq.hq.global.AppConstrants;
import com.ydzq.hq.vo.hq.HqJjsVo;

/**
 * 快照
 * 
 * @author f.w
 *
 */
public class HqJjsHandlerRunnable extends HqBaseHanlerRunnable<HqJjsVo> {

	public Redisson redisson;

	public HqJjsHandlerRunnable(Redisson redisson,String topic) throws Exception {
		super(HqJjsVo.class, new ObjectMapper(), topic, LoggerFactory.getLogger(topic));
		this.redisson = redisson;
	}

	@Override
	protected void save(List<HqJjsVo> l) throws Exception {

		if (l != null && l.size() > 0) {
			RBatch ba = redisson.createBatch();
			for (HqJjsVo vo : l) {
				String key = String.format(AppConstrants.REDIS.HK.JJS, vo.getId(), vo.getnSide());
				RMapAsync<Integer, HqJjsVo> map = ba.getMap(key);
				map.putAsync((Integer) vo.getnSide(), vo);
			}
			ba.execute();
		}

	}

}
