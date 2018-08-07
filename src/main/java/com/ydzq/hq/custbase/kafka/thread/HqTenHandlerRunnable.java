package com.ydzq.hq.custbase.kafka.thread;

import java.util.List;
 

import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.api.RMapAsync;
import org.slf4j.LoggerFactory; 

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ydzq.hq.global.AppConstrants;
import com.ydzq.hq.vo.hq.HqTenVo;

/**
 * 快照
 * 
 * @author f.w
 *
 */
public class HqTenHandlerRunnable extends HqBaseHanlerRunnable<HqTenVo> {

	public Redisson redisson;

	public HqTenHandlerRunnable(Redisson redisson,String topic) throws Exception {
		super(HqTenVo.class, new ObjectMapper(), topic, LoggerFactory.getLogger(topic));
		this.redisson = redisson;
	}

	@Override
	protected void save(List<HqTenVo> l) throws Exception {
		if (l != null && l.size() > 0) {
			RBatch ba = redisson.createBatch();
			for (HqTenVo vo : l) {
				String key = String.format(AppConstrants.REDIS.HK.TEN, vo.getId());
				RMapAsync<Integer, HqTenVo> map = ba.getMap(key);
				map.putAsync((Integer) vo.getnSide(), vo);
			}
			ba.execute();
		}

	}

}
