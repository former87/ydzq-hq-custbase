package com.ydzq.hq.custbase.kafka.thread;

import java.util.Collections;
import java.util.List;

import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.api.RListAsync;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ydzq.hq.global.AppConstrants;
import com.ydzq.hq.vo.hq.HqDealVo;

/**
 * 快照
 * 
 * @author f.w
 *
 */
public class HqDealHandlerRunnable extends HqBaseHanlerRunnable<HqDealVo> {

	public Redisson redisson;

	public HqDealHandlerRunnable(Redisson redisson, String topic) throws Exception {
		super(HqDealVo.class, new ObjectMapper(), topic, LoggerFactory.getLogger(topic));
		this.redisson = redisson;
	}

	@Override
	protected void save(List<HqDealVo> l) throws Exception {

		if (l != null && l.size() > 0) {
			RBatch ba = redisson.createBatch();
			for (HqDealVo vo : l) {
				String key = String.format(AppConstrants.REDIS.HK.DEAL, vo.getId());
				RListAsync<HqDealVo> list = ba.getList(key);
				list.addAllAsync(0, Collections.singleton(vo));
				// list.trimAsync(0, 9);//暂时不修剪
			}
			ba.execute();
		}

	}
}
