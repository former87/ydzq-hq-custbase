package com.ydzq.hq.custbase.service;

import java.util.Collections;

import javax.annotation.Resource;

import org.redisson.Redisson;
import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import com.ydzq.hq.global.AppConstrants;
import com.ydzq.hq.vo.hq.HqDealVo;
import com.ydzq.hq.vo.hq.HqJjsVo;
import com.ydzq.hq.vo.hq.HqMinVo;
import com.ydzq.hq.vo.hq.HqSnapVo;
import com.ydzq.hq.vo.hq.HqTenVo;

@Component
public class CacheHkService {

	@Resource
	@Qualifier("hkQuote")
	public Redisson redisson_quotess;

	public void save(HqTenVo vo) {
		String key = String.format(AppConstrants.REDIS.HK.TEN, vo.getId());
		RMap<Integer, HqTenVo> map = redisson_quotess.getMap(key);
		map.put(vo.getnSide(), vo);
	}

	public void save(HqSnapVo vo) {
		String key = AppConstrants.REDIS.HK.SNAP;
		RMap<String, HqSnapVo> map = redisson_quotess.getMap(key);
		map.put(vo.getId(), vo);
	}

	public void save(HqDealVo vo) {
		String key = String.format(AppConstrants.REDIS.HK.DEAL, vo.getId());
		RList<HqDealVo> list = redisson_quotess.getList(key);
		list.addAll(0, Collections.singleton(vo));
		// list.trimAsync(0, 9);//暂时不修剪
	}

	public void save(HqMinVo vo) {
		String key = String.format(AppConstrants.REDIS.HK.MIN, vo.getId());
		RMap<String, HqMinVo> map = redisson_quotess.getMap(key);
		map.put(vo.getTime(), vo);
	}

	public void save(HqJjsVo vo) {
		String key = String.format(AppConstrants.REDIS.HK.JJS, vo.getId());
		RMap<Integer, HqJjsVo> map = redisson_quotess.getMap(key);
		map.put(vo.getnSide(), vo);
	}

}
