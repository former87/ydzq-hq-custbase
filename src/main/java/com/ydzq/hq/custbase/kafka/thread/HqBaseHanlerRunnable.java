package com.ydzq.hq.custbase.kafka.thread;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ydzq.hq.vo.HqBase;

@Component
public abstract class HqBaseHanlerRunnable<T extends HqBase> implements Runnable {

	private ConcurrentLinkedQueue<ConsumerRecord<Long, byte[]>> collect = new ConcurrentLinkedQueue<ConsumerRecord<Long, byte[]>>();
	public boolean isTrue = true;
	public String topic = null;
	private Logger logger = null;
	protected ObjectMapper mapper = null;
	private Class<T> childClass;
	AtomicInteger RECONNECT_COUNT = new AtomicInteger(0);

	public HqBaseHanlerRunnable() {

	}

	public HqBaseHanlerRunnable(Class<T> childClass, ObjectMapper objectMapper, String topic, Logger logger)
			throws Exception {
		if (objectMapper == null || StringUtils.isBlank(topic) || logger == null)
			throw new Exception("HqBaseHanlerRunable initializer is error");
		this.childClass = childClass;
		this.topic = topic;
		this.logger = logger;
		this.mapper = objectMapper;
	}

	public void addBb(ConsumerRecord<Long, byte[]> l) {
		if (collect == null) {
			collect = new ConcurrentLinkedQueue<ConsumerRecord<Long, byte[]>>();
		}
		collect.offer(l);
	}

	public void reset() {
		RECONNECT_COUNT = new AtomicInteger(0);
		collect = new ConcurrentLinkedQueue<ConsumerRecord<Long, byte[]>>();
	}

	public String countBb() {
		int count = 0;
		if (collect != null) {
			count = collect.size();
		}
		String str = "topic[name:" + topic + " quote:" + count + " pollcount:" + RECONNECT_COUNT.get() + "]";
		logger.info(str);
		return str;
	}

	// protected abstract void save(T vo) throws Exception;
	protected abstract void save(List<T> l) throws Exception;

	@Override
	public void run() {
		List<T> l = new ArrayList<T>();
		while (isTrue) {

			try {

				if (collect == null || collect.isEmpty()) {
					Thread.sleep(500);
					continue;
				}

				long time = System.currentTimeMillis();
				int i = 0;
				while (!collect.isEmpty()) {
					ConsumerRecord<Long, byte[]> b = null;
					try {
						b = (ConsumerRecord<Long, byte[]>) collect.poll();
						RECONNECT_COUNT.getAndIncrement();
						T entity = mapper.readValue(b.value(), childClass);
						entity.setCft(new Date());
						l.add(entity);
						i++;
						logger.info("[topic:" + b.topic() + ",partition:" + b.partition() + ",offset:" + b.offset()
								+ ",l:" + i + "]" + entity.toString());
					} catch (Exception e) {
						logger.error("[topic:" + b.topic() + ",partition:" + b.partition() + ",offset:" + b.offset()
								+ ",l:" + i + "]", e);
					}
				}
				if (i > 0) {
					this.save(l);
					logger.info("[l:" + i + "] time cost:" + (System.currentTimeMillis() - time));
					l.clear();
				}

			}
			// 没有直接处理这个异常跳出，是因为不知道是否有其他地方有这个异常
			// catch (InterruptedException e) {
			// logger.error(topic + " is stop");
			// break;
			// }
			catch (Exception e) {
				logger.error(topic + "error:", e);
			} finally {

			}
		}
		logger.info(Thread.currentThread().getName() + " " + topic + " is close");
	}

	// @Override
	// public void run() {
	// while (isTrue) {
	// ConsumerRecord<Long, byte[]> b = null;
	// try {
	//
	// if (collect == null || collect.isEmpty()) {
	// Thread.sleep(1000);
	// continue;
	// } else {
	//
	// b = (ConsumerRecord<Long, byte[]>) collect.poll();
	// RECONNECT_COUNT.getAndIncrement();
	// T entity = mapper.readValue(b.value(), childClass);
	// entity.setCft(new Date());
	// this.save(entity);
	// logger.info("[topic:" + b.topic() + ",partition:" + b.partition() +
	// ",offset:" + b.offset() + "]"
	// + entity.toString());
	// }
	// }
	// // 没有直接处理这个异常跳出，是因为不知道是否有其他地方有这个异常
	// // catch (InterruptedException e) {
	// // logger.error(topic + " is stop");
	// // break;
	// // }
	// catch (Exception e) {
	// logger.error(topic + "error:", e);
	// } finally {
	// b = null;
	// }
	// }
	// logger.info(Thread.currentThread().getName() + " " + topic + " is
	// close");
	// }
}
