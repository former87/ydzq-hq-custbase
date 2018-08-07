package com.ydzq.hq.custbase.kafka.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ydzq.hq.custbase.kafka.thread.HqBaseHanlerRunnable;
import com.ydzq.hq.custbase.kafka.vo.KafkaVo;

/**
 * kafka代理
 * 
 * @author f.w
 *
 */
public class KafkaClientPoolProxy extends Thread {
	private Logger logger = LoggerFactory.getLogger(KafkaClientPoolProxy.class);

	private KafkaConsumer<Long, byte[]> consumer = null;
	@SuppressWarnings("rawtypes")
	protected ConcurrentHashMap<String, HqBaseHanlerRunnable> handlers = null;
	protected ExecutorService cachedThreadPool = null;
	private KafkaVo vo = null;
	public boolean isTrue = true;

	@SuppressWarnings("rawtypes")
	public KafkaClientPoolProxy(KafkaVo vo, ConcurrentHashMap<String, HqBaseHanlerRunnable> handlers) {
		this.vo = vo;
		this.handlers = handlers;
	}

	public void createProxy() {
		try {
			logger.info("=================kafka:[" + vo + "] is strt=================");
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, vo.getBootstrap_servers());
			props.put(ConsumerConfig.GROUP_ID_CONFIG, vo.getGroup_id());
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, vo.getEnable_auto_commit());
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, vo.getKey_deserializer());
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, vo.getValue_deserializer());
			props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, vo.getHeartbeat_interval_ms());
			props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, vo.getSession_timeout_ms());
			props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, vo.getMax_partition_fetch_bytes());
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, vo.getAuto_offset_reset());// earliest、latest
			consumer = new KafkaConsumer<Long, byte[]>(props);

		} catch (Exception e) {
			logger.error("kafka [" + vo.getGroup_id() + "-" + vo.getBootstrap_servers() + "]-["
					+ Thread.currentThread().getName() + "] fail. error:", e);
			close();
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("rawtypes")
	public void reset() {
		if (handlers != null) {
			for (HqBaseHanlerRunnable r : handlers.values()) {
				r.reset();
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void run() {
		try {
			init();

			List<String> list = null;
			if (handlers != null) {
				list = new ArrayList<String>();
				for (String r : handlers.keySet()) {
					list.add(r);
				}
			}

			if (list == null) {
				logger.info("KafkaConsumerRunnable[" + handlers + "] topic list is empty");
				return;
			}

			consumer.subscribe(list, new ConsumerRebalanceListenerIml());
			while (isTrue) {
				try {

					ConsumerRecords<Long, byte[]> records = consumer.poll(vo.getTimeout());
					logger.info("topic:" + list.toString() + " record size:" + records.count() + ".");
					for (ConsumerRecord<Long, byte[]> consumerRecord : records) {
						try {

							long key = consumerRecord.key();
							logger.info("package key:" + key + ".recieve time cost:" + (System.currentTimeMillis() - key));
							String topic = consumerRecord.topic();

							HqBaseHanlerRunnable handler = handlers.get(topic);
							if (handler != null) {
								handler.addBb(consumerRecord);
							} else
								throw new Exception("topic[" + topic + "] isn't type");

						} catch (Exception e) {
							e.printStackTrace();
							logger.info("consumer false", e);
						}
					}
					consumer.commitSync();
					// Thread.sleep(500);
				} catch (Exception e) {
					e.printStackTrace();
					for (StackTraceElement ste : e.getStackTrace()) {
						logger.info(ste.toString());
					}
				}
			}
		} catch (Exception e) {
			logger.error("kafka [" + vo.getGroup_id() + "-" + vo.getBootstrap_servers() + "]-["
					+ Thread.currentThread().getName() + "]", e);
		}
		logger.info("kafka [" + vo.getGroup_id() + "-" + vo.getBootstrap_servers() + "]-["
				+ Thread.currentThread().getName() + "] is close");
	}

	public void close() {
		try {
			if (this.consumer != null) {
				this.isTrue = false;
				this.consumer.wakeup();
				this.consumer.close();
				this.shutdown();
			}
			logger.info("kafka[" + vo.getGroup_id() + "-" + vo.getBootstrap_servers() + "]-["
					+ Thread.currentThread().getName() + "] close.");
		} catch (Exception e) {
			logger.error("kafka close[" + vo.getGroup_id() + "-" + vo.getBootstrap_servers() + "]-["
					+ Thread.currentThread().getName() + "] fail. error", e);
		}
	}

	@SuppressWarnings("rawtypes")
	private void init() {

		cachedThreadPool = Executors.newFixedThreadPool(handlers.size(), new ThreadFactory() {
			AtomicInteger atomic = new AtomicInteger();

			public Thread newThread(Runnable r) {
				return new Thread(r, vo.getGroup_id() + this.atomic.getAndIncrement());
			}
		});

		if (handlers != null) {
			for (HqBaseHanlerRunnable r : handlers.values()) {
				cachedThreadPool.submit(r);
				logger.info("[" + vo.getGroup_id() + "-" + vo.getBootstrap_servers() + "] is load");
			}
		} else
			logger.info(
					"[" + vo.getGroup_id() + "-" + vo.getBootstrap_servers() + "] HqClientHandler handlers is null");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void shutdown() {
		if (cachedThreadPool != null) {
			try {

				for (HqBaseHanlerRunnable r : handlers.values()) {
					r.isTrue = false;
					r = null;
				}

				cachedThreadPool.shutdown();

				// (所有的任务都结束的时候，返回TRUE)
				if (!cachedThreadPool.awaitTermination(0, TimeUnit.MILLISECONDS)) {
					// 超时的时候向线程池中所有的线程发出中断(interrupted)。
					cachedThreadPool.shutdownNow();
				}

			} catch (InterruptedException e) {
				// awaitTermination方法被中断的时候也中止线程池中全部的线程的执行。
				cachedThreadPool.shutdownNow();
			}

			if (handlers != null) {
				for (HqBaseHanlerRunnable r : handlers.values()) {
					logger.info(r.countBb());
				}
				handlers = null;
			}

			if (cachedThreadPool.isShutdown())
				cachedThreadPool = null;

			logger.info("shutdown ClientWorkHandler true ");
		} else
			logger.info("shutdown cachedThreadPool is null");
	}

	private final class ConsumerRebalanceListenerIml implements ConsumerRebalanceListener {
		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			logger.info("onPartitionsRevoked");

		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			logger.info("onPartitionsAssigned");

		}
	}
}
