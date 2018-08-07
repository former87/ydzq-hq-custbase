package com.ydzq.hq.custbase.kafka.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ydzq.hq.custbase.kafka.thread.HqBaseHanlerRunnable;
import com.ydzq.hq.custbase.kafka.vo.KafkaVo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * kafka线程池
 * 
 * @author f.w
 *
 */
@Component
public class KafkaClientPool {
	private static ExecutorService cachedThreadPool = null;
	private static ConcurrentHashMap<String, KafkaClientPoolProxy> clientPoolMap = null;
	private static Logger logger = LoggerFactory.getLogger(KafkaClientPoolProxy.class);

	private static String getPoolKey(KafkaVo vo) {
		return vo.getGroup_id() + "-" + vo.getBootstrap_servers();
	}

	@SuppressWarnings("rawtypes")
	public static KafkaClientPoolProxy createPool(KafkaVo vo, ConcurrentHashMap<String, HqBaseHanlerRunnable> handlers)
			throws Exception {

		if (cachedThreadPool == null) {
			cachedThreadPool = Executors.newCachedThreadPool();
		}

		if (clientPoolMap == null)
			clientPoolMap = new ConcurrentHashMap<String, KafkaClientPoolProxy>();

		String serverAddress = getPoolKey(vo);
		if (serverAddress == null || serverAddress.trim().length() == 0) {
			throw new IllegalArgumentException("[" + serverAddress + "]  serverAddress is null");
		}

		Future<KafkaClientPoolProxy> queue = cachedThreadPool.submit(new KafkaClientPoolFactory(vo, handlers));
		KafkaClientPoolProxy clientPool = queue.get();
		clientPoolMap.put(serverAddress, clientPool);
		logger.info("NettyClientPool[" + serverAddress + "]-[" + Thread.currentThread().getName() + "]");
		return clientPool;
	}

	public static KafkaClientPoolProxy getPool(KafkaVo vo) throws Exception {

		String serverAddress = vo.getGroup_id() + "-" + vo.getBootstrap_servers();
		// valid serverAddress
		if (serverAddress == null || serverAddress.trim().length() == 0) {
			throw new IllegalArgumentException("[" + serverAddress + "]  serverAddress is null");
		}

		if (clientPoolMap == null)
			throw new IllegalArgumentException("[" + serverAddress + "] isn't create ");

		// get from pool
		KafkaClientPoolProxy clientPool = clientPoolMap.get(serverAddress);
		if (clientPool != null) {
			return clientPool;
		}

		return null;
	}

	public static void removePool(KafkaVo vo) throws Exception {
		logger.info("=================kafka:[" + vo + "] is begin shutdown=================");
		KafkaClientPoolProxy proxy = getPool(vo);
		String serverAddress = getPoolKey(vo);
		if (proxy != null) {
			proxy.close();
			clientPoolMap.remove(serverAddress);

			logger.info(
					"KafkaClientPool[" + serverAddress + "]-[" + Thread.currentThread().getName() + "] is removePool");
		} else
			logger.info("KafkaClientPool[" + serverAddress + "]-[" + Thread.currentThread().getName() + "] is null");
	}
}
