package com.ydzq.hq.custbase.kafka.vo;

import org.apache.commons.lang.StringUtils;

public class KafkaVo {
	@Override
	public String toString() {
		return "KafkaVo [bootstrap_servers=" + bootstrap_servers + ", key_deserializer=" + key_deserializer
				+ ", value_deserializer=" + value_deserializer + ", group_id=" + group_id + ", enable_auto_commit="
				+ enable_auto_commit + ", max_partition_fetch_bytes=" + max_partition_fetch_bytes
				+ ", heartbeat_interval_ms=" + heartbeat_interval_ms + ", session_timeout_ms=" + session_timeout_ms
				+ ", auto_offset_reset=" + auto_offset_reset + ", topic=" + topic + ", timeout=" + timeout + "]";
	}

	public long getTimeout() {
		return timeout;
	}

	public KafkaVo setTimeout(long timeout) {
		this.timeout = timeout;
		return this;
	}

	public String getBootstrap_servers() {
		return bootstrap_servers;
	}

	public KafkaVo setBootstrap_servers(String bootstrap_servers) {
		this.bootstrap_servers = bootstrap_servers;
		return this;
	}

	/**
	 * default org.apache.kafka.common.serialization.LongDeserializer
	 * 
	 * @return
	 */
	public String getKey_deserializer() {
		if (StringUtils.isBlank(key_deserializer))
			return "org.apache.kafka.common.serialization.LongDeserializer";
		return key_deserializer;
	}
	/**
	 * default org.apache.kafka.common.serialization.LongDeserializer
	 * 
	 * @return
	 */
	public KafkaVo setKey_deserializer(String key_deserializer) {
		this.key_deserializer = key_deserializer;
		return this;
	}
	/**
	 * default org.apache.kafka.common.serialization.ByteArrayDeserializer
	 * 
	 * @param value_deserializer
	 */
	public String getValue_deserializer() {
		if (StringUtils.isBlank(value_deserializer))
			return "org.apache.kafka.common.serialization.ByteArrayDeserializer";
		return value_deserializer;
	}

	/**
	 * default org.apache.kafka.common.serialization.ByteArrayDeserializer
	 * 
	 * @param value_deserializer
	 */
	public KafkaVo setValue_deserializer(String value_deserializer) {
		this.value_deserializer = value_deserializer;
		return this;
	}

	public String getGroup_id() {
		return group_id;
	}

	public void setGroup_id(String group_id) {
		this.group_id = group_id;
	}
	/**
	 * default false
	 * 
	 * @param enable_auto_commit
	 */
	public String getEnable_auto_commit() {
		if (StringUtils.isBlank(enable_auto_commit))
			return "false";
		return enable_auto_commit;
	}

	/**
	 * false
	 * 
	 * @param enable_auto_commit
	 */
	public KafkaVo setEnable_auto_commit(String enable_auto_commit) {
		this.enable_auto_commit = enable_auto_commit;
		return this;
	}
	/**
	 * default 102400
	 * 
	 * @param max_partition_fetch_bytes
	 */
	public String getMax_partition_fetch_bytes() {
		if (StringUtils.isBlank(max_partition_fetch_bytes))
			return "102400";
		return max_partition_fetch_bytes;
	}

	/**
	 * default 102400
	 * 
	 * @param max_partition_fetch_bytes
	 */
	public KafkaVo setMax_partition_fetch_bytes(String max_partition_fetch_bytes) {
		this.max_partition_fetch_bytes = max_partition_fetch_bytes;
		return this;
	}

	/**
	 * default 10000
	 * 
	 * @param heartbeat_interval_ms
	 */
	public String getHeartbeat_interval_ms() {
		if (StringUtils.isBlank(heartbeat_interval_ms))
			return "10000";
		return heartbeat_interval_ms;
	}

	/**
	 * default 10000
	 * 
	 * @param heartbeat_interval_ms
	 */
	public KafkaVo setHeartbeat_interval_ms(String heartbeat_interval_ms) {
		this.heartbeat_interval_ms = heartbeat_interval_ms;
		return this;
	}

	/**
	 * default 30000
	 * 
	 * @param session_timeout_ms
	 */
	public String getSession_timeout_ms() {
		if (StringUtils.isBlank(session_timeout_ms))
			return "30000";
		return session_timeout_ms;
	}

	/**
	 * default 30000
	 * 
	 * @param session_timeout_ms
	 */
	public KafkaVo setSession_timeout_ms(String session_timeout_ms) {
		this.session_timeout_ms = session_timeout_ms;
		return this;
	}

	/**
	 * default latest
	 * earliest、latest
	 * 
	 * @return
	 */
	public String getAuto_offset_reset() {
		if (StringUtils.isBlank(auto_offset_reset))
			return "latest";
		return auto_offset_reset;
	}

	/**
	 * default latest
	 * earliest、latest
	 * 
	 * @return
	 */
	public KafkaVo setAuto_offset_reset(String auto_offset_reset) {
		this.auto_offset_reset = auto_offset_reset;
		return this;
	}

	public String getTopic() {
		return topic;
	}

	public KafkaVo setTopic(String topic) {
		this.topic = topic;
		return this;
	}

	private String bootstrap_servers;
	private String key_deserializer;
	private String value_deserializer;
	private String group_id;
	private String enable_auto_commit;
	private String max_partition_fetch_bytes;
	private String heartbeat_interval_ms;
	private String session_timeout_ms;
	private String auto_offset_reset;
	private String topic;
	private long timeout;

}
