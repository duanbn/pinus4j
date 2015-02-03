package org.pinus4j.api;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 任务收集器. 线程安全.
 *
 * @author duanbn
 */
@Deprecated
public class TaskCollector extends ConcurrentHashMap {

	public static final Logger LOG = LoggerFactory.getLogger(TaskCollector.class);

	/**
	 * 增加.
	 */
	public void incr(Object key, int num) {
		try {
			Number val = (Number) get(key);
			if (val == null) {
				put(key, num);
			} else {
				val = val.longValue() + num;
				put(key, val);
			}
		} catch (Exception e) {
			LOG.warn("incr number failure key " + key + " " + e.getMessage());
		}
	}

	/**
	 * 减少.
	 */
	public void decr(Object key, int num) {
		try {
			Number val = (Number) get(key);
			if (val == null) {
				put(key, 0 - num);
			} else {
				val = val.longValue() - num;
				put(key, val);
			}
		} catch (Exception e) {
			LOG.warn("decr number failure key " + key + " " + e.getMessage());
		}
	}

}
