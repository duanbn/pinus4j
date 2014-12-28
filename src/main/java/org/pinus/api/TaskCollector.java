package org.pinus.api;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 任务收集器.
 * 线程安全.
 *
 * @author duanbn
 */
public class TaskCollector extends ConcurrentHashMap {

	public static final Logger LOG = LoggerFactory
			.getLogger(TaskCollector.class);

	private static final Object[] numberLock = new Object[0];

	/**
	 * 增加.
	 */
	public void incr(Object key, int num) {
		synchronized (numberLock) {
			try {
				Number val = (Number) get(key);
                if (val == null) {
                    put(key, num);
                } else {
                    val = val.longValue() + num;
                    put(key, val);
                }
			} catch (Exception e) {
				LOG.warn("incr number failure key " + key + " "
						+ e.getMessage());
			}
		}
	}

	/**
	 * 减少.
	 */
	public void decr(Object key, int num) {
		synchronized (numberLock) {
			try {
				Number val = (Number) get(key);
                if (val == null) {
                    put(key, 0 - num);
                } else {
                    val = val.longValue() - num;
                    put(key, val);
                }
			} catch (Exception e) {
				LOG.warn("decr number failure key " + key + " "
						+ e.getMessage());
			}
		}
	}

}
