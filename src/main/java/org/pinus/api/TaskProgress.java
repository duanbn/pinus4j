package org.pinus.api;

/**
 * 数据处理任务进度.
 * 
 * @author duanbn
 *
 */
public class TaskProgress {
	
	/**
	 * 需要处理的总记录数
	 */
	private long total;

	/**
	 * 已经处理的记录数
	 */
	private long count;

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

}
