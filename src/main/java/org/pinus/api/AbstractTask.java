package org.pinus.api;

/**
 * 抽象任务对象.
 * 
 * @author duanbn
 *
 * @param <T>
 */
public abstract class AbstractTask<T> implements ITask<T> {

	@Override
	public void init() throws Exception {
		// do noting, override by subclass
	}

	@Override
	public void finish() throws Exception {
		// do noting, override by subclass
	}
	
	@Override
	public int taskBuffer() {
		return 0;
	}

}
