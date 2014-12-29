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
	public void init() {
		// do noting, override by subclass
	}

	@Override
	public void finish() {
		// do noting, override by subclass
	}

}
