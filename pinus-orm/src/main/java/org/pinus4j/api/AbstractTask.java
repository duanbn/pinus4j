/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.api;

import org.pinus4j.task.ITask;

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
	public void afterBatch() {
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
