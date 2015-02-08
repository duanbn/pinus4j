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

package org.pinus4j.transaction;

/**
 * transaction manager interface.
 *
 * @author duanbn
 * @since 1.1.0
 */
public interface ITransactionManager {

	/**
	 * start a new transaction. current thread just has one transaction
	 * instance.
	 */
	ITransaction beginTransaction();

	/**
	 * get current thread transaction instance.
	 * 
	 * @return
	 */
	ITransaction getTransaction();

	void commit();

	void rollback();

}
