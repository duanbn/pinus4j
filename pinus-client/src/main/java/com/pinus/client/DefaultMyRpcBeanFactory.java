package com.pinus.client;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.pinus.client.connection.ConnectionPool;
import com.pinus.client.connection.MyConnectionPool;

/**
 * 默认的rpcbean工厂实现.
 * 
 * @author duanbn
 * @since 1.1
 */
public class DefaultMyRpcBeanFactory implements MyRpcBeanFactory {
	public static final Logger log = Logger.getLogger(DefaultMyRpcBeanFactory.class);

	/**
	 * 连接池.
	 */
	private ConnectionPool cpool;

	private static final Map<Class<?>, RpcInvocationHandler> handlerCache = new ConcurrentHashMap<Class<?>, RpcInvocationHandler>();

	public DefaultMyRpcBeanFactory(String host, int port) {
		ConnectionPool cpool = new MyConnectionPool(host, port);
		cpool.startup();
		this.cpool = cpool;
	}

	@SuppressWarnings("unchecked")
	public <T> T getRpcBean(String serviceName, Class<T> clazz) {
		RpcInvocationHandler rpcInvokeHandler = handlerCache.get(clazz);
		if (rpcInvokeHandler == null) {
			rpcInvokeHandler = new RpcInvocationHandler(serviceName, this.cpool);
			handlerCache.put(clazz, rpcInvokeHandler);
		}

		T instance = (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[] { clazz }, rpcInvokeHandler);

		return instance;
	}

	public void destroy() {
		if (cpool != null && !cpool.isShutdown()) {
			cpool.shutdown();
		}
	}

}
