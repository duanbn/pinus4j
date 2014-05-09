package com.pinus.client;

import java.lang.reflect.Proxy;

import com.pinus.client.connection.ConnectionPool;
import com.pinus.client.connection.MyConnectionPool;

public class PinusFactoryBean {

	private ConnectionPool cpool;

	private String serviceName;

	private String interfaceClass;

	public PinusFactoryBean(String host, int port) {
		this.cpool = MyConnectionPool.getInstance(host, port);
		this.cpool.startup();
	}

	public Object create(String serviceName, String interfaceClass) throws ClassNotFoundException {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		Class<?> clazz = Class.forName(interfaceClass);
		RpcInvocationHandler rpcInvokeHandler = new RpcInvocationHandler(serviceName, this.cpool);
		return Proxy.newProxyInstance(cl, new Class<?>[] { clazz }, rpcInvokeHandler);
	}

	public void destroy() {
		this.cpool.shutdown();
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getInterfaceClass() {
		return interfaceClass;
	}

	public void setInterfaceClass(String interfaceClass) {
		this.interfaceClass = interfaceClass;
	}

}
