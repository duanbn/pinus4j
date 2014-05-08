package com.pinus.client;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.io.IOException;

import com.pinus.client.connection.Connection;
import com.pinus.client.connection.ConnectionPool;
import com.pinus.core.message.*;
import com.pinus.core.util.StringUtil;

import org.apache.log4j.Logger;

/**
 * 客户端动态代理执行类.
 * 
 * @author duanbn
 * @since 1.0
 */
public class RpcInvocationHandler implements InvocationHandler {

	public static final Logger log = Logger.getLogger(RpcInvocationHandler.class);

	private String serviceName;
	private ConnectionPool cpool;

	public RpcInvocationHandler(String serviceName, ConnectionPool cpool) {
		this.serviceName = serviceName;
		this.cpool = cpool;
	}

	/**
	 * 执行调用方法.
	 */
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

		// 从连接池中获取一个连接
		Connection conn = cpool.getConnection();
		if (conn == null) {
			throw new RuntimeException("获取连接失败.");
		}

		RpcMessage msg = new RpcMessage();
		msg.setServiceName(serviceName);
		msg.setMethodName(method.getName());
		msg.setArgs(args);

		conn.send(msg);
		Object returnVal = conn.receive().getBody();
		if (returnVal == null) {
			return returnVal;
		}

		if (returnVal instanceof Exception) {
			throw (Exception) returnVal;
		}

		return returnVal;
	}
}
