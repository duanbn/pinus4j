package com.pinus.client.test;

import org.junit.Test;

import com.pinus.client.connection.Connection;
import com.pinus.client.connection.ConnectionPool;
import com.pinus.client.connection.MyConnectionPool;
import com.pinus.core.message.Message;
import com.pinus.core.message.RpcMessage;

public class ConnectionTest {

	private static final String HOST = "localhost";
	public static final int PORT = 9999;

	@Test
	public void test() {
		try {
			ConnectionPool cp = new MyConnectionPool(HOST, PORT);
			cp.startup();
			Connection conn = cp.getConnection();

			RpcMessage msg = new RpcMessage();
			msg.setServiceName("echoServiceImpl");
			msg.setMethodName("echo");
			msg.setArgs("hello pinus");
			conn.send(msg);

			Message in = conn.receive();
			System.out.println(in.getBody());

			msg = new RpcMessage();
			msg.setServiceName("echoServiceImpl");
			msg.setMethodName("discard");
			msg.setArgs(new Object[0]);
			conn.send(msg);

			in = conn.receive();
			System.out.println(in.getBody());

			msg = new RpcMessage();
			msg.setServiceName("echoServiceImpl");
			msg.setMethodName("discard");
			msg.setArgs("pinus");
			conn.send(msg);

			in = conn.receive();
			System.out.println(in.getBody());

			cp.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
