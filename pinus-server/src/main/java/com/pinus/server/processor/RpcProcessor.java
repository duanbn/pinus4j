package com.pinus.server.processor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.springframework.context.ApplicationContext;

import com.pinus.core.message.Message;
import com.pinus.core.message.RpcMessage;

public class RpcProcessor implements IProcessor {

	private ApplicationContext springCtx;

	@Override
	public Message process(Message in) {

		RpcMessage msg = (RpcMessage) in;
		Message out = new Message();

		String serviceName = msg.getServiceName();
		String methodName = msg.getMethodName();
		Object[] args = msg.getArgs();
		Class<?>[] argClass = null;
		if (args != null) {
			argClass = new Class<?>[args.length];
			for (int i = 0; i < args.length; i++) {
				argClass[i] = args[i].getClass();
			}
		}

		Object service = this.springCtx.getBean(serviceName);

		try {
			Method m = service.getClass().getMethod(methodName, argClass);
			Object returnVal = m.invoke(service, args);
			out.setBody(returnVal);
		} catch (InvocationTargetException e) {
			out.setBody(e.getTargetException());
		} catch (Exception e) {
			out.setBody(e);
		}

		return out;
	}

	@Override
	public void setSpringCtx(ApplicationContext springCtx) {
		this.springCtx = springCtx;
	}
}
