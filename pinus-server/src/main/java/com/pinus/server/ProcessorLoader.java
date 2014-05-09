package com.pinus.server;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;

import com.pinus.core.message.RpcMessage;
import com.pinus.server.processor.IProcessor;
import com.pinus.server.processor.RpcProcessor;

public class ProcessorLoader {

	public static final Logger LOG = Logger.getLogger(ProcessorLoader.class);

	public static final Map<Class<?>, IProcessor> processorConfig = new HashMap<Class<?>, IProcessor>(1);

	private ApplicationContext springCtx;

	public ProcessorLoader(ApplicationContext springCtx) {
		this.springCtx = springCtx;
	}

	public void load() {
		IProcessor rpcProcessor = new RpcProcessor();
		rpcProcessor.setSpringCtx(springCtx);
		processorConfig.put(RpcMessage.class, rpcProcessor);
		LOG.info("load rpcProcessor done");
	}

	public IProcessor get(Class<?> msgClazz) {
		return processorConfig.get(msgClazz);
	}
}
