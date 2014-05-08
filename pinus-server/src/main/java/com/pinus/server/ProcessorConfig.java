package com.pinus.server;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pinus.core.message.RpcMessage;
import com.pinus.server.processor.IProcessor;
import com.pinus.server.processor.RpcProcessor;

public class ProcessorConfig {

	public static final Logger LOG = Logger.getLogger(ProcessorConfig.class);

	public static final Map<Class<?>, IProcessor> processorConfig = new HashMap<Class<?>, IProcessor>(1);

	static {
		IProcessor rpcProcessor = new RpcProcessor();
		processorConfig.put(RpcMessage.class, rpcProcessor);
		LOG.info("load rpcProcessor done");
	}

	public static IProcessor get(Class<?> msgClazz) {
		return processorConfig.get(msgClazz);
	}
}
