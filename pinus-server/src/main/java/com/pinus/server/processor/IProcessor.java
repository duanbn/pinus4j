package com.pinus.server.processor;

import org.springframework.context.ApplicationContext;

import com.pinus.core.message.Message;

public interface IProcessor {

	public Message process(Message in);
	
	public void setSpringCtx(ApplicationContext springCtx);

}
