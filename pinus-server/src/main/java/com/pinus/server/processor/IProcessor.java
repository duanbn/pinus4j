package com.pinus.server.processor;

import com.pinus.core.message.Message;

public interface IProcessor {

	public Message process(Message in);

}
