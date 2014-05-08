package com.pinus.core.message;

import com.pinus.core.ser.Serializable;

public class Message implements Serializable {

	protected Object body;

	public void setBody(Object body) {
		this.body = body;
	}

	public Object getBody() {
		return this.body;
	}
}
