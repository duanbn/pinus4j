package com.pinus.server.service;

public interface IEchoService {

	public String echo(String value);
	
	public void discard();
	
	public void discard(String value);
	
}
