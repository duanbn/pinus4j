package com.pinus.demo;

public interface IEchoService {

	public String echo(String value);

	public void discard();

	public void discard(String value);

	public void error() throws Exception;

	public void rterror();

}
