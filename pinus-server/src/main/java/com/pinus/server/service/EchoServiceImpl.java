package com.pinus.server.service;

import org.springframework.stereotype.Service;

@Service
public class EchoServiceImpl implements IEchoService {

	@Override
	public String echo(String value) {
		return value;
	}

	@Override
	public void discard() {
		System.out.println("invoke discard");
	}

	@Override
	public void discard(String value) {
		System.out.println("invoke discard " + value);
	}

}
