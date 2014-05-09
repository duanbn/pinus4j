package com.pinus.demo;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:applicationContext.xml" })
public class MainTest {

	@Resource(name = "echo")
	private IEchoService echoService;

	@Test
	public void test() {
		String value = echoService.echo("hello pinus");
		System.out.println(value);
		echoService.discard();
		echoService.discard("discard pinus");

		try {
			echoService.error();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			echoService.rterror();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
