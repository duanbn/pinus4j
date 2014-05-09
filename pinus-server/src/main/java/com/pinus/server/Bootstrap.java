package com.pinus.server;

import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class Bootstrap implements ApplicationContextAware {

	public static final Logger LOG = Logger.getLogger(Bootstrap.class);

	private int port;

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	private ApplicationContext springCtx;

	public void startup() throws Exception {
		IoAcceptor acceptor = new NioSocketAcceptor();

		acceptor.getFilterChain().addLast("logger", new LoggingFilter());
		acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new RpcProtocolCodecFactory()));

		ProcessorLoader pl = new ProcessorLoader(this.springCtx);
		pl.load();
		acceptor.setHandler(new DispatchHandler(pl));

		acceptor.getSessionConfig().setReadBufferSize(4096);
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);

		acceptor.bind(new InetSocketAddress(this.port));

		LOG.info("startup done listen port " + this.port);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.springCtx = applicationContext;
	}

}
