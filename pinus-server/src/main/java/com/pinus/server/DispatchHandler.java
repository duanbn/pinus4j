package com.pinus.server;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;

import com.pinus.core.message.Message;
import com.pinus.core.ser.Deserializer;
import com.pinus.core.ser.MyDeserializer;
import com.pinus.core.ser.MySerializer;
import com.pinus.core.ser.Serializer;
import com.pinus.server.processor.IProcessor;

public class DispatchHandler extends IoHandlerAdapter {
	private Serializer ser;
	private Deserializer deser;

	public DispatchHandler() {
		this.ser = MySerializer.getInstance();
		this.deser = MyDeserializer.getInstance();
	}

	@Override
	public void messageReceived(IoSession session, Object message) throws Exception {
		byte[] pkg = (byte[]) message;
		Message in = deser.deser(pkg, Message.class);

		IProcessor processor = ProcessorConfig.get(in.getClass());
		Message out = processor.process(in);

		pkg = ser.ser(out);

		IoBuffer buf = IoBuffer.allocate(4 + pkg.length);
		buf.putInt(pkg.length).put(pkg);
		buf.flip();
		session.write(buf);
	}

	@Override
	public void sessionIdle(IoSession session, IdleStatus status) throws Exception {
		System.out.println("IDLE " + session.getIdleCount(status));
	}
}
