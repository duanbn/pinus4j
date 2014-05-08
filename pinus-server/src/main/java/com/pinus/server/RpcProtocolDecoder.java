package com.pinus.server;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

public class RpcProtocolDecoder implements ProtocolDecoder {

	@Override
	public void decode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
		int pkgLenth = in.getInt();

		IoBuffer pkg = IoBuffer.allocate(pkgLenth);
		while (pkg.hasRemaining()) {
			pkg.put(in);
		}

		out.write(pkg.array());
	}

	@Override
	public void finishDecode(IoSession session, ProtocolDecoderOutput out) throws Exception {
	}

	@Override
	public void dispose(IoSession session) throws Exception {
	}
}
