package com.pinus.server;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;

public class RpcProtocolCodecFactory implements ProtocolCodecFactory {

    private final RpcProtocolEncoder encoder;  
    private final RpcProtocolDecoder decoder;  

    public RpcProtocolCodecFactory() {  
        encoder = new RpcProtocolEncoder();  
        decoder = new RpcProtocolDecoder();  
    }  

    public ProtocolEncoder getEncoder(IoSession session) {  
        return encoder;  
    }  

    public ProtocolDecoder getDecoder(IoSession session) {  
        return decoder;  
    }

}
