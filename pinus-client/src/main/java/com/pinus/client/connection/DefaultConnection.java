package com.pinus.client.connection;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.pinus.core.message.*;
import com.pinus.core.ser.*;

import org.apache.log4j.Logger;

/**
 * 默认连接器.
 *
 * @author duanbn
 * @since 1.1
 */
public class DefaultConnection extends AbstractConnection
{
    public static final Logger log = Logger.getLogger(DefaultConnection.class);

    /**
     * 创建一个连接
     *
     * @param host 目标地址
     * @param port 目标端口
     *
     * @throws IOException 连接异常
     */
    public DefaultConnection(String host, int port) throws IOException
    {
        super(host, port);
    }
    
    public void close()
    {
        try {
            if (isOpen())
                this.channel.close();
        } catch (IOException e) {
            log.warn("关闭通道失败");
        }
    }

}
