package com.pinus.client.connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;
import com.pinus.core.message.Message;
import com.pinus.core.ser.DeserializeException;
import com.pinus.core.ser.Deserializer;
import com.pinus.core.ser.MyDeserializer;
import com.pinus.core.ser.MySerializer;
import com.pinus.core.ser.SerializeException;
import com.pinus.core.ser.Serializer;

/**
 * 抽象连接器.
 * 
 * @author duanbn
 * @since 1.1
 */
public abstract class AbstractConnection implements Connection {
    public static final Logger log = Logger.getLogger(AbstractConnection.class);

    /**
     * socket通道.
     */
    protected SocketChannel channel;

    /**
     * 消息头缓冲.
     */
    private static final int HEAD_BODY_LENGTH = 4;
    protected ByteBuffer headBuf = ByteBuffer.allocate(4);

    /**
     * 序列化工具.
     */
    protected Serializer ser;

    /**
     * 反序列化工具.
     */
    protected Deserializer deser;

    protected String remoteAddress;
    protected int remotePort;

    protected String localAddress;
    protected int localPort;

    private Object readLock = new Object();
    private Object writeLock = new Object();

    /**
     * 创建一个连接.
     * 
     * @param host 目标地址
     * @param port 目标端口
     * 
     * @throws IOException 连接异常
     */
    public AbstractConnection(String host, int port) throws IOException {
        this.remoteAddress = host;
        this.remotePort = port;

        this.ser = MySerializer.getInstance();
        this.deser = MyDeserializer.getInstance();

        initChannel();
    }

    /**
     * 创建通道.
     */
    public void initChannel() throws IOException {
        channel = SocketChannel.open();
        channel.configureBlocking(false);

        if (!channel.isConnected()) {
            SocketAddress address = new InetSocketAddress(remoteAddress, remotePort);
            channel.connect(address);
            while (!channel.finishConnect()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }

        this.localAddress = channel.socket().getLocalAddress().getHostAddress();
        this.localPort = channel.socket().getLocalPort();
    }

    /**
     * 获取客户端到服务器端的网络延迟. 无法连接服务器时返回-1.
     */
    public int ping() {
        return -1;
    }

    public void send(Message message) throws IOException {
        synchronized (writeLock) {
            ensureOpen();

            // send message to server
            try {
                byte[] b = ser.ser(message);
                if (log.isDebugEnabled()) {
                    log.debug("send data body length=" + b.length);
                }
                int alloSize = b.length + HEAD_BODY_LENGTH;
                ByteBuffer writeBuf = ByteBuffer.allocate(alloSize);
                writeBuf.putInt(b.length).put(b);
                writeBuf.flip();
                while (writeBuf.hasRemaining()) {
                    channel.write(writeBuf);
                }
                writeBuf.clear();
            } catch (SerializeException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                if (this.channel.isOpen()) {
                    this.channel.close();
                }
                throw e;
            }
        }
    }

    public Message receive() throws IOException {
        synchronized (readLock) {
            ensureOpen();

            // receive message from server.
            try {
                int readCount = 0;
                // read message length.
                while (headBuf.hasRemaining()) {
                    int c = channel.read(headBuf);
                    if (c == -1) {
                        if (this.channel.isOpen()) {
                            this.channel.close();
                        }
                        throw new IOException("连接关闭");
                    }
                    readCount += c;
                }
                int msgSize = headBuf.getInt(0);
                headBuf.clear();
                byte[] msgData = new byte[msgSize];
                ByteBuffer bodyBuf = ByteBuffer.allocate(msgSize);
                // read message data.
                while (readCount < msgSize) {
                    int c = channel.read(bodyBuf);
                    if (c == -1) {
                        if (this.channel.isOpen()) {
                            this.channel.close();
                        }
                        throw new IOException("连接关闭");
                    }
                    readCount += c;
                }
                bodyBuf.flip();
                bodyBuf.get(msgData);
                bodyBuf.clear();

                Message msg = deser.deser(msgData, Message.class);
                return msg;
            } catch (DeserializeException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                if (this.channel.isOpen()) {
                    this.channel.close();
                }
                throw e;
            }
        }
    }

    /**
     * 当通道处于关闭状态时重新连接.
     * 
     * @throws IOException 无法连接服务器.
     */
    public void ensureOpen() throws IOException {
        if (!isOpen()) {
            initChannel();
        }
    }

    /**
     * 此连接通道是否打开.
     */
    public boolean isOpen() {
        return this.channel.isOpen();
    }

    /**
     * Get channel.
     * 
     * @return channel as SocketChannel.
     */
    public SocketChannel getChannel() {
        return channel;
    }


    /**
     * Get localAddress.
     * 
     * @return localAddress as String.
     */
    public String getLocalAddress() {
        return localAddress;
    }

    /**
     * Set localAddress.
     * 
     * @param localAddress the value to set.
     */
    public void setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
    }

    /**
     * Get localPort.
     * 
     * @return localPort as int.
     */
    public int getLocalPort() {
        return localPort;
    }

    /**
     * Set localPort.
     * 
     * @param localPort the value to set.
     */
    public void setLocalPort(int localPort) {
        this.localPort = localPort;
    }

    /**
     * Get remoteAddress.
     * 
     * @return remoteAddress as String.
     */
    public String getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Set remoteAddress.
     * 
     * @param remoteAddress the value to set.
     */
    public void setRemoteAddress(String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    /**
     * Get remotePort.
     * 
     * @return remotePort as int.
     */
    public int getRemotePort() {
        return remotePort;
    }

    /**
     * Set remotePort.
     * 
     * @param remotePort the value to set.
     */
    public void setRemotePort(int remotePort) {
        this.remotePort = remotePort;
    }
}
