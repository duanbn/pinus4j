package com.pinus.client;


/**
 * 工厂方法，通过此类获取rpcbean的实例.
 * 
 * @author duanbn
 * @since 1.1
 */
public interface MyRpcBeanFactory {

    /**
     * 获取远程可执行的bean代理.
     * 
     * @param serviceName spring service name
     * @param clazz rpcbean的接口
     * 
     * @return rpcBean的代理对象
     */
    public <T> T getRpcBean(String serviceName, Class<T> clazz);

    /**
     * 销毁此工厂.
     */
    public void destroy();
}
