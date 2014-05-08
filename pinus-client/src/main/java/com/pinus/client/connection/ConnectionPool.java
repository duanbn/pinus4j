package com.pinus.client.connection;


/**
 * myrpc连接池. 一个连接分为激活状态和未激活状态.当通过线程池获取一个连接此连接在获取时将会被激活
 * 当调用连接的关闭方法时，连接所持有的通过并不会被关闭，而是将次连接的状态设置为未被激活<br>
 * 连接池有一个参数可以进行设置<br>
 * 最小连接数：连接池拥有的最小的连接个数，这个也可以被称为核心连接数，连接池中的连接数大于等于这个连接数<br>
 * 最大连接数：连接池用户的最大的连接个数，当达到最大连接数时从连接池中获取连接将返回空<br>
 * 
 * @author duanbn
 * @since 1.1
 */
public interface ConnectionPool extends Lifecycle {

    /**
     * 设置连接池的最小连接数.
     * 
     * @param num 最小连接数
     */
    public void setMinConnect(int num);

    /**
     * 获取设置的最小连接数.
     * 
     * @return 最小连接数
     */
    public int getMinConnect();

    /**
     * 设置连接池的最大连接数.
     * 
     * @param num 最大连接数
     */
    public void setMaxConnect(int num);

    /**
     * 获取最大连接数.
     * 
     * @return 最大连接数
     */
    public int getMaxConnect();

    /**
     * 获取当前活动的连接数.
     * 
     * @return 活动的连接数.
     */
    public int getActiveConnect();

    /**
     * 获取一个连接，当连接池已经达到最大连接数或者创建连接失败则返回null. 并将次连接设置为激活状态.
     * 
     * @return 连接引用
     */
    public Connection getConnection();

}
