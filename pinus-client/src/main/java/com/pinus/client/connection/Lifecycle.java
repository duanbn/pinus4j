package com.pinus.client.connection;

/**
 * 表示一个组件的生命周期.
 * 我们可以使用生命周期对相关的组件进行管理.通常一个组件的生命周期存在四个状态.
 * 初始化状态、在启动组件之前处于此状态<br>
 * 启动状态，组件已经启动并且正在运行，组件只有在关闭状态下才能执行启动<br>
 * 暂停状态，组件由于某种情况需要被暂停，暂停状态不会提供任何服务，组件只有在运行
 * 状态下才能被关闭<br>
 * 关闭状态，组件被销毁，需要释放之前已经占用的资源，组件在任何情况下都可以被停止.
 *
 * @author duanbn
 * @since 1.0
 */
public interface Lifecycle
{

    public static final int RUNNING = 1 << 0;

    public static final int PAUSE = 1 << 1;

    public static final int SHUTDOWN = 1 << 2;

    /**
     * 启动.
     */
    public void startup();

    /**
     * 暂停.
     */
    public void pause();

    /**
     * 取消暂停.
     */
    public void unpause();

    /**
     * 关闭.
     */
    public void shutdown();

    /**
     * 是否正在运行.
     *
     * @return true:是, false:否
     */
    public boolean isRunning();

    /**
     * 是否暂停.
     *
     * @return true:是, false:否
     */
    public boolean isPause();

    /**
     * 是否关闭.
     *
     * @return true:是, false:否
     */
    public boolean isShutdown();

}
