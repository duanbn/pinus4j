package com.pinus.client.connection;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

/**
 * 抽象的生命周期. 有声明周期的组件可以直接继承这个类并实现相关的抽象方法.
 * 
 * @author duanbn
 * @since 1.1
 */
public abstract class AbstractLifecycle implements Lifecycle {
    public static final Logger log = Logger.getLogger(AbstractLifecycle.class);

    private AtomicInteger state = new AtomicInteger();

    /**
     * 构造方法.
     */
    public AbstractLifecycle() {
        // 初始化时组件处于停止状态.
        setState(SHUTDOWN);
    }

    @Override
    public void startup() {
        if (isShutdown()) {
            try {
                doInit();
                setState(RUNNING);
                doStartup();
            } catch (Exception e) {
                setState(SHUTDOWN);
                log.error("组件启动失败", e);
            }
        } else {
            log.warn("组件不是关闭状态无法启动");
        }
    }

    @Override
    public void pause() {
        if (isRunning()) {
            int currentState = getState();
            try {
                setState(PAUSE);
                doPause();
            } catch (Exception e) {
                setState(currentState);
                log.error("组件暂停失败", e);
            }
        } else {
            log.warn("组件没有处于运行状态，无法暂停");
        }
    }

    @Override
    public void unpause() {
        int currentState = getState();
        if (isPause()) {
            try {
                setState(RUNNING);
                doUnpause();
            } catch (Exception e) {
                setState(currentState);
                log.error("组件从暂停状态恢复失败", e);
            }
        } else {
            log.warn("组件没有处于暂停状态，无法恢复暂停");
        }
    }

    @Override
    public void shutdown() {
        int currentState = getState();
        try {
            setState(SHUTDOWN);
            doShutdown();
        } catch (Exception e) {
            setState(currentState);
            log.error("关闭组件失败", e);
        }
    }

    @Override
    public boolean isRunning() {
        return getState() == RUNNING;
    }

    @Override
    public boolean isPause() {
        return getState() == PAUSE;
    }

    @Override
    public boolean isShutdown() {
        return getState() == SHUTDOWN;
    }

    /**
     * 设置状态.
     * 
     * @param state 状态值.
     */
    public void setState(int state) {
        this.state.getAndSet(state);
    }

    /**
     * 获取当前组件的状态.
     * 
     * @return 组件的状态
     */
    public int getState() {
        return this.state.get();
    }

    /**
     * 组件启动前初始化时执行此方法.
     */
    public void doInit() throws Exception {
    }

    /**
     * 组件启动的时候执行的此方法.
     */
    public abstract void doStartup() throws Exception;

    /**
     * 组件暂停时执行此方法.
     */
    public abstract void doPause() throws Exception;

    /**
     * 取消暂停的时候执行此方法.
     */
    public abstract void doUnpause() throws Exception;

    /**
     * 组件关闭时执行此方法.
     */
    public abstract void doShutdown() throws Exception;

}
