package org.pinus4j.integration.spring;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.pinus4j.api.DefaultPinusClient;
import org.pinus4j.api.PinusClient;
import org.pinus4j.cluster.container.IContainer;
import org.pinus4j.cluster.cp.IDBConnectionPool;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.alibaba.druid.pool.DruidDataSource;

public class SpringPinusClient extends DefaultPinusClient implements PinusClient, ApplicationContextAware,
        InitializingBean, DisposableBean {

    private ApplicationContext appCtx;

    @Override
    public void afterPropertiesSet() throws Exception {
        super.init();

        // inject datasource
        DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) appCtx.getAutowireCapableBeanFactory();

        IDBConnectionPool dbConnectionPool = this.getDBCluster().getDBConnectionPool();
        IContainer<DataSource> dsC = dbConnectionPool.getAllDataSources();

        BeanDefinition dsBean = null;
        DataSource ds = null;
        for (String key : dsC.keys()) {
            ds = dsC.find(key);

            dsBean = createBeanDef(ds);
            ((AbstractBeanDefinition) dsBean).setDestroyMethodName("close");

            beanFactory.registerBeanDefinition(key, dsBean);
            if (LOG.isDebugEnabled()) {
                LOG.debug("reg datasouce {} to spring container done", key);
            }
        }
    }

    @Override
    public void destroy() {
        super.destroy();
    }

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.appCtx = ctx;
    }

    private BeanDefinition createBeanDef(DataSource ds) {
        BeanDefinition dsBean = new RootBeanDefinition(ds.getClass());

        if (ds instanceof DruidDataSource) {
            DruidDataSource dds = (DruidDataSource) ds;
            dsBean.getPropertyValues().add("driverClassName", dds.getDriverClassName());
            dsBean.getPropertyValues().add("url", dds.getUrl());
            dsBean.getPropertyValues().add("username", dds.getUsername());
            dsBean.getPropertyValues().add("password", dds.getPassword());
            dsBean.getPropertyValues().add("maxActive", dds.getMaxActive());
            dsBean.getPropertyValues().add("minIdle", dds.getMinIdle());
            dsBean.getPropertyValues().add("initialSize", dds.getInitialSize());
            dsBean.getPropertyValues().add("removeAbandoned", dds.isRemoveAbandoned());
            dsBean.getPropertyValues().add("removeAbandonedTimeout", dds.getRemoveAbandonedTimeout());
            dsBean.getPropertyValues().add("maxWait", dds.getMaxWait());
            dsBean.getPropertyValues().add("testWhileIdle", dds.isTestWhileIdle());
            dsBean.getPropertyValues().add("testOnBorrow", dds.isTestOnBorrow());
            dsBean.getPropertyValues().add("testOnReturn", dds.isTestOnReturn());
            dsBean.getPropertyValues().add("timeBetweenEvictionRunsMillis", dds.getTimeBetweenEvictionRunsMillis());
            dsBean.getPropertyValues().add("numTestsPerEvictionRun", dds.getNumTestsPerEvictionRun());
            dsBean.getPropertyValues().add("minEvictableIdleTimeMillis", dds.getMinEvictableIdleTimeMillis());
            dsBean.getPropertyValues().add("poolPreparedStatements", dds.isPoolPreparedStatements());
        } else if (ds instanceof BasicDataSource) {
            BasicDataSource bds = (BasicDataSource) ds;
            dsBean.getPropertyValues().add("driverClassName", bds.getDriverClassName());
            dsBean.getPropertyValues().add("url", bds.getUrl());
            dsBean.getPropertyValues().add("username", bds.getUsername());
            dsBean.getPropertyValues().add("password", bds.getPassword());
            dsBean.getPropertyValues().add("maxActive", bds.getMaxActive());
            dsBean.getPropertyValues().add("minIdle", bds.getMinIdle());
            dsBean.getPropertyValues().add("initialSize", bds.getInitialSize());
            dsBean.getPropertyValues().add("removeAbandoned", bds.getRemoveAbandoned());
            dsBean.getPropertyValues().add("removeAbandonedTimeout", bds.getRemoveAbandonedTimeout());
            dsBean.getPropertyValues().add("maxWait", bds.getMaxWait());
            dsBean.getPropertyValues().add("testWhileIdle", bds.getTestWhileIdle());
            dsBean.getPropertyValues().add("testOnBorrow", bds.getTestOnBorrow());
            dsBean.getPropertyValues().add("testOnReturn", bds.getTestOnReturn());
            dsBean.getPropertyValues().add("timeBetweenEvictionRunsMillis", bds.getTimeBetweenEvictionRunsMillis());
            dsBean.getPropertyValues().add("numTestsPerEvictionRun", bds.getNumTestsPerEvictionRun());
            dsBean.getPropertyValues().add("minEvictableIdleTimeMillis", bds.getMinEvictableIdleTimeMillis());
            dsBean.getPropertyValues().add("poolPreparedStatements", bds.isPoolPreparedStatements());
        } else {
            throw new IllegalStateException("unknow datasource type " + ds.getClass());
        }

        return dsBean;
    }
}
