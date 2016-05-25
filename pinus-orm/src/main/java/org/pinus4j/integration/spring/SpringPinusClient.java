package org.pinus4j.integration.spring;

import javax.sql.DataSource;

import org.pinus4j.api.DefaultPinusClient;
import org.pinus4j.api.PinusClient;
import org.pinus4j.cluster.container.IContainer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class SpringPinusClient extends DefaultPinusClient implements PinusClient, ApplicationContextAware,
        InitializingBean, DisposableBean {

    private ApplicationContext appCtx;

    @Override
    public void afterPropertiesSet() throws Exception {
        super.init();

        // inject datasource
        DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) appCtx.getAutowireCapableBeanFactory();

        IContainer<DataSource> dsC = this.getDBCluster().getDBConnectionPool().getAllDataSources();

        BeanDefinition dsBean = null;
        DataSource ds = null;
        for (String key : dsC.keys()) {
            ds = dsC.find(key);

            dsBean = new RootBeanDefinition(ds.getClass());

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

}
