package com.demo.spring_cache_ignite.config.factory;

import com.demo.spring_cache_ignite.constant.DataSourceConstants;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;

import javax.cache.configuration.Factory;

public class MyCacheStoreSessionListenerFactory implements Factory {

    @Override
    public CacheStoreSessionListener create() {
        // Data Source
        HikariConfig config = new HikariConfig();
        config.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
        config.addDataSourceProperty("serverName", DataSourceConstants.DATASOURCE_CONFIG_PROP_HOSTNAME);
        config.addDataSourceProperty("databaseName", DataSourceConstants.DATASOURCE_CONFIG_PROP_DATABASE);
        config.addDataSourceProperty("user", DataSourceConstants.DATASOURCE_CONFIG_PROP_USERNAME);
        config.addDataSourceProperty("password", DataSourceConstants.DATASOURCE_CONFIG_PROP_PASSWORD);
        config.setMaximumPoolSize(30);

        HikariDataSource dataSource = new HikariDataSource(config);
        CacheJdbcStoreSessionListener listener = new CacheJdbcStoreSessionListener();
        listener.setDataSource(dataSource);

        return listener;
    }
}
