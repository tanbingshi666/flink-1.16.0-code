package org.apache.flink.connector.jdbc.dialect.tdengine;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory;

public class TDEngineDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:TAOS-RS");
    }

    @Override
    public JdbcDialect create() {
        return new TDEngineDialect();
    }
}
