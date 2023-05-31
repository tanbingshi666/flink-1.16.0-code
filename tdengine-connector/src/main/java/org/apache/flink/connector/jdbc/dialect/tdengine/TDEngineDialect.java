package org.apache.flink.connector.jdbc.dialect.tdengine;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.internal.converter.TDEngineRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

@Internal
public class TDEngineDialect extends AbstractDialect {
    private static final long serialVersionUID = 1L;
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 0;
    private static final int MAX_DECIMAL_PRECISION = 65;
    private static final int MIN_DECIMAL_PRECISION = 1;
    private static final String REWRITE_BATCHED_STATEMENTS = "rewriteBatchedStatements";

    public TDEngineDialect() {
    }

    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new TDEngineRowConverter(rowType);
    }

    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    public Optional<String> defaultDriverName() {
        return Optional.of("com.taosdata.jdbc.rs.RestfulDriver");
    }

    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String updateClause = (String) Arrays.stream(fieldNames).map((f) -> {
            return this.quoteIdentifier(f) + "=VALUES(" + this.quoteIdentifier(f) + ")";
        }).collect(Collectors.joining(", "));
        return Optional.of(this.getInsertIntoStatement(tableName, fieldNames) + " ON DUPLICATE KEY UPDATE " + updateClause);
    }

    public String dialectName() {
        return "TDEngine";
    }

    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(1, 65));
    }

    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(0, 6));
    }

    public Set<LogicalTypeRoot> supportedTypes() {
        return EnumSet.of(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR, LogicalTypeRoot.BOOLEAN, LogicalTypeRoot.VARBINARY, LogicalTypeRoot.DECIMAL, LogicalTypeRoot.TINYINT, LogicalTypeRoot.SMALLINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BIGINT, LogicalTypeRoot.FLOAT, LogicalTypeRoot.DOUBLE, LogicalTypeRoot.DATE, LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
    }

    public String appendDefaultUrlProperties(String url) {
        if (!url.contains("rewriteBatchedStatements")) {
            String defaultUrlProperties = "rewriteBatchedStatements=true";
            return url.contains("?") ? url + "&" + defaultUrlProperties : url + "?" + defaultUrlProperties;
        } else {
            return url;
        }
    }
}

