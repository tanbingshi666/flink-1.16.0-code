package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

@Internal
public class TDEngineRowConverter extends AbstractJdbcRowConverter {
    private static final long serialVersionUID = 1L;

    public String converterName() {
        return "TDEngine";
    }

    public TDEngineRowConverter(RowType rowType) {
        super(rowType);
    }
}
