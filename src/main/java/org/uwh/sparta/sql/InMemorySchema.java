package org.uwh.sparta.sql;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.java.tuple.Tuple3;
import org.uwh.IssuerRisk;
import org.uwh.RiskPosition;
import org.uwh.sparta.Generators;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class InMemorySchema extends AbstractSchema {
    private List<RiskPosition> data = Generators.positionList(10000,100);
    private List<IssuerRisk> data2 = Generators.issuerRiskList(5000, 10000, 200);

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String,Table> tables = new HashMap<>();
        tables.put("RISKPOSITION", createRiskPositionTable());
        tables.put("ISSUERRISK", createIssuerRiskTable());
        return tables;
    }

    private Table createIssuerRiskTable() {
        return new InMemoryTable<>(data2,
                Arrays.asList(
                        new Tuple3<Function<IssuerRisk, Object>, String, SqlTypeName>(r -> r.getUIDType().name(), "UIDTYPE", SqlTypeName.VARCHAR),
                        new Tuple3<Function<IssuerRisk, Object>, String, SqlTypeName>(r -> r.getUID(), "UID", SqlTypeName.VARCHAR),
                        new Tuple3<Function<IssuerRisk, Object>, String, SqlTypeName>(r -> r.getCR01(), "CR01", SqlTypeName.DOUBLE),
                        new Tuple3<Function<IssuerRisk, Object>, String, SqlTypeName>(r -> r.getJTD(), "JTD", SqlTypeName.DOUBLE)
                ));
    }

    private Table createRiskPositionTable() {
        return new InMemoryTable<>(data,
                Arrays.asList(
                        new Tuple3<Function<RiskPosition, Object>, String, SqlTypeName>((pos) -> pos.getUIDType().name(), "UIDTYPE", SqlTypeName.VARCHAR),
                        new Tuple3<Function<RiskPosition, Object>, String, SqlTypeName>((pos) -> pos.getUID(), "UID", SqlTypeName.VARCHAR),
                        new Tuple3<Function<RiskPosition, Object>, String, SqlTypeName>((pos) -> pos.getFirmAccountMnemonic(), "FIRMACCOUNTMNEMONIC", SqlTypeName.VARCHAR),
                        new Tuple3<Function<RiskPosition, Object>, String, SqlTypeName>((pos) -> pos.getProductType(), "PRODUCTTYPE", SqlTypeName.VARCHAR)
                ));
    }
}
