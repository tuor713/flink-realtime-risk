package org.uwh.sparta;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.uwh.IssuerRiskLine;
import org.uwh.flink.data.generic.Field;

import java.util.List;

public class Fields {
    public static final Field<String> F_ISSUER_ID = new Field<>("issuer","id", String.class, Types.STRING);
    public static final Field<String> F_ISSUER_NAME = new Field<>("issuer", "name", String.class, Types.STRING);
    public static final Field<String> F_ISSUER_ULTIMATE_PARENT_ID = new Field<>("issuer", "ultimate-parent-id", F_ISSUER_ID);
    public static final Field<String> F_ISSUER_ULTIMATE_PARENT_NAME = new Field<>("issuer", "ultimate-parent-name", F_ISSUER_NAME);

    public static final Field<String> F_ACCOUNT_MNEMONIC = new Field<>("account", "mnemonic", String.class, Types.STRING);
    public static final Field<String> F_ACCOUNT_STRATEGY_CODE = new Field<>("account", "strategy-code", String.class, Types.STRING);

    public static final Field<String> F_POS_UID = new Field<>("position", "uid", String.class, Types.STRING);
    public static final Field<String> F_POS_UID_TYPE = new Field<>("position", "uid-type", String.class, Types.STRING);
    public static final Field<String> F_POS_PRODUCT_TYPE = new Field<>("position", "product-type", String.class, Types.STRING);

    public static final Field<Double> F_RISK_ISSUER_CR01 = new Field<>("issuer-risk","cr01", Double.class, Types.DOUBLE);
    public static final Field<Double> F_RISK_ISSUER_JTD = new Field<>("issuer-risk","jtd", Double.class, Types.DOUBLE);

    public static final Field<List<IssuerRiskLine>> F_RISK_ISSUER_RISKS = new Field<List<IssuerRiskLine>>("issuer-risk", "risks", (Class) List.class, new ListTypeInfo<>(IssuerRiskLine.class));

    public static final Field<Double> F_RISK_LIMIT_CR01_THRESHOLD = new Field<>("risk-limit", "cr01-threshold", Double.class, Types.DOUBLE);
    public static final Field<Double> F_RISK_LIMIT_JTD_THRESHOLD = new Field<>("risk-limit", "jtd-threshold", Double.class, Types.DOUBLE);

    public static final Field<Double> F_RISK_LIMIT_CR01_UTILIZATION = new Field<>("risk-limit", "cr01-utilization", Double.class, Types.DOUBLE);
    public static final Field<Double> F_RISK_LIMIT_JTD_UTILIZATION = new Field<>("risk-limit", "jtd-utilization", Double.class, Types.DOUBLE);
}
