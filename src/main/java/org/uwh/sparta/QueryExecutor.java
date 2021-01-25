package org.uwh.sparta;

import org.apache.flink.api.java.tuple.Tuple2;
import org.uwh.IssuerRisk;
import org.uwh.Query;
import org.uwh.RiskPosition;

import java.util.*;
import java.util.function.Function;

public class QueryExecutor {
    private final Query q;
    private final Set<String> ids;
    private final Function<Tuple2<IssuerRisk, RiskPosition>, Boolean> where;

    public QueryExecutor(Query q) {
        this.q = q;
        if (q.getIds().isEmpty()) {
            this.ids = null;
        } else {
            this.ids = new HashSet<>(q.getIds());
        }
        this.where = buildWhere(q.getWhere());
    }

    public void accept(String key, Tuple2<IssuerRisk, RiskPosition> tuple) {
        if (this.ids != null && this.ids.contains(key)) {
            System.out.println(tuple);
        } else if (where.apply(tuple)) {
            System.out.println(tuple);
        }
    }

    private Function<Tuple2<IssuerRisk, RiskPosition>, Boolean> buildWhere(Map<String, List<String>> conditions) {
        final List<Function<Tuple2<IssuerRisk,RiskPosition>, String>> accessors = new ArrayList<>();
        final List<Set<String>> values = new ArrayList<>();
        for (Map.Entry<String, List<String>> e : conditions.entrySet()) {
            accessors.add(accessor(e.getKey()));
            values.add(new HashSet<>(e.getValue()));
        }

        return (t) -> {
            for (int i=0; i<accessors.size(); i++) {
                String val = accessors.get(i).apply(t);
                if (!values.get(i).contains(val)) {
                    return false;
                }
            }

            return true;
        };
    }

    private Function<Tuple2<IssuerRisk, RiskPosition>, String> accessor(String field) {
        if ("FirmAccountMnemonic".equals(field)) {
            return (t) -> t.f1.getFirmAccountMnemonic();
        }

        throw new IllegalArgumentException("Unsupported field "+field);
    }
}
