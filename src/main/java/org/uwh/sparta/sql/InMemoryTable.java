package org.uwh.sparta.sql;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class InMemoryTable<E> extends AbstractTable implements ScannableTable {
    private final List<E> data;
    private final List<Tuple3<Function<E,Object>, String, SqlTypeName>> fields;

    public InMemoryTable(List<E> data, List<Tuple3<Function<E,Object>, String, SqlTypeName>> fields) {
        this.data = data;
        this.fields = fields;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        List<RelDataType> types = new ArrayList<>();
        List<String> names = new ArrayList<>();

        for (Tuple3<Function<E,Object>, String, SqlTypeName> t : fields) {
            names.add(t.f1);
            types.add(relDataTypeFactory.createSqlType(t.f2));
        }

        return relDataTypeFactory.createStructType(types, names);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new Enumerator<Object[]>() {
                    private int index = -1;

                    @Override
                    public Object[] current() {
                        Object[] res = new Object[fields.size()];

                        for (int i=0; i<fields.size(); i++) {
                            res[i] = fields.get(i).f0.apply(data.get(index));
                        }

                        return res;
                    }

                    @Override
                    public boolean moveNext() {
                        index = index + 1;
                        return index < data.size();
                    }

                    @Override
                    public void reset() {
                        index = 0;
                    }

                    @Override
                    public void close() {}
                };
            }
        };
    }
}
