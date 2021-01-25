package org.uwh.sparta.sql;

import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelRunners;

import java.sql.*;
import java.util.Properties;

public class SqlTest {
    public static void main(String[] args) throws Exception {
        SchemaPlus root = Frameworks.createRootSchema(false);
        SchemaPlus def = root.add("default", new InMemorySchemaFactory().create(root, "default", null));
        FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(def).build();
        Planner planner = Frameworks.getPlanner(config);

        String sql = "select pos.firmaccountmnemonic, sum(risk.cr01) as cr01, sum(risk.jtd) as jtd, count(*) as c\n" +
                "from issuerrisk risk\n" +
                "join riskposition pos on pos.uid = risk.uid\n" +
                "group by pos.firmaccountmnemonic order by pos.firmaccountmnemonic";

        RelRoot node = planner.rel(planner.validate(planner.parse(sql)));
        PreparedStatement stmt = RelRunners.run(node.rel);
        stmt.execute();

        ResultSet rs = stmt.getResultSet();
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();

        while (rs.next()) {
            for (int i=1; i<=columns; i++) {
                System.out.print((i == 1 ? "" : ", ") + meta.getColumnName(i) + ":" + rs.getObject(i));
            }
            System.out.println();
        }
    }
}
