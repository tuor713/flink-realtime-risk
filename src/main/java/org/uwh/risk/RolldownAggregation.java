package org.uwh.risk;

import org.uwh.RolldownItem;
import org.uwh.flink.data.generic.Expressions;
import org.uwh.flink.data.generic.Field;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.uwh.risk.Fields.F_RISK_ISSUER_JTD_ROLLDOWN;

public class RolldownAggregation implements Expressions.Aggregation<List<RolldownItem>,List<RolldownItem>> {
    @Override
    public Field<List<RolldownItem>> getInputField() {
        return F_RISK_ISSUER_JTD_ROLLDOWN;
    }

    @Override
    public Field<List<RolldownItem>> getOutputField() {
        return F_RISK_ISSUER_JTD_ROLLDOWN;
    }

    @Override
    public List<RolldownItem> init(List<RolldownItem> value, boolean retract) {
        if (retract) {
            return value.stream().map(it -> new RolldownItem(it.getDate(), -it.getJTD())).collect(Collectors.toList());
        } else {
            return value;
        }
    }

    @Override
    public List<RolldownItem> update(List<RolldownItem> value, List<RolldownItem> accumulator, boolean retract) {
        double factor = retract ? -1 : 1;
        List<RolldownItem> result = new ArrayList<>();

        int i = 0;
        int j = 0;

        double curJTD = 0.0;
        double curJTDLeft = 0.0;
        double curJTDRight = 0.0;

        while (i < value.size() || j < accumulator.size()) {
            if (i == value.size()) {
                for (int k = j; k<accumulator.size(); k++) {
                    double nextJTD = accumulator.get(k).getJTD();
                    curJTD += (nextJTD - curJTDRight);
                    result.add(new RolldownItem(accumulator.get(k).getDate(), curJTD));
                    curJTDRight = nextJTD;
                }
                break;
            }
            if (j == accumulator.size()) {
                for (int k = i; k<value.size(); k++) {
                    double nextJTD = value.get(k).getJTD();
                    curJTD += factor*(nextJTD - curJTDLeft);
                    result.add(new RolldownItem(value.get(k).getDate(), curJTD));
                    curJTDLeft = nextJTD;
                }
                break;
            }

            LocalDate iDate = value.get(i).getDate();
            double nextJTDLeft = value.get(i).getJTD();
            LocalDate jDate = accumulator.get(j).getDate();
            double nextJTDRight = accumulator.get(j).getJTD();
            if (iDate.equals(jDate)) {
                curJTD += (nextJTDRight-curJTDRight) + factor * (nextJTDLeft-curJTDLeft);
                result.add(new RolldownItem(iDate, curJTD));
                curJTDLeft = nextJTDLeft;
                curJTDRight = nextJTDRight;
                i++;
                j++;
            } else if (iDate.isBefore(jDate)) {
                curJTD += factor * (nextJTDLeft - curJTDLeft);
                result.add(new RolldownItem(iDate, curJTD));
                curJTDLeft = nextJTDLeft;
                i++;
            } else {
                curJTD += (nextJTDRight - curJTDRight);
                result.add(new RolldownItem(jDate, curJTD));
                curJTDRight = nextJTDRight;
                j++;
            }
        }

        return result;
    }
}
