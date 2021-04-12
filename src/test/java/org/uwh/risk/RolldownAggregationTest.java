package org.uwh.risk;

import org.junit.jupiter.api.Test;
import org.uwh.RolldownItem;

import java.time.LocalDate;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RolldownAggregationTest {
    private static LocalDate today = LocalDate.now();
    private RolldownAggregation sut = new RolldownAggregation();

    @Test
    public void testLongerInput() {
        assertEquals(
                Arrays.asList(new RolldownItem(today, 100.0), new RolldownItem(today.plusDays(5), 0.0)),
                sut.update(
                        Arrays.asList(new RolldownItem(today, 100.0), new RolldownItem(today.plusDays(5), 0.0)),
                        Arrays.asList(),
                        false)
        );

    }

    @Test
    public void testAlignedDates() {
        assertEquals(
                Arrays.asList(new RolldownItem(today, 300.0), new RolldownItem(today.plusDays(5), 0.0)),
                sut.update(
                        Arrays.asList(new RolldownItem(today, 100.0), new RolldownItem(today.plusDays(5), 0.0)),
                        Arrays.asList(new RolldownItem(today, 200.0), new RolldownItem(today.plusDays(5), 0.0)),
                        false)
        );
    }

    @Test
    public void testAlignedDatesRetract() {
        assertEquals(
                Arrays.asList(new RolldownItem(today, 100.0), new RolldownItem(today.plusDays(5), 0.0)),
                sut.update(
                        Arrays.asList(new RolldownItem(today, 100.0), new RolldownItem(today.plusDays(5), 0.0)),
                        Arrays.asList(new RolldownItem(today, 200.0), new RolldownItem(today.plusDays(5), 0.0)),
                        true)
        );
    }

    @Test
    public void testNonAlignedDates() {
        assertEquals(
                Arrays.asList(new RolldownItem(today, 300.0), new RolldownItem(today.plusDays(3), 100.0), new RolldownItem(today.plusDays(5), 0.0)),
                sut.update(
                        Arrays.asList(new RolldownItem(today, 100.0), new RolldownItem(today.plusDays(5), 0.0)),
                        Arrays.asList(new RolldownItem(today, 200.0), new RolldownItem(today.plusDays(3), 0.0)),
                        false)
        );
    }

    @Test
    public void testNonAlignedDatesRetract() {
        assertEquals(
                Arrays.asList(new RolldownItem(today, 100.0), new RolldownItem(today.plusDays(3), -100.0), new RolldownItem(today.plusDays(5), 0.0)),
                sut.update(
                        Arrays.asList(new RolldownItem(today, 100.0), new RolldownItem(today.plusDays(5), 0.0)),
                        Arrays.asList(new RolldownItem(today, 200.0), new RolldownItem(today.plusDays(3), 0.0)),
                        true)
        );
    }

    @Test
    public void testPartialOverlap() {
        assertEquals(
                Arrays.asList(new RolldownItem(today, 300.0), new RolldownItem(today.plusDays(3), 50.0), new RolldownItem(today.plusDays(5), 0.0)),
                sut.update(
                        Arrays.asList(new RolldownItem(today, 100.0), new RolldownItem(today.plusDays(3), 0.0)),
                        Arrays.asList(new RolldownItem(today, 200.0), new RolldownItem(today.plusDays(3), 50.0), new RolldownItem(today.plusDays(5), 0.0)),
                        false)
        );
    }

    @Test
    public void testPartialOverlapRetract() {
        assertEquals(
                Arrays.asList(new RolldownItem(today, 100.0), new RolldownItem(today.plusDays(3), 50.0), new RolldownItem(today.plusDays(5), 0.0)),
                sut.update(
                        Arrays.asList(new RolldownItem(today, 100.0), new RolldownItem(today.plusDays(3), 0.0)),
                        Arrays.asList(new RolldownItem(today, 200.0), new RolldownItem(today.plusDays(3), 50.0), new RolldownItem(today.plusDays(5), 0.0)),
                        true)
        );
    }
}
