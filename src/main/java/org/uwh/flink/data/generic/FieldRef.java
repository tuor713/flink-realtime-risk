package org.uwh.flink.data.generic;

import java.io.Serializable;

/**
 * Concrete instance of a field in a given record type. Use for performance to speed up
 * lookup by keeping pre-computed position information
 */
public class FieldRef<T> implements Serializable {
    private final Field<T> field;
    private final int position;

    public FieldRef(Field<T> field, int position) {
        this.field = field;
        this.position = position;
    }

    public Field<T> getField() {
        return field;
    }

    public int getPosition() {
        return position;
    }
}
