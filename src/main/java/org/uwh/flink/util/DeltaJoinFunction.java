package org.uwh.flink.util;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
import java.util.Collection;

@FunctionalInterface
public interface DeltaJoinFunction<X,Y,Z> extends Function, Serializable {
    Collection<Z> join(X currentX, Y currentY, X newX, Y newY) throws Exception;
}
