package org.apache.flink.streaming.api.operators.util.interpolators;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Abstract class to be extended by interpolators.
 * @param <I> Input type
 */
public abstract class Interpolator<I> implements Serializable {
	public Object interpolate(ArrayList<Tuple2<Long, I>> interpolationBuffer, long intervalTimestamp, Class<?> typeClass) throws Exception {
		return null;
	}
}
