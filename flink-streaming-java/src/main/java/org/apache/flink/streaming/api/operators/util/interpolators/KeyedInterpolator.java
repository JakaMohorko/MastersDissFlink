package org.apache.flink.streaming.api.operators.util.interpolators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Abstract class to be extended by interpolators. Used for keyed streams.
 * @param <I> Input type
 */
public abstract class KeyedInterpolator<I> implements Serializable {
	public Object interpolate(ArrayList<Tuple2<Long, I>> interpolationBuffer, long intervalTimestamp,
						Class<?> typeClass, FieldAccessor<I, Object> fieldAccessor) throws  Exception{
		return null;
	}

}
