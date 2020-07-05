
package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.util.interpolators.KeyedInterpolator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Resamples by moving values to their nearest sampling interval and interpolating values where
 * values are missing. Used for keyed streams.
 * @param <I> Input type
 */
public class KeyedResampler<I> extends Resampler<I> implements Function, Serializable {

	KeyedInterpolator<I> interpolator;
	FieldAccessor<I, Object> fieldAccessor;

	public KeyedResampler(long samplingInterval, KeyedInterpolator<I> interpolator,
						  Class<?> typeClass, FieldAccessor<I, Object> fieldAccessor){
		super(samplingInterval, typeClass);
		this.interpolator = interpolator;
		this.fieldAccessor = fieldAccessor;
	}

	public KeyedResampler(long samplingInterval, KeyedInterpolator<I> interpolator,
						  Class<?> typeClass, FieldAccessor<I, Object> fieldAccessor, long samplingWindow){
		super(samplingInterval, typeClass, samplingWindow);
		this.interpolator = interpolator;
		this.fieldAccessor = fieldAccessor;
	}

	public DataStorage<I> resample(I value, long timestamp, TimestampedCollector<I> out, DataStorage<I> data) throws Exception{
		currentElement = data.currentElement;
		currentIntervalTimestamp = data.currentIntervalTimestamp;
		currentClosestTimestampOffset = data.currentClosestTimestampOffset;
		collectionBuffer = data.collectionBuffer;

		interpolator.interpolationBuffer = data.interpolationBuffer;
		interpolator.defaultElement = data.defaultElement;

		if (currentIntervalTimestamp == Long.MAX_VALUE){
			currentIntervalTimestamp = timestamp;
			currentClosestTimestampOffset = 0;
			currentElement = value;
			interpolator.defaultElement = value;
		}
		else {
			doResample(value, timestamp, out);
		}

		if (!collectionBuffer.isEmpty() && collectionBuffer.get(0).f1 == null) {
			collectionBuffer = interpolator.interpolateAndCollect(collectionBuffer, out, timestamp, typeClass, fieldAccessor);
		}

		interpolator.updateBuffer(value, timestamp);

		data.currentElement = currentElement;
		data.currentIntervalTimestamp = currentIntervalTimestamp;
		data.currentClosestTimestampOffset = currentClosestTimestampOffset;
		data.collectionBuffer = collectionBuffer;

		data.defaultElement = interpolator.defaultElement;
		data.interpolationBuffer = interpolator.interpolationBuffer;

		return data;
	}

}
