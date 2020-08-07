
package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.util.interpolators.Interpolator;

/**
 * Resamples by moving values to their nearest sampling interval and interpolating values where
 * values are missing.
 * @param <I> Input type
 */
public class AllResampler<I> extends Resampler<I> {

	Interpolator<I> interpolator;

	public AllResampler(long samplingInterval, Interpolator<I> interpolator,
						Class<?> typeClass){
		super(samplingInterval, typeClass);
		this.interpolator = interpolator;
	}

	public AllResampler(long samplingInterval, Interpolator<I> interpolator, Class<?> typeClass, long samplingWindow) {
		super(samplingInterval, typeClass, samplingWindow);
		this.interpolator = interpolator;
	}

	public void resample(I value, long timestamp, TimestampedCollector<I> out) throws Exception {

		if (currentIntervalTimestamp == Long.MAX_VALUE) {
			currentIntervalTimestamp = timestamp;
			currentClosestTimestampOffset = 0;
			currentElement = value;
		}
		else {
			doResample(value, timestamp, out);
		}

		if (!collectionBuffer.isEmpty() && collectionBuffer.get(0).f1 == null) {
			collectionBuffer = interpolator.interpolateAndCollect(collectionBuffer, out, timestamp, typeClass);
		}
		interpolator.updateBuffer(value, timestamp);

	}
}
