
package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.util.interpolators.Interpolator;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Resamples by moving values to their nearest sampling interval and interpolating values where
 * values are missing.
 * @param <I> Input type
 */
public class InterpolatingResampler <I> implements Function, Serializable {

	long currentInterval = Long.MAX_VALUE;
	long samplingInterval;
	long currentClosestTimestampOffset;
	I currentElement;

	int interpolationBufferWindow;
	ArrayList<Tuple2<Long, I>> interpolationBuffer;
	Interpolator<I> interpolator;
	ArrayList<Tuple2<Long, I>> collectionBuffer;
	Class<?> typeClass;

	public InterpolatingResampler(long samplingInterval, int interpolationBufferWindow, Interpolator<I> interpolator,
									Class<?> typeClass){
		this.samplingInterval = samplingInterval;
		this.interpolationBufferWindow = interpolationBufferWindow;
		this.interpolator = interpolator;
		this.typeClass = typeClass;
		interpolationBuffer = new ArrayList<>();
		collectionBuffer = new ArrayList<>();
	}

	public void resample(I value, long timestamp, TimestampedCollector<I> out) throws Exception{

		if (currentInterval == Long.MAX_VALUE){
			currentInterval = timestamp;
			currentClosestTimestampOffset = timestamp;
			currentElement = value;
		}
		else {
			if (timestamp - currentInterval > Math.abs(timestamp - (currentInterval + samplingInterval))){

				if (collectionBuffer.isEmpty()){
					out.emitWatermark(new Watermark(currentInterval - 1));
					out.setAbsoluteTimestamp(currentInterval);
					out.collect(currentElement);
				}
				else {
					collectionBuffer.add(new Tuple2<>(currentInterval, currentElement));
				}

				currentInterval += samplingInterval;
				while (timestamp - currentInterval > Math.abs(timestamp - (currentInterval + samplingInterval))){
					collectionBuffer.add(new Tuple2<>(currentInterval, null));
					currentInterval += samplingInterval;
				}

				currentElement = value;
				currentClosestTimestampOffset = Math.abs(timestamp - currentInterval);
			}
			else if (Math.abs(timestamp - currentInterval) < currentClosestTimestampOffset){
				currentElement = value;
				currentClosestTimestampOffset = Math.abs(currentInterval - timestamp);
			}
		}

		//System.out.println("Collection buffer: " + collectionBuffer + " Current interval: " + currentInterval + " Current timestamp: " + timestamp + " Interpolation Buffer: " + interpolationBuffer);
		if (interpolationBuffer.size() != interpolationBufferWindow){
			interpolationBuffer.add(new Tuple2<>(timestamp, value));
		}
		else {
			while (!collectionBuffer.isEmpty()){
				if (collectionBuffer.get(0).f1 == null){
					if (Math.abs(timestamp - collectionBuffer.get(0).f0) >
						Math.abs(interpolationBuffer.get(0).f0 - collectionBuffer.get(0).f0)){

						//System.out.println((I) interpolator.interpolate(interpolationBuffer, collectionBuffer.get(0).f0, typeClass));
						out.emitWatermark(new Watermark(collectionBuffer.get(0).f0 - 1));
						out.setAbsoluteTimestamp(collectionBuffer.get(0).f0);
						out.collect((I) interpolator.interpolate(interpolationBuffer, collectionBuffer.get(0).f0, typeClass));
						collectionBuffer.remove(0);
					}
					else {
						break;
					}
				}
				else {
					out.emitWatermark(new Watermark(collectionBuffer.get(0).f0 - 1));
					out.setAbsoluteTimestamp(collectionBuffer.get(0).f0);
					out.collect(collectionBuffer.get(0).f1);
					collectionBuffer.remove(0);
				}
			}

			interpolationBuffer.remove(0);
			interpolationBuffer.add(new Tuple2<>(timestamp, value));
		}
	}

}
