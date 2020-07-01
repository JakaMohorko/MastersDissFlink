
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
public class KeyedInterpolatingResampler <I> implements Function, Serializable {

	long currentInterval;
	long currentClosestTimestampOffset;
	I currentElement;
	I defaultElement;

	ArrayList<Tuple2<Long, I>> interpolationBuffer;
	ArrayList<Tuple2<Long, I>> collectionBuffer;

	int interpolationBufferWindow;
	KeyedInterpolator<I> interpolator;
	long samplingInterval;
	Class<?> typeClass;
	FieldAccessor<I, Object> fieldAccessor;

	public KeyedInterpolatingResampler(long samplingInterval, int interpolationBufferWindow, KeyedInterpolator<I> interpolator,
					Class<?> typeClass, FieldAccessor<I, Object> fieldAccessor){
		this.samplingInterval = samplingInterval;
		this.interpolationBufferWindow = interpolationBufferWindow;
		this.interpolator = interpolator;
		this.fieldAccessor = fieldAccessor;
		this.typeClass = typeClass;
	}

	@SuppressWarnings({ "unchecked" })
	public DataStorage<I> resample(I value, long timestamp, TimestampedCollector<I> out, DataStorage<I> data) throws Exception{
		currentElement = data.currentElement;
		defaultElement = data.defaultElement;
		currentInterval = data.currentInterval;
		currentClosestTimestampOffset = data.currentClosestTimestampOffset;
		interpolationBuffer = data.interpolationBuffer;
		collectionBuffer = data.collectionBuffer;

		if (currentInterval == Long.MAX_VALUE){
			currentInterval = timestamp;
			currentClosestTimestampOffset = timestamp;
			currentElement = value;
			defaultElement = value;
		}
		else {
			if (timestamp - currentInterval > Math.abs(timestamp - (currentInterval + samplingInterval))){

				if (collectionBuffer.isEmpty()){
					out.setAbsoluteTimestamp(currentInterval);
					out.collect(currentElement);
					out.emitWatermark(new Watermark(currentInterval - 1));
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
		//System.out.print("Current Element: " + currentElement + " ");
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
						I result = fieldAccessor.set(defaultElement, (I) interpolator.interpolate(interpolationBuffer, collectionBuffer.get(0).f0, typeClass, fieldAccessor));
						//System.out.println(collectionBuffer.get(0).f0);
						out.setAbsoluteTimestamp(collectionBuffer.get(0).f0);
						out.collect(result);
						out.emitWatermark(new Watermark(collectionBuffer.get(0).f0 - 1));
						collectionBuffer.remove(0);
					}
					else {
						break;
					}
				}
				else {
					//System.out.println(collectionBuffer.get(0).f0);
					out.setAbsoluteTimestamp(collectionBuffer.get(0).f0);
					out.collect(collectionBuffer.get(0).f1);
					out.emitWatermark(new Watermark(collectionBuffer.get(0).f0 - 1));
					collectionBuffer.remove(0);
				}
			}

			interpolationBuffer.remove(0);
			interpolationBuffer.add(new Tuple2<>(timestamp, value));
		}

		data.currentElement = currentElement;
		data.defaultElement = defaultElement;
		data.currentInterval = currentInterval;
		data.currentClosestTimestampOffset = currentClosestTimestampOffset;
		data.interpolationBuffer = interpolationBuffer;
		data.collectionBuffer = collectionBuffer;

		return data;
	}

}
