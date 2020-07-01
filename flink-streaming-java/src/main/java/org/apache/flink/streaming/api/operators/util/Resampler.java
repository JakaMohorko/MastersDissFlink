
package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;

/**
 * Resamples by moving values to their nearest sampling interval and inserting default values where
 * values are missing.
 * @param <I> Input type
 */
public class Resampler <I> implements Function, Serializable {

	long currentInterval = Long.MAX_VALUE;
	long samplingInterval;
	long currentClosestTimestampOffset;
	I currentElement;
	I defaultValue = null;

	public Resampler(long samplingInterval, I defaultValue){
		this.samplingInterval = samplingInterval;
		this.defaultValue = defaultValue;

	}

	public void resample(I value, long timestamp, TimestampedCollector<I> out){

		if (currentInterval == Long.MAX_VALUE){
			currentInterval = timestamp;
			currentClosestTimestampOffset = timestamp;
			currentElement = value;
		}
		else {
			if (timestamp - currentInterval > Math.abs(timestamp - (currentInterval + samplingInterval))){
				out.emitWatermark(new Watermark(currentInterval - 1));
				out.setAbsoluteTimestamp(currentInterval);
				out.collect(currentElement);
				currentInterval += samplingInterval;

				while (timestamp - currentInterval > Math.abs(timestamp - (currentInterval + samplingInterval))){
					out.emitWatermark(new Watermark(currentInterval - 1));
					out.setAbsoluteTimestamp(currentInterval);
					out.collect(defaultValue);
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
	}

}
