package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;
import java.util.ArrayList;

public abstract class Resampler <I> implements Function, Serializable {
	long currentIntervalTimestamp = Long.MAX_VALUE;
	long samplingInterval;
	long currentClosestTimestampOffset;
	long samplingWindow = -1L;
	I currentElement;

	ArrayList<Tuple2<Long, I>> collectionBuffer;
	Class<?> typeClass;

	public Resampler(long samplingInterval, Class<?> typeClass){
		this.samplingInterval = samplingInterval;
		this.typeClass = typeClass;
		collectionBuffer = new ArrayList<>();
	}

	public Resampler(long samplingInterval, Class<?> typeClass, long samplingWindow){
		this.samplingInterval = samplingInterval;
		this.typeClass = typeClass;
		collectionBuffer = new ArrayList<>();
		this.samplingWindow = samplingWindow;
	}

	public void doResample(I value, long timestamp, TimestampedCollector<I> out) {
		if (samplingWindow == -1){
			if (timestamp - currentIntervalTimestamp > Math.abs(timestamp - (currentIntervalTimestamp + samplingInterval))) {

				if (collectionBuffer.isEmpty()) {
					out.emitWatermark(new Watermark(currentIntervalTimestamp - 1));
					out.setAbsoluteTimestamp(currentIntervalTimestamp);
					out.collect(currentElement);
				} else {
					collectionBuffer.add(new Tuple2<>(currentIntervalTimestamp, currentElement));
				}

				currentIntervalTimestamp += samplingInterval;
				while (timestamp - currentIntervalTimestamp > Math.abs(timestamp - (currentIntervalTimestamp + samplingInterval))) {
					collectionBuffer.add(new Tuple2<>(currentIntervalTimestamp, null));
					currentIntervalTimestamp += samplingInterval;
				}

				currentElement = value;
				currentClosestTimestampOffset = Math.abs(timestamp - currentIntervalTimestamp);
			}
			else if (Math.abs(timestamp - currentIntervalTimestamp) < currentClosestTimestampOffset) {
				currentElement = value;
				currentClosestTimestampOffset = Math.abs(currentIntervalTimestamp - timestamp);
			}

		}
		else {
			if (timestamp - currentIntervalTimestamp > Math.abs(timestamp - (currentIntervalTimestamp + samplingInterval))) {

				if (currentElement == null){
					collectionBuffer.add(new Tuple2<>(currentIntervalTimestamp, null));
				}
				else if (collectionBuffer.isEmpty()) {
					out.emitWatermark(new Watermark(currentIntervalTimestamp - 1));
					out.setAbsoluteTimestamp(currentIntervalTimestamp);
					out.collect(currentElement);

				} else {
					collectionBuffer.add(new Tuple2<>(currentIntervalTimestamp, currentElement));
				}

				currentIntervalTimestamp += samplingInterval;
				while (true) {
					if (timestamp - currentIntervalTimestamp > Math.abs(timestamp - (currentIntervalTimestamp + samplingInterval))){
						collectionBuffer.add(new Tuple2<>(currentIntervalTimestamp, null));
						currentIntervalTimestamp += samplingInterval;
					}
					else if (Math.abs(timestamp - samplingInterval) > samplingWindow){
						currentElement = null;
						currentClosestTimestampOffset = Long.MIN_VALUE;
						break;
					}
					else {
						currentElement = value;
						currentClosestTimestampOffset = Math.abs(timestamp - currentIntervalTimestamp);
						break;
					}
				}
			}
			else if (currentElement == null && Math.abs(timestamp - samplingInterval) <= samplingWindow ||
				Math.abs(timestamp - currentIntervalTimestamp) < currentClosestTimestampOffset){
				currentElement = value;
				currentClosestTimestampOffset = Math.abs(currentIntervalTimestamp - timestamp);
			}

		}
	}

}
