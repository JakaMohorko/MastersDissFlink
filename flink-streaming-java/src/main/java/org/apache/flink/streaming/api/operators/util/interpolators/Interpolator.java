package org.apache.flink.streaming.api.operators.util.interpolators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Abstract class to be extended by interpolators.
 * @param <I> Input type
 */
public abstract class Interpolator<I> implements Serializable {

	ArrayList<Tuple2<Long, I>> interpolationBuffer;
	int interpolationBufferWindowSize;

	public Interpolator(int interpolationBufferWindowSize){
		this.interpolationBufferWindowSize = interpolationBufferWindowSize;
		interpolationBuffer = new ArrayList<>();
	}

	public ArrayList<Tuple2<Long, I>> interpolateAndCollect(ArrayList<Tuple2<Long, I>> collectionBuffer, TimestampedCollector<I> collector,
									  long latestTimestamp, Class<?> typeClass) throws Exception{

		if (interpolationBuffer.size() == interpolationBufferWindowSize){
			while (!collectionBuffer.isEmpty()){
				long collectionTimestamp = collectionBuffer.get(0).f0;
				I collectionValue = collectionBuffer.get(0).f1;
				if (collectionValue == null){
					if (Math.abs(latestTimestamp - collectionTimestamp) >
						Math.abs(interpolationBuffer.get(0).f0 - collectionTimestamp)){

						collector.emitWatermark(new Watermark(collectionTimestamp - 1));
						collector.setAbsoluteTimestamp(collectionTimestamp);
						collector.collect((I) interpolate(collectionTimestamp, typeClass));
						collectionBuffer.remove(0);
					}
					else {
						break;
					}
				}
				else {
					collector.emitWatermark(new Watermark(collectionTimestamp - 1));
					collector.setAbsoluteTimestamp(collectionTimestamp);
					collector.collect(collectionValue);
					collectionBuffer.remove(0);
				}
			}
		}

		return collectionBuffer;
	}

	public Object interpolate(long collectionTimestamp, Class<?> typeClass) throws Exception{
		return null;
	}

	public void updateBuffer(I value, long timestamp){
		if (interpolationBuffer.size() == interpolationBufferWindowSize){
			interpolationBuffer.remove(0);
		}
		interpolationBuffer.add(new Tuple2<>(timestamp, value));
	}
}
