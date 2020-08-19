package org.apache.flink.streaming.api.operators.util.interpolators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Abstract class to be extended by interpolators. Used for keyed streams.
 * @param <I> Input type
 */
public abstract class KeyedInterpolator<I> implements Serializable {
	public ArrayList<Tuple2<Long, I>> interpolationBuffer;
	int interpolationBufferWindowSize;
	public I defaultElement;

	public KeyedInterpolator(int interpolationBufferWindowSize){
		this.interpolationBufferWindowSize = interpolationBufferWindowSize;
	}

	public ArrayList<Tuple2<Long, I>> interpolateAndCollect(ArrayList<Tuple2<Long, I>> collectionBuffer, TimestampedCollector<I> collector,
									long latestTimestamp, Class<?> typeClass, FieldAccessor<I, Object> fieldAccessor) throws Exception {

		if (interpolationBuffer.size() == interpolationBufferWindowSize) {
			{
				while (!collectionBuffer.isEmpty()) {
					long collectionTimestamp = collectionBuffer.get(0).f0;
					I collectionValue = collectionBuffer.get(0).f1;

					if (collectionValue == null) {
						if (Math.abs(latestTimestamp - collectionTimestamp) >
							Math.abs(interpolationBuffer.get(0).f0 - collectionTimestamp)) {

							I result = fieldAccessor.set(defaultElement, (I) interpolate(collectionTimestamp, typeClass, fieldAccessor));
							collector.setAbsoluteTimestamp(collectionTimestamp);
							collector.collect(result);
							collector.emitWatermark(new Watermark(collectionTimestamp - 1));
							collectionBuffer.remove(0);

						} else {
							break;
						}
					} else {
						collector.setAbsoluteTimestamp(collectionBuffer.get(0).f0);
						collector.collect(collectionBuffer.get(0).f1);
						collector.emitWatermark(new Watermark(collectionBuffer.get(0).f0 - 1));
						collectionBuffer.remove(0);
					}
				}
			}
		}
		return collectionBuffer;
	}

	public Object interpolate(long collectionTimestamp, Class<?> typeClass,
								FieldAccessor<I, Object> fieldAccessor) throws Exception{
		return null;
	}

	public void updateBuffer(I value, long timestamp){
		if (interpolationBuffer.size() == interpolationBufferWindowSize){
			interpolationBuffer.remove(0);
		}
		interpolationBuffer.add(new Tuple2<>(timestamp, value));
	}

}
