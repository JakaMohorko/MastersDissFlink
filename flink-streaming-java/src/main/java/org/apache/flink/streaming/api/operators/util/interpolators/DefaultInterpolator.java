package org.apache.flink.streaming.api.operators.util.interpolators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;

/**
 * Uses n nearest values for interpolation by averaging them.
 * @param <I> Input type
 */
public class DefaultInterpolator <I> extends Interpolator <I> {

	I defaultValue;

	public DefaultInterpolator(I defaultValue){
		super(0);
		this.defaultValue = defaultValue;
	}

	@Override
	public Object interpolate(long collectionTimestamp, Class<?> typeClass) {
		return null;
	}

	@Override
	public void updateBuffer(I value, long timestamp){ }

	@Override
	public ArrayList<Tuple2<Long, I>> interpolateAndCollect(ArrayList<Tuple2<Long, I>> collectionBuffer, TimestampedCollector<I> collector,
											long latestTimestamp, Class<?> typeClass) throws Exception{
		collector.emitWatermark(new Watermark(collectionBuffer.get(0).f0 - 1));
		collector.setAbsoluteTimestamp(collectionBuffer.get(0).f0);
		collector.collect(defaultValue);

		return collectionBuffer;
	}

}
