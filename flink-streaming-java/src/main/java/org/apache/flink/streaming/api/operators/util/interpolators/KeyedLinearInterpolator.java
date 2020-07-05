package org.apache.flink.streaming.api.operators.util.interpolators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;

import java.util.ArrayList;

/**
 * Uses two nearest values for interpolation by using a linear function. Used for interpolations on keyed streams.
 * @param <I> Input type
 */
public class KeyedLinearInterpolator <I> extends KeyedInterpolator <I> {
	
	public KeyedLinearInterpolator(){
		super(2);
	}

	@Override
	public Object interpolate(long collectionTimestamp, Class<?> typeClass, 
							  FieldAccessor<I, Object> fieldAccessor) throws Exception{

		if (interpolationBuffer.size() != 2){
			throw new Exception("Buffer size of linear interpolators should be 2.");
		}
		long x1 = interpolationBuffer.get(0).f0;
		long x2 = interpolationBuffer.get(1).f0;

		if (typeClass == Long.class){
			long y1 = (long) fieldAccessor.get(interpolationBuffer.get(0).f1);
			long y2 = (long) fieldAccessor.get(interpolationBuffer.get(1).f1);

			float gradient = (float) (y1 - y2) / (float) (x1 - x2);
			return (long) (gradient * (collectionTimestamp - x1) + y1);
		}
		else if (typeClass == Integer.class){
			int y1 = (int) fieldAccessor.get(interpolationBuffer.get(0).f1);
			int y2 = (int) fieldAccessor.get(interpolationBuffer.get(1).f1);

			float gradient = (float) (y1 - y2) / (float) (x1 - x2);
			return (int) (gradient * (collectionTimestamp - x1) + y1);
		}
		else if (typeClass == Float.class){
			float y1 = (float) fieldAccessor.get(interpolationBuffer.get(0).f1);
			float y2 = (float) fieldAccessor.get(interpolationBuffer.get(1).f1);

			float gradient = (y1 - y2) / (float) (x1 - x2);
			return (gradient * (collectionTimestamp - x1) + y1);
		}
		else if (typeClass == Double.class){
			double y1 = (double) fieldAccessor.get(interpolationBuffer.get(0).f1);
			double y2 = (double) fieldAccessor.get(interpolationBuffer.get(1).f1);

			float gradient = (float) (y1 - y2) / (float) (x1 - x2);
			return (double) (gradient * (collectionTimestamp - x1) + y1);
		}
		else if (typeClass == Short.class){
			short y1 = (short) fieldAccessor.get(interpolationBuffer.get(0).f1);
			short y2 = (short) fieldAccessor.get(interpolationBuffer.get(1).f1);

			float gradient = (float) (y1 - y2) / (float) (x1 - x2);
			return (short) (gradient * (collectionTimestamp - x1) + y1);
		}
		else {
			throw new Exception("Value type is non-numeric. Linear interpolator cannot be used.");
		}
	}
}
