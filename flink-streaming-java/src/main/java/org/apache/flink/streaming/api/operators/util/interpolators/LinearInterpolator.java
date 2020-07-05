package org.apache.flink.streaming.api.operators.util.interpolators;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * Uses two nearest values for interpolation by using a linear function.
 * @param <I> Input type
 */
public class LinearInterpolator <I> extends Interpolator <I> {

	public LinearInterpolator(){
		super(2);
	}

	@Override
	public Object interpolate(long collectionTimestamp, Class<?> typeClass) throws Exception{

		if (interpolationBuffer.size() != 2){
			throw new Exception("Buffer size of linear interpolators should be 2.");
		}
		long x1 = interpolationBuffer.get(0).f0;
		long x2 = interpolationBuffer.get(1).f0;

		if (typeClass == Long.class){
			long y1 = (long) interpolationBuffer.get(0).f1;
			long y2 = (long) interpolationBuffer.get(1).f1;

			float gradient = (float) (y1 - y2) / (float) (x1 - x2);
			return (long) (gradient * (collectionTimestamp - x1) + y1);
		}
		else if (typeClass == Integer.class){
			int y1 = (int) interpolationBuffer.get(0).f1;
			int y2 = (int) interpolationBuffer.get(1).f1;

			float gradient = (float) (y1 - y2) / (float) (x1 - x2);
			return (int) (gradient * (collectionTimestamp - x1) + y1);
		}
		else if (typeClass == Float.class){
			float y1 = (float) interpolationBuffer.get(0).f1;
			float y2 = (float) interpolationBuffer.get(1).f1;

			float gradient = (y1 - y2) / (float) (x1 - x2);
			return (gradient * (collectionTimestamp - x1) + y1);
		}
		else if (typeClass == Double.class){
			double y1 = (double) interpolationBuffer.get(0).f1;
			double y2 = (double) interpolationBuffer.get(1).f1;

			float gradient = (float) (y1 - y2) / (float) (x1 - x2);
			return (double) (gradient * (collectionTimestamp - x1) + y1);
		}
		else if (typeClass == Short.class){
			short y1 = (short) interpolationBuffer.get(0).f1;
			short y2 = (short) interpolationBuffer.get(1).f1;

			float gradient = (float) (y1 - y2) / (float) (x1 - x2);
			return (short) (gradient * (collectionTimestamp - x1) + y1);
		}
		else {
			throw new Exception("Value type is non-numeric. Linear interpolator cannot be used.");
		}
	}

}
