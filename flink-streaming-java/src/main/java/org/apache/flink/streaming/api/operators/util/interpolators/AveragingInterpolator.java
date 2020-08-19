package org.apache.flink.streaming.api.operators.util.interpolators;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Uses n nearest values for interpolation by averaging them.
 * @param <I> Input type
 */
public class AveragingInterpolator <I> extends Interpolator <I> {

	public AveragingInterpolator(int interpolationBufferWindowSize){
		super(interpolationBufferWindowSize);
	}

	@Override
	public Object interpolate (long collectionTimestamp, Class<?> typeClass) throws Exception{
		if (typeClass == Long.class){
			long avg = 0;
			for (Tuple2<Long, I> tup : interpolationBuffer){
				avg += (Long) tup.f1;
			}
			return avg / interpolationBuffer.size();
		}
		else if (typeClass == Integer.class){
			int avg = 0;
			for (Tuple2<Long, I> tup : interpolationBuffer){
				avg += (Integer) tup.f1;
			}
			return avg / interpolationBuffer.size();
		}
		else if (typeClass == Float.class){
			float avg = 0;
			for (Tuple2<Long, I> tup : interpolationBuffer){
				avg += (Float) tup.f1;
			}
			return avg / interpolationBuffer.size();
		}
		else if (typeClass == Double.class){
			double avg = 0;
			for (Tuple2<Long, I> tup : interpolationBuffer){
				avg += (Double) tup.f1;
			}
			return avg / interpolationBuffer.size();
		}
		else if (typeClass == Short.class){
			short avg = 0;
			for (Tuple2<Long, I> tup : interpolationBuffer){
				avg += (Short) tup.f1;
			}
			return avg / interpolationBuffer.size();
		}
		else {
			throw new Exception("Value type is non-numeric. Averaging interpolator cannot be used.");
		}
	}
}
