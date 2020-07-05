package org.apache.flink.streaming.api.operators.util.interpolators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;

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
				avg += (long) tup.f1;
			}
			return avg / interpolationBuffer.size();
		}
		else if (typeClass == Integer.class){
			int avg = 0;
			for (Tuple2<Long, I> tup : interpolationBuffer){
				avg += (int) tup.f1;
			}
			return avg / interpolationBuffer.size();
		}
		else if (typeClass == Float.class){
			float avg = 0;
			for (Tuple2<Long, I> tup : interpolationBuffer){
				avg += (float) tup.f1;
			}
			return avg / interpolationBuffer.size();
		}
		else if (typeClass == Double.class){
			double avg = 0;
			for (Tuple2<Long, I> tup : interpolationBuffer){
				avg += (double) tup.f1;
			}
			return avg / interpolationBuffer.size();
		}
		else if (typeClass == Short.class){
			short avg = 0;
			for (Tuple2<Long, I> tup : interpolationBuffer){
				avg += (short) tup.f1;
			}
			return avg / interpolationBuffer.size();
		}
		else {
			throw new Exception("Value type is non-numeric. Averaging interpolator cannot be used.");
		}
	}
}
