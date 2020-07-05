package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * Storage to be used as a value state entry to be used for interpolation with different keys.
 * @param <IN>
 */
public class DataStorage<IN>{
	long currentIntervalTimestamp = Long.MAX_VALUE;
	long currentClosestTimestampOffset;
	IN currentElement;

	IN defaultElement;
	ArrayList<Tuple2<Long, IN>> interpolationBuffer = new ArrayList<>();
	ArrayList<Tuple2<Long, IN>> collectionBuffer = new ArrayList<>();
}
