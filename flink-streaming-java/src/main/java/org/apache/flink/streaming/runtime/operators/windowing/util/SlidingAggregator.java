package org.apache.flink.streaming.runtime.operators.windowing.util;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 *  Aggregator for overlapping values in sliding windows.
 */
@Public
public interface SlidingAggregator <OUT> extends Serializable {
	OUT aggregate(OUT previousVal, OUT currentVal);
}
