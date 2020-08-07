package org.apache.flink.streaming.runtime.operators.windowing.util;

import org.apache.flink.annotation.Public;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;

@Public
public interface SlidingAggregator <OUT> extends Serializable {
	OUT aggregate(OUT previousVal, OUT currentVal);
}
