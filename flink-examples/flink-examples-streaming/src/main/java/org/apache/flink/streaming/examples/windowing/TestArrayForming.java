/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ApplyToArrayFunction;
import org.apache.flink.streaming.api.operators.util.interpolators.LinearInterpolator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements a windowed version of the streaming "WordCount" program.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt; --window &lt;n&gt; --slide &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>use basic windowing abstractions.
 * </ul>
 */
public class TestArrayForming {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		final List<Tuple2<Long, Long>> input = new ArrayList<>();

		input.add(new Tuple2<>(10L, 0L));
		input.add(new Tuple2<>(12L, 5L));
		//input.add(new Tuple2<>(25L, 10L));
		//input.add(new Tuple2<>(16L, 16L));
		//input.add(new Tuple2<>(22L, 21L));
		//input.add(new Tuple2<>(21L, 24L));
		input.add(new Tuple2<>(11L, 30L));
		input.add(new Tuple2<>(14L, 33L));
		input.add(new Tuple2<>(16L, 40L));
		//input.add(new Tuple2<>(17L, 39L));
		//input.add(new Tuple2<>(15L, 44L));
		//input.add(new Tuple2<>(10L, 51L));
		//input.add(new Tuple2<>(11L, 56L));
		input.add(new Tuple2<>(13L, 59L));
		input.add(new Tuple2<>(20L, 64L));
		input.add(new Tuple2<>(27L, 70L));
		input.add(new Tuple2<>(25L, 74L));
		//input.add(new Tuple2<>(20L, 82L));
		input.add(new Tuple2<>(21L, 85L));
		input.add(new Tuple2<>(23L, 92L));
		input.add(new Tuple2<>(16L, 99L));
		input.add(new Tuple2<>(16L, 105L));

		DataStream<Long> source = env
			.addSource(new SourceFunction<Long>() {
				private static final long serialVersionUID = 1L;
				@Override
				public void run(SourceContext<Long> ctx) throws Exception {
					for (Tuple2<Long, Long> value : input) {
						ctx.collectWithTimestamp(value.f0, value.f1);
						ctx.emitWatermark(new Watermark(value.f1 - 1));
					}
					ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
				}

				@Override
				public void cancel() {
				}
			});

		env.getConfig().setGlobalJobParameters(params);
		LinearInterpolator<Long> interpolator = new LinearInterpolator<>();

		DataStream<Long> counts =
			source
				.resample(5L, interpolator)
				.countWindowAll(5)
				.toArrayStream()
				.applyToArray(new MyApplyToArray());

		//counts.print();

		env.execute("WindowWordCount");

	}

	/**
	 * Sums up all data with same key.
	 */
	public static class MyReduce implements ReduceFunction<Tuple2<String, Long>>{
		@Override
		public Tuple2<String, Long> reduce(Tuple2<String, Long> v1, Tuple2<String, Long> v2){
			return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
		}
	}

	/**
	 * Test class for array operations.
	 */
	public static class MyApplyToArray extends ApplyToArrayFunction<Byte, GlobalWindow, Long>{
		@Override
		public ArrayList<Long> userFunction(ArrayList<Long> arr){
			ArrayList<Long> out = new ArrayList<>();
			for (long t : arr){
				out.add(t + 5);
			}
			//System.out.println(arr);
			return out;
		}
	}
}
