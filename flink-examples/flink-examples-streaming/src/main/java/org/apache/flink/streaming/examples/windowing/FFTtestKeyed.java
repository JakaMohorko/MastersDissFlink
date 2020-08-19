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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.util.FFTKeyed;
import org.apache.flink.streaming.api.operators.util.interpolators.KeyedLinearInterpolator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;

import org.apache.commons.math3.complex.Complex;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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

public class FFTtestKeyed {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		final List<Tuple3<Double, Long, Long>> input = new ArrayList<>();

		long numElements = 1000000;
		long windowSize = 256L;

		long numKeys = 100;
		long key = 1L;
		long watermark = 0L;

		Random rnd = new Random();
		for (long x = 1L; x <= numElements; x += 1L) {
			if (x % numKeys == 0) {
				key = 1L;
				watermark += 1;
			} else {
				key++;
			}
			input.add(new Tuple3<>(rnd.nextDouble(), key, watermark));
		}

		DataStream<Tuple2<Double, Long>> source = env
			.addSource(new SourceFunction<Tuple2<Double, Long>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void run(SourceContext<Tuple2<Double, Long>> ctx) {
					final long startTime = System.currentTimeMillis();
					for (Tuple3<Double, Long, Long> value : input) {
						ctx.collectWithTimestamp(new Tuple2<>(value.f0, value.f1), value.f2);
						ctx.emitWatermark(new Watermark(value.f2));
					}
					ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
					final long endTime = System.currentTimeMillis();
					System.out.println(endTime - startTime);
				}

				@Override
				public void cancel() {
				}
			});

		KeyedLinearInterpolator<Tuple2<Double, Long>> interpolator = new KeyedLinearInterpolator<>();
		source
			.keyBy(t -> t.f1)
			.keyedResample(1L, interpolator, 0)
			.countWindow(windowSize)
			.toArrayStream()
			.applyToArray(new DoFFT());

		env.execute("FFTtest");
	}

	/**
	 * FFT test.
	 */
	public static class DoFFT extends FFTKeyed<Long, GlobalWindow> {
		@Override
		public Complex[] fftFunction(Complex[] data){
			for (int x = 0; x < data.length; x++){
				data[x] = data[x].divide(0.5);
			}
			return data;
		}
	}
}
