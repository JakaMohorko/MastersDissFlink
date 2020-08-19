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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.util.FFTSliding;
import org.apache.flink.streaming.api.operators.util.interpolators.LinearInterpolator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.streaming.runtime.operators.windowing.util.SlidingAggregator;

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
public class FFTtestSliding {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		final List<Tuple2<Double, Long>> input = new ArrayList<>();

		long numElements = 1000000L;
		long windowSize = 256L;
		long slideSize = 64L;

		Random rnd = new Random();
		for (long y = 1L; y <= numElements; y += 1L) {
			input.add(new Tuple2<>(rnd.nextDouble(), y));
		}

		DataStream<Double> source = env
			.addSource(new SourceFunction<Double>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void run(SourceContext<Double> ctx) {
					final long startTime = System.currentTimeMillis();
					for (Tuple2<Double, Long> value : input) {
						ctx.collectWithTimestamp(value.f0, value.f1);
						ctx.emitWatermark(new Watermark(value.f1));
					}
					ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
					final long endTime = System.currentTimeMillis();
					System.out.println("Duration: " + (endTime - startTime) + " Elements: " + numElements + " Window: " + windowSize + "\n");
				}

				@Override
				public void cancel() {
				}
			});

		LinearInterpolator<Double> interpolator = new LinearInterpolator<>();
		DataStream<Complex> out =
		source
			.resample(1L, interpolator)
			.countWindowAllSlide(windowSize, slideSize)
			.toArrayStreamSliding()
			.applyToSlidingArray(new DoFFT(), new Adder());

		env.execute("FFTtest");
	}

	/**
	 * FFT test.
	 */
	public static class DoFFT extends FFTSliding<Byte, GlobalWindow> {
		@Override
		public Complex[] fftFunction(Complex[] data){
			for (int x = 0; x < data.length; x++){
				data[x] = data[x].divide(0.5);
			}
			return data;
		}
	}

	/**
	 * Sliding sum aggregator.
	 */
	public static class Adder implements SlidingAggregator<Complex>{
		@Override
		public Complex aggregate(Complex previous, Complex current){
			return current.add(previous);
		}
	}

}
