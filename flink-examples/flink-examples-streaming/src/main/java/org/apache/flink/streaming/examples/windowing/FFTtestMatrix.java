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

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ApplyToArrayFunction;
import org.apache.flink.streaming.api.functions.windowing.ApplyToMatrixFunction;
import org.apache.flink.streaming.api.operators.util.interpolators.KeyedLinearInterpolator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
public class FFTtestMatrix {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		final List<Tuple3<Double, Integer, Long>> input = new ArrayList<>();

		int[] windowSizeArr = {4};
		long[] numElementsArr = {100L};

		for (long numElements : numElementsArr) {
			for (int windowSize : windowSizeArr) {
				for (int y = 0; y < 1; y++) {
					int numKeys = 5;
					int key = 0;
					long watermark = 0L;
					input.clear();
					Random rnd = new Random();
					for (long x = 1L; x <= numElements; x += 1L) {
						if (x % numKeys == 0) {
							key = 0;
							watermark += 1;
						} else {
							key++;
						}
						input.add(new Tuple3<>(rnd.nextDouble(), key, watermark));
					}

					DataStream<Tuple2<Double, Integer>> source = env
						.addSource(new SourceFunction<Tuple2<Double, Integer>>() {
							private static final long serialVersionUID = 1L;

							@Override
							public void run(SourceContext<Tuple2<Double, Integer>> ctx) {
								final long startTime = System.currentTimeMillis();
								for (Tuple3<Double, Integer, Long> value : input) {
									ctx.collectWithTimestamp(new Tuple2<>(value.f0, value.f1), value.f2);
									ctx.emitWatermark(new Watermark(value.f2));
								}
								ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
								final long endTime = System.currentTimeMillis();
								System.out.println(endTime - startTime);
								try {
									File file = new File("/home/jaka/Documents/masters/dataout");
									FileWriter myWriter = new FileWriter(file, true);
									//myWriter.write("Duration: " + (endTime - startTime) + " Elements: " + numElements + " Window: " + windowSize + " keys: 100\n");
									//myWriter.write("Duration: " + (endTime - startTime) + " Elements: " + numElements + " Window: " + windowSize + " Slide: " + slideSize + "\n");
									myWriter.close();
								} catch (IOException e) {
									System.out.println("An error occurred.");
									e.printStackTrace();
								}
							}

							@Override
							public void cancel() {
							}
						});

					KeyedLinearInterpolator<Tuple2<Double, Integer>> interpolator = new KeyedLinearInterpolator<>();
					//DataStream<Tuple2<Double, Long>> out =
					source
						.keyBy(t -> t.f1)
						.resample(1L, interpolator, 0)
						//.countWindow(windowSize)
						//.toArrayStream()
						.countWindowAllMatrix(windowSize, numKeys, t -> t.f1)
						//.sum(0);
						.toArrayStreamMatrix(windowSize, numKeys, t -> t.f1)
						.applyToMatrix(new MyApplyToMatrix());

					//out.print();
					//System.out.println(env.getExecutionPlan());
					env.execute("FFTtest");
				}
			}
		}
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

	public static class MyApplyToArray extends ApplyToArrayFunction<Long, GlobalWindow, Tuple2<Double, Long>, Tuple2<Double, Long>>{
		@Override
		public ArrayList<Tuple2<Double, Long>> userFunction(ArrayList<Tuple2<Double, Long>> arr){
			return arr;
		}
	}

	public static class MyApplyToMatrix extends ApplyToMatrixFunction<Byte, GlobalWindow, Tuple2<Double, Integer>, Tuple2<Double, Integer>> {

		@Override
		public ArrayList<ArrayList<Tuple2<Double, Integer>>> userFunction(ArrayList<ArrayList<Tuple2<Double, Integer>>>  matrix){
			for (ArrayList<Tuple2<Double, Integer>> m : matrix){
				for(Tuple2<Double,Integer> n : m){
					System.out.print(n + " ");
				}
				System.out.println();
			}
			System.out.println();
			return matrix;
		}
	}

	/**
	 * FFT test.
	 */
	public static class DoFFT extends ApplyToArrayFunction<Long, GlobalWindow, Tuple2<Double, Long>, Double>{
		@Override
		public ArrayList<Double> userFunction(ArrayList<Tuple2<Double, Long>> arr){
			if (((Math.log(arr.size()) / Math.log(2))) % 1 != 0){
				return new ArrayList<>();
			}
			else {
				double[] array = arr.stream().map(t -> t.f0).mapToDouble(Double::doubleValue).toArray();
				FastFourierTransformer fastFourierTransformer = new FastFourierTransformer(DftNormalization.STANDARD);

				Complex[] temp = fastFourierTransformer.transform(array, TransformType.FORWARD);

				for (Complex t : temp) {
					t.divide(0.5);
				}

				temp = fastFourierTransformer.transform(array, TransformType.INVERSE);
				ArrayList<Double> out = new ArrayList<>();

				out.add(0.0);
				for (Complex t : temp) {
					out.set(0, out.get(0) + t.getReal());
				}
				return out;
			}
		}
	}
}
