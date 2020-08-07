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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ApplyToArrayFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.operators.util.DataStorage;
import org.apache.flink.streaming.api.operators.util.interpolators.DefaultInterpolator;
import org.apache.flink.streaming.api.operators.util.interpolators.LinearInterpolator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;
import com.mathworks.engine.*;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Implements a windowed version of the streaming "WordCount" program.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt; --window &lt;n&gt; --slide &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}. jav
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>use basic windowing abstractions.
 * </ul>
 */
public class FFTtest {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		final List<Tuple2<Double, Long>> input = new ArrayList<>();

		//long[] numElementsArr = {10000L, 100000L, 1000000L, 2500000L, 5000000L, 10000000L, 15000000L};
		long[] numElementsArr = {1000000L};
		//int[] windowSizeArr = {128, 256, 512, 1024, 2048, 4096};
		//long [] slideSizes = {256L, 230L, 179L, 128L, 76L, 25L};
		long [] slideSizes = {25L};
		int[] windowSizeArr = {256};
		for (long numElements : numElementsArr){
			for (int windowSize : windowSizeArr) {
				for (long slideSize : slideSizes){
					for (int x = 0; x < 3; x++) {
						input.clear();
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
									//System.out.println("Duration: " + (endTime - startTime) + " Elements: " + numElements + " Window: " + windowSize + "\n");
									try {
										File file = new File("/home/jaka/Documents/masters/dataout");
										FileWriter myWriter = new FileWriter(file, true);
										myWriter.write("Duration: " + (endTime - startTime) + " Elements: " + numElements + " Window: " + windowSize + "\n");
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

						LinearInterpolator<Double> interpolator = new LinearInterpolator<>();
						DataStream<Double> out =
						source
							.resample(1L, interpolator)
							.countWindowAll(windowSize)
							.toArrayStream()
							//.countWindowAllSlide(windowSize, slideSize)
							//.sum(0);
							//.toArrayStreamSliding()
							.applyToArray(new MatlabTest());
							//.applyToArray(new MyApplyToArray());
							//.countWindowAllSlide(windowSize, slideSize)
							//.countWindowAll(windowSize)
							//.sum(0);
						//out.print();
						env.execute("FFTtest");
					}
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

	public static class MyApplyToArray extends ApplyToArrayFunction<Byte, GlobalWindow, Double, Double>{
		@Override
		public ArrayList<Double> userFunction(ArrayList<Double> arr){
			//System.out.println(arr);
			return arr;
		}
	}

	public static class MatlabTest extends ApplyToArrayFunction<Byte, GlobalWindow, Double, Double> {

		@Override
		public ArrayList<Double> userFunction(ArrayList<Double> arr) {
			try{
				MatlabEngine eng = MatlabEngine.startMatlab();
				double[] array = arr.stream().mapToDouble(Double::doubleValue).toArray();
				Object[] out = eng.feval("fft", array);
				eng.close();
				ArrayList<Double> outarr = new ArrayList<>();
				/*for(double a : out){
					outarr.add(a);
				}*/

				return outarr;
			}
			catch(Exception e){
				e.printStackTrace();
			}
			return null;
		}
	}

	/**
	 * FFT test.
	 */
	public static class DoFFT extends ApplyToArrayFunction<Byte, GlobalWindow, Double, Double>{
		@Override
		public ArrayList<Double> userFunction(ArrayList<Double> arr){
			if (((Math.log(arr.size()) / Math.log(2))) % 1 != 0){
				return new ArrayList<>();
			}
			else {
				double[] array = arr.stream().mapToDouble(Double::doubleValue).toArray();
				FastFourierTransformer fastFourierTransformer = new FastFourierTransformer(DftNormalization.STANDARD);

				Complex[] temp = fastFourierTransformer.transform(array, TransformType.FORWARD);

				for (Complex t : temp){
					t.divide(0.5);
				}

				temp = fastFourierTransformer.transform(temp, TransformType.INVERSE);
				ArrayList<Double> out = new ArrayList<>();

				out.add(0.0);
				for (Complex t : temp){
					out.set(0, out.get(0) + t.getReal());
				}
				return out;
			}
		}
	}
}
