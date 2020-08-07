package org.apache.flink.streaming.examples.windowing;

import com.google.common.collect.Lists;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ApplyToArrayFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.operators.util.interpolators.DefaultInterpolator;
import org.apache.flink.streaming.api.operators.util.interpolators.LinearInterpolator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * baseline time estimate.
 */

public class BaselineTest {

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		final List<Tuple2<Double, Long>> input = new ArrayList<>();

		long numElements = 100L;

		Random rnd = new Random();
		for (long x = 1L; x <= numElements; x += 1) {

			input.add(new Tuple2<>((double) x, x));
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
					System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");
				}

				@Override
				public void cancel() {
				}
			});

		LinearInterpolator<Double> interpolator = new LinearInterpolator<>();
		//DataStream<Double> counts =
			source
				.resample(1L, interpolator)
				//.countWindowAll(10, 8)
				/*.process(new ProcessAllWindowFunction<Double, Double, GlobalWindow>() {
					@Override
					public void process(Context context, Iterable<Double> elements, Collector<Double> out){
						System.out.println(elements);
					}
				});*/
				//.sum(0);
				.countWindowAllSlide(10, 2)
				.toArrayStreamSliding()
				.applyToArray(new Test());

		//counts.print();

		env.execute("FFTtest");
	}

	/**
	 * FFT test.
	 */
	public static class DoFFT extends ApplyToArrayFunction<Byte, GlobalWindow, Complex, Complex> {
		@Override
		public ArrayList<Complex> userFunction(ArrayList<Complex> arr){
			Complex[] array = arr.toArray(new Complex[arr.size()]);
			FastFourierTransformer fastFourierTransformer = new FastFourierTransformer(DftNormalization.STANDARD);

			Complex[] temp = fastFourierTransformer.transform(array, TransformType.FORWARD);

			return new ArrayList<>(Arrays.asList(temp));
		}
	}

	/**
	 * Baseline test.
	 */
	public static class Test extends ApplyToArrayFunction<Byte, GlobalWindow, Double, Double> {
		@Override
		public ArrayList<Double> userFunction(ArrayList<Double> arr){
			System.out.println(arr);
			return arr;
		}
	}
}
