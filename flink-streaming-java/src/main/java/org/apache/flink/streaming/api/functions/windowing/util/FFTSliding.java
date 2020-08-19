package org.apache.flink.streaming.api.functions.windowing.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ApplyToSlidingArrayFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * FFT on sliding windows.
 */
public abstract class FFTSliding<K, W extends Window> extends ApplyToSlidingArrayFunction<K, W, Double, Complex> {

	@Override
	public Tuple2<ArrayList<Complex>, ArrayList<Complex>> userFunction(ArrayList<Complex> outputPrevious, ArrayList<Double> input){
		if (((Math.log(input.size()) / Math.log(2))) % 1 != 0){
			ArrayList<Complex> out = new ArrayList<>();
			for (double in : input){
				out.add(new Complex(in));
			}
			return new Tuple2<>(out, null);
		}
		else {
			double[] array = input.stream().mapToDouble(Double::doubleValue).toArray();
			FastFourierTransformer fastFourierTransformer = new FastFourierTransformer(DftNormalization.STANDARD);

			Complex[] temp = fastFourierTransformer.transform(array, TransformType.FORWARD);

			temp = fftFunction(temp);

			temp = fastFourierTransformer.transform(temp, TransformType.INVERSE);

			return new Tuple2<>(new ArrayList<>(Arrays.asList(temp)), null);
		}
	}

	public Complex[] fftFunction (Complex[] data){
		return data;
	}

}
