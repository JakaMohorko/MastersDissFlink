package org.apache.flink.streaming.api.functions.windowing.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ApplyToArrayFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * FFT on keyed tumbling windows.
 */
public abstract class FFTKeyed<K, W extends Window> extends ApplyToArrayFunction<K, W, Tuple2<Double, K>, Complex> {
	@Override
	public ArrayList<Complex> userFunction(ArrayList<Tuple2<Double, K>> arr){
		if (((Math.log(arr.size()) / Math.log(2))) % 1 != 0){
			return new ArrayList<>();
		}
		else {
			double[] array = arr.stream().map(t -> t.f0).mapToDouble(Double::doubleValue).toArray();
			FastFourierTransformer fastFourierTransformer = new FastFourierTransformer(DftNormalization.STANDARD);

			Complex[] temp = fastFourierTransformer.transform(array, TransformType.FORWARD);

			temp = fftFunction(temp);

			temp = fastFourierTransformer.transform(temp, TransformType.INVERSE);

			return new ArrayList<>(Arrays.asList(temp));
		}
	}

	public Complex[] fftFunction (Complex[] data){
		return data;
	}

}
