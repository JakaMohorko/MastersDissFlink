package org.apache.flink.streaming.api.functions.windowing.util;

import org.apache.flink.streaming.api.functions.windowing.ApplyToArrayFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * FFT on tumbling windows.
 */
public abstract class FFT<K, W extends Window> extends ApplyToArrayFunction<K, W, Double, Complex> {
	@Override
	public ArrayList<Complex> userFunction(ArrayList<Double> arr){
		if (((Math.log(arr.size()) / Math.log(2))) % 1 != 0){
			return new ArrayList<>();
		}
		else {
			double[] array = arr.stream().mapToDouble(Double::doubleValue).toArray();
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
