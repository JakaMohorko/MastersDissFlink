package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.SlidingWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.ArrayWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.SlidingArrayWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableSlidingWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.util.SlidingAggregator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

import java.lang.reflect.Type;

import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getExecutionEnvironment;


/**
 * Windowed stream enabling operations on arrays.
 * @param <T> Input type
 * @param <K> Key type
 * @param <W> Window type
 */
@Public
public class ArrayWindowedStream<T, K, W extends Window> {

	/**
	 * The keyed data stream that is windowed by this stream.
	 */
	private final KeyedStream<T, K> input;

	/**
	 * The window assigner.
	 */
	private final WindowAssigner<? super T, W> windowAssigner;

	/**
	 * The trigger that is used for window evaluation/emission.
	 */
	private Trigger<? super T, ? super W> trigger;

	/** The evictor that is used for evicting elements before window evaluation. */
	private Evictor<? super T, ? super W> evictor;

	/** The user-specified allowed lateness. */
	private long allowedLateness;
	/**
	 * Side output {@code OutputTag} for late data. If no tag is set late data will simply be
	 * dropped.
	 */
	private OutputTag<T> lateDataOutputTag;
	private long slideRemainingElements;
	private long slideSize;

	private KeySelector<T, Integer> matrixKey;
	private long windowSize;
	private long keyNum;

	public ArrayWindowedStream(KeyedStream<T, K> input, WindowAssigner<? super T, W> windowAssigner, Trigger<? super T, ? super W> trigger,
					Long allowedLateness, OutputTag<T> lateDataOutputTag, Evictor<? super T, ? super W> evictor) {
		this.input = input;
		this.windowAssigner = windowAssigner;
		this.trigger = trigger;
		this.evictor = evictor;
	}

	public ArrayWindowedStream(KeyedStream<T, K> input, WindowAssigner<? super T, W> windowAssigner, Trigger<? super T, ? super W> trigger,
								Long allowedLateness, OutputTag<T> lateDataOutputTag, Evictor<? super T, ? super W> evictor, long windowSize, long slideSize) {
		this.input = input;
		this.windowAssigner = windowAssigner;
		this.trigger = trigger;
		this.evictor = evictor;
		this.windowSize = windowSize;
		this.slideSize = slideSize;
		slideRemainingElements = windowSize - slideSize;
	}

	public <R> SingleOutputStreamOperator<R> applyToArray(
		WindowFunction<T, R, K, W> function){

		TypeInformation<R> resultType;

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);

		TypeInformation<T> inType = input.getType();
		resultType = getWindowFunctionReturnType(function, inType);

		final String opName = generateOperatorName(windowAssigner, trigger, evictor, null, function);
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		@SuppressWarnings({"unchecked", "rawtypes"})
		TypeSerializer<StreamRecord<T>> streamRecordSerializer =
			(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

		ListStateDescriptor<StreamRecord<T>> stateDesc =
			new ListStateDescriptor<>("window-contents", streamRecordSerializer);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		operator =
			new ArrayWindowOperator<>(windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(function),
				trigger,
				evictor,
				allowedLateness,
				lateDataOutputTag,
				slideRemainingElements
			);

		return input.transform(opName, resultType, operator);
	}

	public <R> SingleOutputStreamOperator<R> applyToSlidingArray(
		SlidingWindowFunction<T, R, K, W> function, SlidingAggregator<R> slidingAggregator){

		TypeInformation<R> resultType;

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);

		TypeInformation<T> inType = input.getType();
		resultType = getSlidingWindowFunctionReturnType(function, inType);

		final String opName = generateOperatorName(windowAssigner, trigger, evictor, null, function);
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		@SuppressWarnings({"unchecked", "rawtypes"})
		TypeSerializer<StreamRecord<T>> streamRecordSerializer =
			(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

		ListStateDescriptor<StreamRecord<T>> stateDesc =
			new ListStateDescriptor<>("window-contents", streamRecordSerializer);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		operator =
			new SlidingArrayWindowOperator<>(windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				new InternalIterableSlidingWindowFunction<>(function),
				trigger,
				allowedLateness,
				lateDataOutputTag,
				windowSize,
				slideSize,
				slidingAggregator
			);

		return input.transform(opName, resultType, operator);
	}

	private static String generateFunctionName(Function function) {
		Class<? extends Function> functionClass = function.getClass();
		if (functionClass.isAnonymousClass()) {
			// getSimpleName returns an empty String for anonymous classes
			Type[] interfaces = functionClass.getInterfaces();
			if (interfaces.length == 0) {
				// extends an existing class (like RichMapFunction)
				Class<?> functionSuperClass = functionClass.getSuperclass();
				return functionSuperClass.getSimpleName() + functionClass.getName().substring(functionClass.getEnclosingClass().getName().length());
			} else {
				// implements a Function interface
				Class<?> functionInterface = functionClass.getInterfaces()[0];
				return functionInterface.getSimpleName() + functionClass.getName().substring(functionClass.getEnclosingClass().getName().length());
			}
		} else {
			return functionClass.getSimpleName();
		}
	}

	private static String generateOperatorName(
		WindowAssigner<?, ?> assigner,
		Trigger<?, ?> trigger,
		@Nullable Evictor<?, ?> evictor,
		@Nullable Function function1,
		@Nullable Function function2) {
		return "Window(" +
			assigner + ", " +
			trigger.getClass().getSimpleName() + ", " +
			(evictor == null ? "" : (evictor.getClass().getSimpleName())) +
			(function1 == null ? "" : (", " + generateFunctionName(function1))) +
			(function2 == null ? "" : (", " + generateFunctionName(function2))) +
			")";
	}

	private static <IN, OUT, KEY> TypeInformation<OUT> getWindowFunctionReturnType(
		WindowFunction<IN, OUT, KEY, ?> function,
		TypeInformation<IN> inType) {
		return TypeExtractor.getUnaryOperatorReturnType(
			function,
			WindowFunction.class,
			0,
			1,
			new int[]{3, 0},
			inType,
			null,
			false);
	}

	private static <IN, OUT, KEY> TypeInformation<OUT> getSlidingWindowFunctionReturnType(
		SlidingWindowFunction<IN, OUT, KEY, ?> function,
		TypeInformation<IN> inType) {
		return TypeExtractor.getUnaryOperatorReturnType(
			function,
			SlidingWindowFunction.class,
			0,
			1,
			new int[]{3, 0},
			inType,
			null,
			false);
	}

	private static <IN, OUT, KEY> TypeInformation<OUT> getProcessWindowFunctionReturnType(
		ProcessWindowFunction<IN, OUT, KEY, ?> function,
		TypeInformation<IN> inType,
		String functionName) {
		return TypeExtractor.getUnaryOperatorReturnType(
			function,
			ProcessWindowFunction.class,
			0,
			1,
			TypeExtractor.NO_INDEX,
			inType,
			functionName,
			false);
	}

}
