package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.ArrayWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
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

	public ArrayWindowedStream(KeyedStream<T, K> input, WindowAssigner<? super T, W> windowAssigner, Trigger<? super T, ? super W> trigger,
					Long allowedLateness, OutputTag<T> lateDataOutputTag, Evictor<? super T, ? super W> evictor) {
		this.input = input;
		this.windowAssigner = windowAssigner;
		this.trigger = trigger;
		this.evictor = evictor;

	}

	public SingleOutputStreamOperator<T> applyToArray(){
		return applyToArray(new PassThroughWindowFunction<K, W, T>());
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
				lateDataOutputTag
			);

		return input.transform(opName, resultType, operator);
	}

	public WindowedStream<T, K, W> toWindowedStream(){
		return new WindowedStream<>(input, windowAssigner).trigger(trigger);
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

	// ------------------------------------------------------------------------
	//  Operations on the keyed windows
	// ------------------------------------------------------------------------

	/**
	 * Applies a reduce function to the window. The window function is called for each evaluation
	 * of the window for each key individually. The output of the reduce function is interpreted
	 * as a regular non-windowed stream.
	 *
	 * <p>This window will try and incrementally aggregate data as much as the window policies
	 * permit. For example, tumbling time windows can aggregate the data, meaning that only one
	 * element per key is stored. Sliding time windows will aggregate on the granularity of the
	 * slide interval, so a few elements are stored per key (one per slide interval).
	 * Custom windows may not be able to incrementally aggregate, or may need to store extra values
	 * in an aggregation tree.
	 *
	 * @param function The reduce function.
	 * @return The data stream that is the result of applying the reduce function to the window.
	 */
	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> function) {
		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of reduce can not be a RichFunction. " +
				"Please use reduce(ReduceFunction, WindowFunction) instead.");
		}

		//clean the closure
		function = input.getExecutionEnvironment().clean(function);
		return reduce(function, new PassThroughWindowFunction<K, W, T>());
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	public <R> SingleOutputStreamOperator<R> reduce(
		ReduceFunction<T> reduceFunction,
		WindowFunction<T, R, K, W> function) {

		TypeInformation<T> inType = input.getType();
		TypeInformation<R> resultType = getWindowFunctionReturnType(function, inType);
		return reduce(reduceFunction, function, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param resultType Type information for the result type of the window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	public <R> SingleOutputStreamOperator<R> reduce(
		ReduceFunction<T> reduceFunction,
		WindowFunction<T, R, K, W> function,
		TypeInformation<R> resultType) {

		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of reduce can not be a RichFunction.");
		}

		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		final String opName = generateOperatorName(windowAssigner, trigger, evictor, reduceFunction, function);
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		@SuppressWarnings({"unchecked", "rawtypes"})
		TypeSerializer<StreamRecord<T>> streamRecordSerializer =
			(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

		ListStateDescriptor<StreamRecord<T>> stateDesc =
			new ListStateDescriptor<>("window-contents", streamRecordSerializer);

		operator =
			new ArrayWindowOperator<>(windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new ReduceApplyWindowFunction<>(reduceFunction, function)),
				trigger,
				evictor,
				allowedLateness,
				lateDataOutputTag
			);

		return input.transform(opName, resultType, operator);
	}


	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> reduce(ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> function) {
		TypeInformation<R> resultType = getProcessWindowFunctionReturnType(function, input.getType(), null);

		return reduce(reduceFunction, function, resultType);
	}


	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param function The window function.
	 * @param resultType Type information for the result type of the window function
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	@Internal
	public <R> SingleOutputStreamOperator<R> reduce(ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> function, TypeInformation<R> resultType) {
		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of apply can not be a RichFunction.");
		}
		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		final String opName = generateOperatorName(windowAssigner, trigger, evictor, reduceFunction, function);
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		@SuppressWarnings({"unchecked", "rawtypes"})
		TypeSerializer<StreamRecord<T>> streamRecordSerializer =
			(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

		ListStateDescriptor<StreamRecord<T>> stateDesc =
			new ListStateDescriptor<>("window-contents", streamRecordSerializer);

		operator =
			new ArrayWindowOperator<>(windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				new InternalIterableProcessWindowFunction<>(new ReduceApplyProcessWindowFunction<>(reduceFunction, function)),
				trigger,
				evictor,
				allowedLateness,
				lateDataOutputTag
			);

		return input.transform(opName, resultType, operator);
	}

}
