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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.util.DataStorage;
import org.apache.flink.streaming.api.operators.util.KeyedResampler;
import org.apache.flink.streaming.api.operators.util.interpolators.KeyedInterpolator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;

/**
 * Resampling for keyed array creation with interpolation.
 * @param <IN> Type of input
 */
@Internal
public class KeyedResamplingOperator<IN, KEY> extends AbstractUdfStreamOperator<IN, KeyedResampler<IN>> implements OneInputStreamOperator<IN, IN> {

	private static final long serialVersionUID = 1L;

	private transient TimestampedCollector<IN> collector;

	public KeySelector<IN, KEY> keySelector;

	private transient ValueState<DataStorage<IN>> data;

	private static final String STATE_NAME = "_op_state";

	TypeSerializer<DataStorage<IN>> typeSerializer;

	public KeyedResamplingOperator(long samplingInterval, KeyedInterpolator<IN> interpolator,
							Class<?> typeClass, FieldAccessor<IN, Object> fieldAccessor, KeySelector<IN, KEY> keySelector,
							TypeSerializer<DataStorage<IN>> typeSerializer){

		super(new KeyedResampler<IN>(samplingInterval, interpolator,
			typeClass, fieldAccessor));

		this.typeSerializer = typeSerializer;
		this.keySelector = keySelector;
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		ValueStateDescriptor<DataStorage<IN>> stateId = new ValueStateDescriptor<>(STATE_NAME, typeSerializer);
		data = getPartitionedState(stateId);
		collector = new TimestampedCollector<>(output);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		IN value = element.getValue();
		DataStorage<IN> currentData = data.value();
		if (currentData == null){
			currentData = new DataStorage<IN>();
		}

		DataStorage<IN>  out = userFunction.resample(value, element.getTimestamp(), collector, currentData);
		data.update(out);

	}
}
