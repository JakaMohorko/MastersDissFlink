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
import org.apache.flink.streaming.api.operators.util.AllResampler;
import org.apache.flink.streaming.api.operators.util.interpolators.Interpolator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Resampling for array creation with interpolation.
 * @param <IN> Type of input
 */
@Internal
public class ResamplingOperator<IN> extends AbstractUdfStreamOperator<IN, AllResampler<IN>> implements OneInputStreamOperator<IN, IN> {

	private static final long serialVersionUID = 1L;

	private transient TimestampedCollector<IN> collector;

	public ResamplingOperator(long samplingInterval, Interpolator<IN> interpolator, Class<?> typeClass){
		super(new AllResampler<IN>(samplingInterval, interpolator, typeClass));
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		collector.setTimestamp(element);
		userFunction.resample(element.getValue(), element.getTimestamp(), collector);
	}

}
