/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * A {@link WindowFunction} that just emits each input element.
 */
@Internal
public class ApplyToMatrixFunction<K, W extends Window, T, O> implements WindowMatrixFunction<T, O, K, W> {

	private static final long serialVersionUID = 1L;

	@Override
	public void apply(K k, W window, ArrayList<ArrayList<T>> matrix, Collector<O> out) throws Exception {

		ArrayList<ArrayList<O>>  processedData = userFunction(matrix);

		for (int x = 0; x < processedData.get(0).size(); x++) {
			for (ArrayList<O> dat : processedData){
				out.collect(dat.get(x));
			}
		}
	}

	public ArrayList<ArrayList<O>> userFunction(ArrayList<ArrayList<T>>  matrix){
		return new ArrayList<ArrayList<O>>();
	}
}
