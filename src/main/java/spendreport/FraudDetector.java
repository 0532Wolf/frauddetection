/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * 关于数据流的业务逻辑
 * 欺诈检查类 FraudDetector 是 KeyedProcessFunction 接口的一个实现。
 * 他的方法 KeyedProcessFunction#processElement 将会在每个交易事件上被调用。
 * 这个程序里边会对每笔交易发出警报
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;
	//判断上笔交易是否为小额交易状态
	private transient ValueState<Boolean> flagState;
	//时间间隔
	private transient ValueState<Long> timerState;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	/**
	 * 逻辑处理方法
	 *  对于一个账户，如果出现小于 $1 美元的交易后紧跟着一个大于 $500 的交易，就输出一个报警信息。
	 * @param transaction
	 * @param context
	 * @param collector
	 * @throws Exception
	 */
	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {
		// Get the current state for the current key
		Boolean lastTransactionWasSmall = flagState.value();

		// Check if the flag is set
		if (lastTransactionWasSmall != null) {
			if (transaction.getAmount() > LARGE_AMOUNT) {
				// Output an alert downstream
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());

				collector.collect(alert);
			}

			// Clean up our state
			flagState.clear();
		}
		if (transaction.getAmount() < SMALL_AMOUNT) {
			// set the flag to true
			flagState.update(true);

			// set the timer and timer state
			long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
			context.timerService().registerProcessingTimeTimer(timer);
			timerState.update(timer);
		}
		if (transaction.getAmount() < SMALL_AMOUNT) {
			// Set the flag to true
			flagState.update(true);
		}
		/*Alert alert = new Alert();
		alert.setId(transaction.getAccountId());

		collector.collect(alert);*/
	}

	/**
	 *
	 * 当标记状态被设置为 true 时，设置一个在当前时间一分钟后触发的定时器。
	 * 当定时器被触发时，重置标记状态。
	 * 当标记状态被重置时，删除定时器。
	 * @param parameters
	 */
	@Override
	public void open(Configuration parameters) {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN);
		flagState = getRuntimeContext().getState(flagDescriptor);

		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
				"timer-state",
				Types.LONG);
		timerState = getRuntimeContext().getState(timerDescriptor);

	}

	/**
	 * 当定时器触发时，将会调用 KeyedProcessFunction#onTimer 方法。 通过重写这个方法来实现一个你自己的重置状态的回调逻辑。
	 * @param timestamp 时间
	 * @param ctx
	 * @param out
	 */
	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
		// remove flag after 1 minute
		timerState.clear();
		flagState.clear();
	}

	private void cleanUp(Context ctx) throws Exception {
		// delete timer
		Long timer = timerState.value();
		ctx.timerService().deleteProcessingTimeTimer(timer);

		// clean up all state
		timerState.clear();
		flagState.clear();
	}
}
