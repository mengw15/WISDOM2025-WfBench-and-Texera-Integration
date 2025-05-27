/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import edu.uci.ics.amber.engine.architecture.logreplay.{ReplayLogManager, ReplayLogRecord}
import edu.uci.ics.amber.engine.architecture.messaginglayer.WorkerTimerService
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.{
  METHOD_PAUSE_WORKER,
  METHOD_RESUME_WORKER
}
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  FIFOMessageElement,
  TimerBasedControlElement
}
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import java.util.concurrent.LinkedBlockingQueue

class DPThreadSpec extends AnyFlatSpec with MockFactory {

  private val workerId: ActorVirtualIdentity = ActorVirtualIdentity("DP mock")
  private val senderWorkerId: ActorVirtualIdentity = ActorVirtualIdentity("mock sender")
  private val dataChannelId = ChannelIdentity(senderWorkerId, workerId, isControl = false)
  private val controlChannelId = ChannelIdentity(senderWorkerId, workerId, isControl = true)
  private val executor = mock[OperatorExecutor]
  private val mockInputPortId = PortIdentity()

  private val schema: Schema = Schema().add("field1", AttributeType.INTEGER)
  private val tuples: Array[Tuple] = (0 until 5000)
    .map(i => TupleLike(i).enforceSchema(schema))
    .toArray
  private val logStorage = SequentialRecordStorage.getStorage[ReplayLogRecord](None)
  private val logManager: ReplayLogManager =
    ReplayLogManager.createLogManager(logStorage, "none", x => {})

  "DP Thread" should "handle pause/resume during processing" in {
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    val dp = new DataProcessor(workerId, x => {}, inputMessageQueue = inputQueue)
    dp.executor = executor
    dp.inputManager.addPort(mockInputPortId, schema, List.empty, List.empty)
    dp.inputGateway.getChannel(dataChannelId).setPortId(mockInputPortId)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val dpThread = new DPThread(workerId, dp, logManager, inputQueue)
    dpThread.start()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    val message = WorkflowFIFOMessage(dataChannelId, 0, DataFrame(tuples))
    inputQueue.put(FIFOMessageElement(message))
    inputQueue.put(
      TimerBasedControlElement(
        ControlInvocation(METHOD_PAUSE_WORKER, EmptyRequest(), AsyncRPCContext(SELF, SELF), 0)
      )
    )
    Thread.sleep(1000)
    assert(dp.pauseManager.isPaused)
    inputQueue.put(
      TimerBasedControlElement(
        ControlInvocation(METHOD_RESUME_WORKER, EmptyRequest(), AsyncRPCContext(SELF, SELF), 1)
      )
    )
    Thread.sleep(1000)
    while (dp.inputManager.hasUnfinishedInput) {
      Thread.sleep(100)
    }
  }

  "DP Thread" should "handle pause/resume using fifo messages" in {
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    val dp = new DataProcessor(workerId, x => {}, inputMessageQueue = inputQueue)
    dp.inputManager.addPort(mockInputPortId, schema, List.empty, List.empty)
    dp.inputGateway.getChannel(dataChannelId).setPortId(mockInputPortId)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val dpThread = new DPThread(workerId, dp, logManager, inputQueue)
    dp.executor = executor
    dpThread.start()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    val message = WorkflowFIFOMessage(dataChannelId, 0, DataFrame(tuples))
    val pauseControl = WorkflowFIFOMessage(
      controlChannelId,
      0,
      ControlInvocation(METHOD_PAUSE_WORKER, EmptyRequest(), AsyncRPCContext(SELF, SELF), 0)
    )
    val resumeControl =
      WorkflowFIFOMessage(
        controlChannelId,
        1,
        ControlInvocation(METHOD_RESUME_WORKER, EmptyRequest(), AsyncRPCContext(SELF, SELF), 1)
      )
    inputQueue.put(FIFOMessageElement(message))
    inputQueue.put(
      FIFOMessageElement(pauseControl)
    )
    Thread.sleep(1000)
    assert(dp.pauseManager.isPaused)
    inputQueue.put(FIFOMessageElement(resumeControl))
    Thread.sleep(1000)
    while (dp.inputManager.hasUnfinishedInput) {
      Thread.sleep(100)
    }
  }

  "DP Thread" should "handle multiple batches from multiple sources" in {
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    val dp = new DataProcessor(workerId, x => {}, inputMessageQueue = inputQueue)
    dp.executor = executor
    val anotherSenderWorkerId = ActorVirtualIdentity("another")
    dp.inputManager.addPort(mockInputPortId, schema, List.empty, List.empty)
    dp.inputGateway.getChannel(dataChannelId).setPortId(mockInputPortId)
    dp.inputGateway
      .getChannel(ChannelIdentity(anotherSenderWorkerId, workerId, isControl = false))
      .setPortId(mockInputPortId)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val dpThread = new DPThread(workerId, dp, logManager, inputQueue)
    dpThread.start()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    val dataChannelID2 = ChannelIdentity(anotherSenderWorkerId, workerId, isControl = false)
    val message1 = WorkflowFIFOMessage(dataChannelId, 0, DataFrame(tuples.slice(0, 100)))
    val message2 = WorkflowFIFOMessage(dataChannelId, 1, DataFrame(tuples.slice(100, 200)))
    val message3 = WorkflowFIFOMessage(dataChannelID2, 0, DataFrame(tuples.slice(300, 1000)))
    val message4 = WorkflowFIFOMessage(dataChannelId, 2, DataFrame(tuples.slice(200, 300)))
    val message5 = WorkflowFIFOMessage(dataChannelID2, 1, DataFrame(tuples.slice(1000, 5000)))
    inputQueue.put(FIFOMessageElement(message1))
    inputQueue.put(FIFOMessageElement(message2))
    inputQueue.put(FIFOMessageElement(message3))
    inputQueue.put(FIFOMessageElement(message4))
    inputQueue.put(FIFOMessageElement(message5))
    Thread.sleep(1000)
    while (dp.inputManager.hasUnfinishedInput) {
      Thread.sleep(100)
    }
  }

  "DP Thread" should "write determinant logs to local storage while processing" in {
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    val dp = new DataProcessor(workerId, _ => {}, inputMessageQueue = inputQueue)
    dp.executor = executor
    val anotherSenderWorkerId = ActorVirtualIdentity("another")
    dp.inputManager.addPort(mockInputPortId, schema, List.empty, List.empty)
    dp.inputGateway.getChannel(dataChannelId).setPortId(mockInputPortId)
    dp.inputGateway
      .getChannel(ChannelIdentity(anotherSenderWorkerId, workerId, isControl = false))
      .setPortId(mockInputPortId)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val logStorage = SequentialRecordStorage.getStorage[ReplayLogRecord](
      Some(new URI("ram:///recovery-logs/tmp"))
    )
    logStorage.deleteStorage()
    val logManager: ReplayLogManager =
      ReplayLogManager.createLogManager(logStorage, "tmpLog", _ => {})
    val dpThread = new DPThread(workerId, dp, logManager, inputQueue)
    dpThread.start()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    val dataChannelId2 = ChannelIdentity(anotherSenderWorkerId, workerId, isControl = false)
    val message1 = WorkflowFIFOMessage(dataChannelId, 0, DataFrame(tuples.slice(0, 100)))
    val message2 = WorkflowFIFOMessage(dataChannelId, 1, DataFrame(tuples.slice(100, 200)))
    val message3 = WorkflowFIFOMessage(dataChannelId2, 0, DataFrame(tuples.slice(300, 1000)))
    val message4 = WorkflowFIFOMessage(dataChannelId, 2, DataFrame(tuples.slice(200, 300)))
    val message5 = WorkflowFIFOMessage(dataChannelId2, 1, DataFrame(tuples.slice(1000, 5000)))
    inputQueue.put(FIFOMessageElement(message1))
    inputQueue.put(FIFOMessageElement(message2))
    inputQueue.put(FIFOMessageElement(message3))
    Thread.sleep(1000)
    inputQueue.put(FIFOMessageElement(message4))
    inputQueue.put(FIFOMessageElement(message5))
    Thread.sleep(1000)
    while (logManager.getStep < 4999) {
      Thread.sleep(100)
    }
    logManager.sendCommitted(null) // drain in-mem records to flush
    logManager.terminate()
    val logs = logStorage.getReader("tmpLog").mkRecordIterator().toArray
    logStorage.deleteStorage()
    assert(logs.length > 1)
  }

}
