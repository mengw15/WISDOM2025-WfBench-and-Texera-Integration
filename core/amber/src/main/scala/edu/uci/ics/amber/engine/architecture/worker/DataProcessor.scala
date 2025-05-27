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

import com.softwaremill.macwire.wire
import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.marker.{EndOfInputChannel, StartOfInputChannel, State}
import edu.uci.ics.amber.core.tuple.{
  FinalizeExecutor,
  FinalizePort,
  SchemaEnforceable,
  Tuple,
  TupleLike
}
import edu.uci.ics.amber.engine.architecture.common.AmberProcessor
import edu.uci.ics.amber.engine.architecture.logreplay.ReplayLogManager
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  InputManager,
  OutputManager,
  WorkerTimerService
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ChannelMarkerType.REQUIRE_ALIGNMENT
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  MainThreadDelegateMessage
}
import edu.uci.ics.amber.engine.architecture.worker.managers.SerializationManager
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{
  COMPLETED,
  READY,
  RUNNING
}
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerStatistics
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.error.ErrorUtils.{mkConsoleMessage, safely}
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity

import java.util.concurrent.LinkedBlockingQueue

class DataProcessor(
    actorId: ActorVirtualIdentity,
    outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit,
    inputMessageQueue: LinkedBlockingQueue[DPInputQueueElement]
) extends AmberProcessor(actorId, outputHandler)
    with Serializable {

  @transient var executor: OperatorExecutor = _

  def initTimerService(adaptiveBatchingMonitor: WorkerTimerService): Unit = {
    this.adaptiveBatchingMonitor = adaptiveBatchingMonitor
  }

  @transient var adaptiveBatchingMonitor: WorkerTimerService = _

  // inner dependencies
  private val initializer = new DataProcessorRPCHandlerInitializer(this)
  val pauseManager: PauseManager = wire[PauseManager]
  val stateManager: WorkerStateManager = new WorkerStateManager(actorId)
  val inputManager: InputManager = new InputManager(actorId, inputMessageQueue)
  val outputManager: OutputManager = new OutputManager(actorId, outputGateway)
  val channelMarkerManager: ChannelMarkerManager = new ChannelMarkerManager(actorId, inputGateway)
  val serializationManager: SerializationManager = new SerializationManager(actorId)

  def getQueuedCredit(channelId: ChannelIdentity): Long = {
    inputGateway.getChannel(channelId).getQueuedCredit
  }

  /**
    * provide API for actor to get stats of this operator
    */
  def collectStatistics(): WorkerStatistics =
    statisticsManager.getStatistics(executor)

  /**
    * process currentInputTuple through executor logic.
    * this function is only called by the DP thread.
    */
  private[this] def processInputTuple(tuple: Tuple): Unit = {
    try {
      val portIdentity: PortIdentity =
        this.inputGateway.getChannel(inputManager.currentChannelId).getPortId
      outputManager.outputIterator.setTupleOutput(
        executor.processTupleMultiPort(
          tuple,
          portIdentity.id
        )
      )

      statisticsManager.increaseInputStatistics(portIdentity, tuple.inMemSize)

    } catch safely {
      case e =>
        // forward input tuple to the user and pause DP thread
        handleExecutorException(e)
    }
  }

  private[this] def processInputState(state: State, port: Int): Unit = {
    try {
      val outputState = executor.processState(state, port)
      if (outputState.isDefined) {
        outputManager.emitMarker(outputState.get)
      }
    } catch safely {
      case e =>
        handleExecutorException(e)
    }
  }

  /**
    * process start of an input port with Executor.produceStateOnStart().
    * this function is only called by the DP thread.
    */
  private[this] def processStartOfInputChannel(portId: Int): Unit = {
    try {
      outputManager.emitMarker(StartOfInputChannel())
      val outputState = executor.produceStateOnStart(portId)
      if (outputState.isDefined) {
        outputManager.emitMarker(outputState.get)
      }
    } catch safely {
      case e =>
        handleExecutorException(e)
    }
  }

  /**
    * process end of an input port with Executor.produceStateOnFinish().
    * this function is only called by the DP thread.
    */
  private[this] def processEndOfInputChannel(portId: Int): Unit = {
    try {
      val outputState = executor.produceStateOnFinish(portId)
      if (outputState.isDefined) {
        outputManager.emitMarker(outputState.get)
      }
      outputManager.outputIterator.setTupleOutput(
        executor.onFinishMultiPort(portId)
      )
    } catch safely {
      case e =>
        // forward input tuple to the user and pause DP thread
        handleExecutorException(e)
    }
  }

  /** transfer one tuple from iterator to downstream.
    * this function is only called by the DP thread
    */
  private[this] def outputOneTuple(): Unit = {
    adaptiveBatchingMonitor.startAdaptiveBatching()
    var out: (TupleLike, Option[PortIdentity]) = null
    try {
      out = outputManager.outputIterator.next()
    } catch safely {
      case e =>
        // invalidate current output tuple
        out = null
        // also invalidate outputIterator
        outputManager.outputIterator.setTupleOutput(Iterator.empty)
        // forward input tuple to the user and pause DP thread
        handleExecutorException(e)
    }
    if (out == null) return

    val (outputTuple, outputPortOpt) = out

    if (outputTuple == null) return

    outputTuple match {
      case FinalizeExecutor() =>
        outputManager.emitMarker(EndOfInputChannel())
        // Send Completed signal to worker actor.
        executor.close()
        adaptiveBatchingMonitor.stopAdaptiveBatching()
        stateManager.transitTo(COMPLETED)
        logger.info(
          s"$executor completed, # of input ports = ${inputManager.getAllPorts.size}, " +
            s"input tuple count = ${statisticsManager.getInputTupleCount}, " +
            s"output tuple count = ${statisticsManager.getOutputTupleCount}"
        )
        asyncRPCClient.controllerInterface.workerExecutionCompleted(
          EmptyRequest(),
          asyncRPCClient.mkContext(CONTROLLER)
        )
      case FinalizePort(portId, input) =>
        outputManager.closeOutputStorageWriterIfNeeded(portId)
        asyncRPCClient.controllerInterface.portCompleted(
          PortCompletedRequest(portId, input),
          asyncRPCClient.mkContext(CONTROLLER)
        )
      case schemaEnforceable: SchemaEnforceable =>
        val portIdentity = outputPortOpt.getOrElse(outputManager.getSingleOutputPortIdentity)
        val tuple = schemaEnforceable.enforceSchema(outputManager.getPort(portIdentity).schema)
        statisticsManager.increaseOutputStatistics(portIdentity, tuple.inMemSize)
        outputManager.passTupleToDownstream(tuple, outputPortOpt)
        outputManager.saveTupleToStorageIfNeeded(tuple, outputPortOpt)

      case other => // skip for now
    }
  }

  def continueDataProcessing(): Unit = {
    val dataProcessingStartTime = System.nanoTime()
    if (outputManager.hasUnfinishedOutput) {
      outputOneTuple()
    } else {
      processInputTuple(inputManager.getNextTuple)
    }
    statisticsManager.increaseDataProcessingTime(System.nanoTime() - dataProcessingStartTime)
  }

  def processDataPayload(
      channelId: ChannelIdentity,
      dataPayload: DataPayload
  ): Unit = {
    val dataProcessingStartTime = System.nanoTime()
    val portId = this.inputGateway.getChannel(channelId).getPortId
    dataPayload match {
      case DataFrame(tuples) =>
        stateManager.conditionalTransitTo(
          READY,
          RUNNING,
          () => {
            asyncRPCClient.controllerInterface.workerStateUpdated(
              WorkerStateUpdatedRequest(stateManager.getCurrentState),
              asyncRPCClient.mkContext(CONTROLLER)
            )
          }
        )
        inputManager.initBatch(channelId, tuples)
        processInputTuple(inputManager.getNextTuple)
      case MarkerFrame(marker) =>
        marker match {
          case state: State =>
            processInputState(state, portId.id)
          case StartOfInputChannel() =>
            processStartOfInputChannel(portId.id)
          case EndOfInputChannel() =>
            this.inputManager.getPort(portId).channels(channelId) = true
            if (inputManager.isPortCompleted(portId)) {
              inputManager.initBatch(channelId, Array.empty)
              processEndOfInputChannel(portId.id)
              outputManager.outputIterator.appendSpecialTupleToEnd(
                FinalizePort(portId, input = true)
              )
            }
            if (inputManager.getAllPorts.forall(portId => inputManager.isPortCompleted(portId))) {
              // assuming all the output ports finalize after all input ports are finalized.
              outputManager.finalizeOutput()
            }
        }
    }
    statisticsManager.increaseDataProcessingTime(System.nanoTime() - dataProcessingStartTime)
  }

  def processChannelMarker(
      channelId: ChannelIdentity,
      marker: ChannelMarkerPayload,
      logManager: ReplayLogManager
  ): Unit = {
    val markerId = marker.id
    val command = marker.commandMapping.get(actorId.name)
    logger.info(s"receive marker from $channelId, id = ${marker.id}, cmd = ${command}")
    if (marker.markerType == REQUIRE_ALIGNMENT) {
      pauseManager.pauseInputChannel(EpochMarkerPause(markerId), List(channelId))
    }
    if (channelMarkerManager.isMarkerAligned(channelId, marker)) {
      logManager.markAsReplayDestination(markerId)
      // invoke the control command carried with the epoch marker
      logger.info(s"process marker from $channelId, id = ${marker.id}, cmd = ${command}")
      if (command.isDefined) {
        asyncRPCServer.receive(command.get, channelId.fromWorkerId)
      }
      // if this worker is not the final destination of the marker, pass it downstream
      val downstreamChannelsInScope = marker.scope.filter(_.fromWorkerId == actorId).toSet
      if (downstreamChannelsInScope.nonEmpty) {
        outputManager.flush(Some(downstreamChannelsInScope))
        outputGateway.getActiveChannels.foreach { activeChannelId =>
          if (downstreamChannelsInScope.contains(activeChannelId)) {
            logger.info(
              s"send marker to $activeChannelId, id = ${marker.id}, cmd = ${command}"
            )
            outputGateway.sendTo(activeChannelId, marker)
          }
        }
      }
      // unblock input channels
      if (marker.markerType == REQUIRE_ALIGNMENT) {
        pauseManager.resume(EpochMarkerPause(markerId))
      }
    }
  }

  private[this] def handleExecutorException(e: Throwable): Unit = {
    asyncRPCClient.controllerInterface.consoleMessageTriggered(
      ConsoleMessageTriggeredRequest(mkConsoleMessage(actorId, e)),
      asyncRPCClient.mkContext(CONTROLLER)
    )
    logger.warn(e.getLocalizedMessage + "\n" + e.getStackTrace.mkString("\n"))
    // invoke a pause in-place
    pauseManager.pause(OperatorLogicPause)
  }
}
