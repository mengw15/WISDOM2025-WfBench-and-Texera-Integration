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

package edu.uci.ics.amber.engine.faulttolerance

import akka.actor.{ActorSystem, Props}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.core.workflow.{PortIdentity, WorkflowContext}
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ControllerProcessor}
import edu.uci.ics.amber.engine.architecture.worker.DataProcessor
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.DPInputQueueElement
import edu.uci.ics.amber.engine.common.SerializedState.{CP_STATE_KEY, DP_STATE_KEY}
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}
import edu.uci.ics.amber.engine.common.{AmberRuntime, CheckpointState}
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.amber.operator.TestOperators
import edu.uci.ics.texera.workflow.LogicalLink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.concurrent.LinkedBlockingQueue

class CheckpointSpec extends AnyFlatSpecLike with BeforeAndAfterAll {

  var system: ActorSystem = _

  val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
  val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
  val workflow = buildWorkflow(
    List(csvOpDesc, keywordOpDesc),
    List(
      LogicalLink(
        csvOpDesc.operatorIdentifier,
        PortIdentity(),
        keywordOpDesc.operatorIdentifier,
        PortIdentity()
      )
    ),
    new WorkflowContext()
  )

  override def beforeAll(): Unit = {
    system = ActorSystem("CheckpointSpec", AmberRuntime.akkaConfig)
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  "Default controller state" should "be serializable" in {
    val cp =
      new ControllerProcessor(
        workflow.context,
        ControllerConfig.default,
        CONTROLLER,
        msg => {}
      )
    val chkpt = new CheckpointState()
    chkpt.save(CP_STATE_KEY, cp)
  }

  "Default worker state" should "be serializable" in {
    val dp = new DataProcessor(
      SELF,
      msg => {},
      inputMessageQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    )
    val chkpt = new CheckpointState()
    chkpt.save(DP_STATE_KEY, dp)
  }

//  "CSVScanOperator" should "be serializable" in {
//    val chkpt = new CheckpointState()
//    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
//    val context = new WorkflowContext()
//    headerlessCsvOpDesc.setContext(context)
//    val phyOp = headerlessCsvOpDesc.getPhysicalOp(WorkflowIdentity(1), ExecutionIdentity(1))
//    phyOp.opExecInitInfo match {
//      case OpExecInitInfoWithCode(codeGen) => ???
//      case OpExecInitInfoWithFunc(opGen) =>
//        val operator = opGen(1, 1)
//        operator.open()
//        val outputIter =
//          operator.asInstanceOf[SourceOperatorExecutor].produceTuple().map(t => (t, None))
//        outputIter.next()
//        outputIter.next()
//        operator.asInstanceOf[CheckpointSupport].serializeState(outputIter, chkpt)
//        chkpt.save("deserialization", opGen)
//        val opGen2 = chkpt.load("deserialization").asInstanceOf[(Int, Int) => OperatorExecutor]
//        val op = opGen2.apply(1, 1)
//        op.asInstanceOf[CheckpointSupport].deserializeState(chkpt)
//    }
//  }
//
//  "Workflow " should "take global checkpoint, reload and continue" in {
//    val client1 = new AmberClient(
//      system,
//      workflow.context,
//      workflow.physicalPlan,
//      resultStorage,
//      ControllerConfig.default,
//      error => {}
//    )
//    Await.result(client1.controllerInterface.startWorkflow(EmptyRequest(), ()))
//    Thread.sleep(100)
//    Await.result(client1.controllerInterface.pauseWorkflow(EmptyRequest(), ()))
//    val checkpointId = ChannelMarkerIdentity(s"Checkpoint_test_1")
//    val uri = new URI("ram:///recovery-logs/tmp/")
//    Await.result(
//      client1.controllerInterface.takeGlobalCheckpoint(
//        TakeGlobalCheckpointRequest(estimationOnly = false, checkpointId, uri.toString),
//        ()
//      ),
//      Duration.fromSeconds(30)
//    )
//    client1.shutdown()
//    Thread.sleep(100)
//    var controllerConfig = ControllerConfig.default
//    controllerConfig =
//      controllerConfig.copy(stateRestoreConfOpt = Some(StateRestoreConfig(uri, checkpointId)))
//    val completableFuture = new CompletableFuture[Unit]()
//    val client2 = new AmberClient(
//      system,
//      workflow.context,
//      workflow.physicalPlan,
//      resultStorage,
//      controllerConfig,
//      error => {}
//    )
//    client2.registerCallback[ExecutionStateUpdate] { evt =>
//      if (evt.state == COMPLETED) {
//        completableFuture.complete(())
//      }
//    }
//    Thread.sleep(1000)
//    assert(
//      Await
//        .result(client2.controllerInterface.startWorkflow(EmptyRequest(), ()))
//        .workflowState == PAUSED
//    )
//    Thread.sleep(5000)
//    Await.result(client2.controllerInterface.resumeWorkflow(EmptyRequest(), ()))
//    completableFuture.get(30000, TimeUnit.MILLISECONDS)
//  }

}
