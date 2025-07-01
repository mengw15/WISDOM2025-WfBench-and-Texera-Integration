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

package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import akka.pattern.gracefulStop
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ExecutionStateUpdate
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  QueryStatisticsRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.common.FutureBijection._
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF

import scala.concurrent.duration.DurationInt

/** indicate a worker has completed its execution
  * i.e. received and processed all data from upstreams
  * note that this doesn't mean all the output of this worker
  * has been received by the downstream workers.
  *
  * possible sender: worker
  */
trait WorkerExecutionCompletedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def workerExecutionCompleted(
      msg: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {

    // after worker execution is completed, query statistics immediately one last time
    // because the worker might be killed before the next query statistics interval
    // and the user sees the last update before completion
    val statsRequest =
      controllerInterface.controllerInitiateQueryStatistics(
        QueryStatisticsRequest(Seq(ctx.sender)),
        mkContext(SELF)
      )

    Future
      .collect(Seq(statsRequest))
      .flatMap(_ => {
        // if entire workflow is completed, clean up
        if (cp.workflowExecution.isCompleted) {
          // after query result come back: send completed event, cleanup ,and kill workflow
          sendToClient(ExecutionStateUpdate(cp.workflowExecution.getState))
          cp.controllerTimerService.disableStatusUpdate()
        }
        ////72-78(region release hacky solution)
        // TODO: this temporarily fixes the status update issue but is not clean
        Thread.sleep(10)
        val ref = cp.actorRefService.getActorRef(ctx.sender)
        val stop = gracefulStop(ref, 5.seconds).asTwitter()

        Future.collect(Seq(stop))
      })
    EmptyReturn()
  }
}
