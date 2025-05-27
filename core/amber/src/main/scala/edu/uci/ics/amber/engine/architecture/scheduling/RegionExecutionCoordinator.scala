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

package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.VFSURIFactory.decodeURI
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PhysicalLink, PhysicalOp}
import edu.uci.ics.amber.engine.architecture.common.{AkkaActorService, ExecutorDeployment}
import edu.uci.ics.amber.engine.architecture.controller.execution.{
  OperatorExecution,
  WorkflowExecution
}
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerConfig,
  ExecutionStatsUpdate,
  WorkerAssignmentUpdate
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AssignPortRequest,
  EmptyRequest,
  InitializeExecutorRequest,
  LinkWorkersRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  WorkflowAggregatedState
}
import edu.uci.ics.amber.engine.architecture.scheduling.config.{
  InputPortConfig,
  OperatorConfig,
  OutputPortConfig,
  ResourceConfig
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource

class RegionExecutionCoordinator(
    region: Region,
    workflowExecution: WorkflowExecution,
    asyncRPCClient: AsyncRPCClient,
    controllerConfig: ControllerConfig
) {
  def execute(actorService: AkkaActorService): Future[Unit] = {

    // fetch resource config
    val resourceConfig = region.resourceConfig.get

    // Create storage objects for output ports of the region
    createOutputPortStorageObjects(
      resourceConfig.portConfigs.collect { // keep only output-port configs
        case (id, cfg: OutputPortConfig) => id -> cfg
      }
    )

    val regionExecution = workflowExecution.getRegionExecution(region.id)

    region.getOperators.foreach(physicalOp => {
      // Check for existing execution for this operator
      val existOpExecution =
        workflowExecution.getAllRegionExecutions.exists(_.hasOperatorExecution(physicalOp.id))

      // Initialize operator execution, reusing existing execution if available
      val operatorExecution = regionExecution.initOperatorExecution(
        physicalOp.id,
        if (existOpExecution) Some(workflowExecution.getLatestOperatorExecution(physicalOp.id))
        else None
      )

      // If no existing execution, build the operator with specified config
      if (!existOpExecution) {
        buildOperator(
          actorService,
          physicalOp,
          resourceConfig.operatorConfigs(physicalOp.id),
          operatorExecution
        )
      }
    })

    // update UI
    asyncRPCClient.sendToClient(
      ExecutionStatsUpdate(
        workflowExecution.getAllRegionExecutionsStats
      )
    )
    asyncRPCClient.sendToClient(
      WorkerAssignmentUpdate(
        region.getOperators
          .map(_.id)
          .map(physicalOpId => {
            physicalOpId.logicalOpId.id -> regionExecution
              .getOperatorExecution(physicalOpId)
              .getWorkerIds
              .map(_.name)
              .toList
          })
          .toMap
      )
    )

    // initialize the operators that are uninitialized
    val operatorsToInit = region.getOperators.filter(op =>
      regionExecution.getAllOperatorExecutions
        .filter(a => a._2.getState == WorkflowAggregatedState.UNINITIALIZED)
        .map(_._1)
        .toSet
        .contains(op.id)
    )

    Future(())
      .flatMap(_ => initExecutors(operatorsToInit, resourceConfig))
      .flatMap(_ => assignPorts(region))
      .flatMap(_ => connectChannels(region.getLinks))
      .flatMap(_ => openOperators(operatorsToInit))
      .flatMap(_ => sendStarts(region))
      .unit
  }

  private def buildOperator(
      actorService: AkkaActorService,
      physicalOp: PhysicalOp,
      operatorConfig: OperatorConfig,
      operatorExecution: OperatorExecution
  ): Unit = {
    ExecutorDeployment.createWorkers(
      physicalOp,
      actorService,
      operatorExecution,
      operatorConfig,
      controllerConfig.stateRestoreConfOpt,
      controllerConfig.faultToleranceConfOpt
    )
  }

  private def initExecutors(
      operators: Set[PhysicalOp],
      resourceConfig: ResourceConfig
  ): Future[Seq[EmptyReturn]] = {
    Future
      .collect(
        operators
          .flatMap(physicalOp => {
            val workerConfigs = resourceConfig.operatorConfigs(physicalOp.id).workerConfigs
            workerConfigs.map(_.workerId).map { workerId =>
              asyncRPCClient.workerInterface.initializeExecutor(
                InitializeExecutorRequest(
                  workerConfigs.length,
                  physicalOp.opExecInitInfo,
                  physicalOp.isSourceOperator
                ),
                asyncRPCClient.mkContext(workerId)
              )
            }
          })
          .toSeq
      )
  }

  private def assignPorts(region: Region): Future[Seq[EmptyReturn]] = {
    val resourceConfig = region.resourceConfig.get
    Future.collect(
      region.getOperators
        .flatMap { physicalOp: PhysicalOp =>
          val inputPortMapping = physicalOp.inputPorts
            .filter {
              // Because of the hack on input dependency, some input ports may not belong to this region.
              case (inputPortId, _) =>
                val globalInputPortId = GlobalPortIdentity(physicalOp.id, inputPortId, input = true)
                region.getPorts.contains(globalInputPortId)
            }
            .flatMap {
              case (inputPortId, (_, _, Right(schema))) =>
                val globalInputPortId = GlobalPortIdentity(physicalOp.id, inputPortId, input = true)
                val (storageURIs, partitionings) =
                  resourceConfig.portConfigs.get(globalInputPortId) match {
                    case Some(cfg: InputPortConfig) =>
                      (
                        cfg.storagePairs.map(_._1.toString),
                        cfg.storagePairs.map(_._2)
                      )
                    case _ =>
                      (List.empty[String], List.empty[Partitioning])
                  }

                Some(globalInputPortId -> (storageURIs, partitionings, schema))
              case _ => None
            }
          // Currently an output port uses the same AssignPortRequest as an Input port.
          // However, an output port does not need a list of URIs or partitionings.
          // TODO: Separate AssignPortRequest for Input and Output Ports
          val outputPortMapping = physicalOp.outputPorts
            .filter {
              case (outputPortId, _) =>
                val globalInputPortId = GlobalPortIdentity(physicalOp.id, outputPortId)
                region.getPorts.contains(globalInputPortId)
            }
            .flatMap {
              case (outputPortId, (_, _, Right(schema))) =>
                val storageURI = resourceConfig.portConfigs
                  .collectFirst {
                    case (gid, cfg: OutputPortConfig)
                        if gid == GlobalPortIdentity(opId = physicalOp.id, portId = outputPortId) =>
                      cfg.storageURI.toString
                  }
                  .getOrElse("")
                Some(
                  GlobalPortIdentity(physicalOp.id, outputPortId) -> (List(
                    storageURI
                  ), List.empty, schema)
                )
              case _ => None
            }
          inputPortMapping ++ outputPortMapping
        }
        .flatMap {
          case (globalPortId, (storageUris, partitionings, schema)) =>
            resourceConfig.operatorConfigs(globalPortId.opId).workerConfigs.map(_.workerId).map {
              workerId =>
                asyncRPCClient.workerInterface.assignPort(
                  AssignPortRequest(
                    globalPortId.portId,
                    globalPortId.input,
                    schema.toRawSchema,
                    storageUris,
                    partitionings
                  ),
                  asyncRPCClient.mkContext(workerId)
                )
            }
        }
        .toSeq
    )
  }

  private def connectChannels(links: Set[PhysicalLink]): Future[Seq[EmptyReturn]] = {
    Future.collect(
      links.map { link: PhysicalLink =>
        asyncRPCClient.controllerInterface.linkWorkers(
          LinkWorkersRequest(link),
          asyncRPCClient.mkContext(CONTROLLER)
        )
      }.toSeq
    )
  }

  private def openOperators(operators: Set[PhysicalOp]): Future[Seq[EmptyReturn]] = {
    Future
      .collect(
        operators
          .map(_.id)
          .flatMap(opId =>
            workflowExecution.getRegionExecution(region.id).getOperatorExecution(opId).getWorkerIds
          )
          .map { workerId =>
            asyncRPCClient.workerInterface
              .openExecutor(EmptyRequest(), asyncRPCClient.mkContext(workerId))
          }
          .toSeq
      )
  }

  private def sendStarts(region: Region): Future[Seq[Unit]] = {
    asyncRPCClient.sendToClient(
      ExecutionStatsUpdate(
        workflowExecution.getAllRegionExecutionsStats
      )
    )
    Future.collect(
      region.getStarterOperators
        .map(_.id)
        .flatMap { opId =>
          workflowExecution
            .getRegionExecution(region.id)
            .getOperatorExecution(opId)
            .getWorkerIds
            .map { workerId =>
              asyncRPCClient.workerInterface
                .startWorker(EmptyRequest(), asyncRPCClient.mkContext(workerId))
                .map(resp =>
                  // update worker state
                  workflowExecution
                    .getRegionExecution(region.id)
                    .getOperatorExecution(opId)
                    .getWorkerExecution(workerId)
                    .setState(resp.state)
                )
            }
        }
        .toSeq
    )
  }

  private def createOutputPortStorageObjects(
      portConfigs: Map[GlobalPortIdentity, OutputPortConfig]
  ): Unit = {
    portConfigs.foreach {
      case (outputPortId, portConfig) =>
        val storageUriToAdd = portConfig.storageURI
        val (_, eid, _, _) = decodeURI(storageUriToAdd)
        val schemaOptional =
          region.getOperator(outputPortId.opId).outputPorts(outputPortId.portId)._3
        val schema =
          schemaOptional.getOrElse(throw new IllegalStateException("Schema is missing"))
        DocumentFactory.createDocument(storageUriToAdd, schema)
        WorkflowExecutionsResource.insertOperatorPortResultUri(
          eid = eid,
          globalPortId = outputPortId,
          uri = storageUriToAdd
        )
    }
  }

}
