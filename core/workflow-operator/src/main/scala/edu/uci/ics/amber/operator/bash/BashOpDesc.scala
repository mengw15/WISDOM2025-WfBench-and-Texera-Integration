// src/main/scala/edu/uci/ics/amber/operator/bash/BashOpDesc.scala
// src/main/scala/edu/uci/ics/amber/operator/bash/BashOpDesc.scala
package edu.uci.ics.amber.operator.bash

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.operator.{LogicalOp, PortDescription, StateTransferFunc}

import scala.util.{Success, Try}

class BashOpDesc extends LogicalOp {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Command")
  @JsonPropertyDescription("The full bash command to execute")
  var cmd: String = _

  override def getPhysicalOp(
                              workflowId: WorkflowIdentity,
                              executionId: ExecutionIdentity
                            ): PhysicalOp = {

    val outSchema = Schema(List(new Attribute("success", AttributeType.INTEGER)))

    val partitionRequirement: List[Option[PartitionInfo]] =
      if (inputPorts != null) inputPorts.map(p => Option(p.partitionRequirement))
      else operatorInfo.inputPorts.map(_ => None)

    val base = PhysicalOp
      .manyToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.bash.BashOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withDerivePartition(_ => UnknownPartition())
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(partitionRequirement)
      .withIsOneToManyOp(true)
      .withParallelizable(false)
      .withPropagateSchema(SchemaPropagationFunc { _ =>
        Map(operatorInfo.outputPorts.head.id -> outSchema)
      })

    base
  }

  override def operatorInfo: OperatorInfo = {
    val inputPortInfo = if (inputPorts != null) {
      inputPorts.zipWithIndex.map {
        case (portDesc: PortDescription, idx) =>
          InputPort(
            PortIdentity(idx),
            displayName = portDesc.displayName,
            allowMultiLinks = true,
            dependencies = portDesc.dependencies.map(idx => PortIdentity(idx))
          )
      }
    } else {
      List(InputPort(PortIdentity(), allowMultiLinks = true))
    }
    val outputPortInfo = if (outputPorts != null) {
      outputPorts.zipWithIndex.map {
        case (portDesc, idx) => OutputPort(PortIdentity(idx), displayName = portDesc.displayName, blocking = true) //blocking to true
      }
    } else {
      List(OutputPort(blocking = true)) //blocking to true
    }

    OperatorInfo(
      "Bash",
      "Execute an arbitrary bash command",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts           = inputPortInfo,
      outputPorts          = outputPortInfo,
      dynamicInputPorts    = true,
      dynamicOutputPorts   = true,
      allowPortCustomization = true,
    )
  }

}
