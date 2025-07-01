package edu.uci.ics.amber.operator.bash


import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.{Attribute, Schema, AttributeType}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

class BashSourceOpDesc extends SourceOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Command")
  @JsonPropertyDescription("The bash command to execute (bash -c <cmd>)")
  var cmd: String = _

  override def getPhysicalOp(
                              workflowId: WorkflowIdentity,
                              executionId: ExecutionIdentity
                            ): PhysicalOp = {

    val outSchema = Schema(List(new Attribute("success", AttributeType.INTEGER)))

    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.bash.BashSourceOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withIsOneToManyOp(true)
      .withPropagateSchema(SchemaPropagationFunc { _ =>
        Map(operatorInfo.outputPorts.head.id -> outSchema)
      })
      .withParallelizable(false)
      .withLocationPreference(Option.empty)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Bash Source",
      "Execute a bash command and emit each stdout line as a tuple",
      OperatorGroupConstants.INPUT_GROUP,
      List.empty,
      List(OutputPort(blocking = true)),
    )

  override def sourceSchema(): Schema =
    Schema()
}
