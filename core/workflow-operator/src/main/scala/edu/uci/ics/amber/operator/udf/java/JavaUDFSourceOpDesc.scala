package edu.uci.ics.amber.operator.udf.java

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithCode
import edu.uci.ics.amber.core.tuple.{Attribute, Schema}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{OutputPort, PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor

class JavaUDFSourceOpDesc extends SourceOperatorDescriptor{

  @JsonProperty(
    required = true,
    defaultValue =
      "import edu.uci.ics.amber.operator.udf.java.JavaUDFSourceOpExec;\n" +
        "import edu.uci.ics.amber.core.tuple.Tuple;\n" +
        "import edu.uci.ics.amber.core.tuple.TupleLike;\n" +
        "import scala.Function0;\n" +
        "import java.io.Serializable;\n" +
        "\n" +
        "public class JavaUDFOpExec extends JavaUDFSourceOpExec {\n" +
        "    public JavaUDFOpExec () {\n" +
        "        this.setProduceFunc((Function0<TupleLike> & Serializable) this::processOne);\n" +
        "    }\n" +
        "    \n" +
        "    public TupleLike produceOne() {\n" +
        "        return null;\n" +
        "    }\n" +
        "}"
  )
  @JsonSchemaTitle("Java Source UDF script")
  @JsonPropertyDescription("Input your code here")
  var code: String = ""

  @JsonProperty(required = true, defaultValue = "1")
  @JsonSchemaTitle("Worker count")
  @JsonPropertyDescription("Specify how many parallel workers to launch")
  var workers: Int = 1

  @JsonProperty()
  @JsonSchemaTitle("Columns")
  @JsonPropertyDescription("The columns of the source")
  var columns: List[Attribute] = List.empty

  override def getPhysicalOp(
       workflowId: WorkflowIdentity,
       executionId: ExecutionIdentity
    ): PhysicalOp = {
    require(workers >= 1, "Need at least 1 worker.")
    val physicalOp = PhysicalOp
      .sourcePhysicalOp(workflowId, executionId, operatorIdentifier, OpExecWithCode(code, "java"))
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withIsOneToManyOp(true)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
      .withLocationPreference(Option.empty)

    if (workers > 1) {
      physicalOp
        .withParallelizable(true)
        .withSuggestedWorkerNum(workers)
    } else {
      physicalOp.withParallelizable(false)
    }
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "1-out Java UDF",
      "User-defined function operator in Java script",
      OperatorGroupConstants.JAVA_GROUP,
      List.empty, // No input ports for a source operator
      List(OutputPort(blocking = true)), //blocking to true
      supportReconfiguration = true
    )
  }

  override def sourceSchema(): Schema = {
    if (columns != null && columns.nonEmpty) {
      Schema().add(columns)
    } else {
      Schema()
    }
  }
}

