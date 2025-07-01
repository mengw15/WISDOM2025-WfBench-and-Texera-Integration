// src/main/scala/edu/uci/ics/amber/operator/bash/BashOpExec.scala
package edu.uci.ics.amber.operator.bash

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.io.{BufferedReader, FileWriter, InputStreamReader}
import java.nio.file.Paths

/**
 * Executor: When all inputs have been processed (framework invokes onFinish),
 * execute `bash -c <cmd>`, printing stdout directly.
 * If exitCode == 0, return {"success": 0}.
 */
class BashOpExec(descString: String) extends OperatorExecutor {
  private val desc: BashOpDesc =
    objectMapper.readValue(descString, classOf[BashOpDesc])

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    Iterator.empty
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    val pb = new ProcessBuilder("bash", "-c", desc.cmd)
    pb.redirectErrorStream(true)
    val process = pb.start()

    val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
    var line: String = null
    while ( {
      line = reader.readLine(); line != null
    }) {
            println(line)
    }

    val exitCode = process.waitFor()

    if (exitCode == 0) {
      Iterator(TupleLike("success" -> 0))
    } else {
      Iterator(TupleLike("success" -> -1))
    }


  }
}
