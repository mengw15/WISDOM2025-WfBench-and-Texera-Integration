package edu.uci.ics.amber.operator.bash

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.io.{BufferedReader, FileWriter, InputStreamReader}


class BashSourceOpExec(descString: String) extends SourceOperatorExecutor {
  private val desc: BashSourceOpDesc =
    objectMapper.readValue(descString, classOf[BashSourceOpDesc])

  override def produceTuple(): Iterator[TupleLike] = {
    
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

