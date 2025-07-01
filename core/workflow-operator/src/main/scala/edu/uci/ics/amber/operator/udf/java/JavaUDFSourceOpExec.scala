package edu.uci.ics.amber.operator.udf.java

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
class JavaUDFSourceOpExec extends SourceOperatorExecutor with Serializable{
  private var produceFunc: () => TupleLike = _

  def setProduceFunc(func: () => TupleLike): Unit = produceFunc = func

  override def produceTuple(): Iterator[TupleLike] = Iterator(produceFunc())
}


