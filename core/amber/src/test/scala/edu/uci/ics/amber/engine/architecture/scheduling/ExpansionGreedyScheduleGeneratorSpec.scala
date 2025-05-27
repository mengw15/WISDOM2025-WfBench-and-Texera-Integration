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

import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.amber.operator.TestOperators
import edu.uci.ics.amber.operator.split.SplitOpDesc
import edu.uci.ics.amber.operator.udf.python.{DualInputPortsPythonUDFOpDescV2, PythonUDFOpDescV2}
import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.texera.workflow.LogicalLink
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
class ExpansionGreedyScheduleGeneratorSpec extends AnyFlatSpec with MockFactory {

  "RegionPlanGenerator" should "correctly find regions in headerlessCsv->keyword workflow" in {
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, keywordOpDesc),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(0),
          keywordOpDesc.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      new WorkflowContext()
    )

    val (schedule, _) = new ExpansionGreedyScheduleGenerator(
      workflow.context,
      workflow.physicalPlan
    ).generate()

    // Assuming each level only has one region
    val regionList = schedule.toList.map(level => level.head)
    assert(regionList.size == 1)

    regionList.zip(Iterator(2)).foreach {
      case (region, opCount) =>
        assert(region.getOperators.size == opCount)
    }

    regionList.zip(Iterator(1)).foreach {
      case (region, linkCount) =>
        assert(region.getLinks.size == linkCount)
    }

    regionList.zip(Iterator(3)).foreach {
      case (region, portCount) =>
        assert(region.getPorts.size == portCount)
    }
  }

  "RegionPlanGenerator" should "correctly find regions in csv->(csv->)->join workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val headerlessCsvOpDesc2 = TestOperators.headerlessSmallCsvScanOpDesc()
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        headerlessCsvOpDesc2,
        joinOpDesc
      ),
      List(
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          headerlessCsvOpDesc2.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity(1)
        )
      ),
      new WorkflowContext()
    )

    val (schedule, _) = new ExpansionGreedyScheduleGenerator(
      workflow.context,
      workflow.physicalPlan
    ).generate()

    // Assuming each level only has one region
    val regionList = schedule.toList.map(level => level.head)
    assert(regionList.size == 2)

    regionList.zip(Iterator(3, 2)).foreach {
      case (region, opCount) =>
        assert(region.getOperators.size == opCount)
    }

    regionList.zip(Iterator(2, 1)).foreach {
      case (region, linkCount) =>
        assert(region.getLinks.size == linkCount)
    }

    regionList.zip(Iterator(4, 4)).foreach {
      case (region, portCount) =>
        assert(region.getPorts.size == portCount)
    }

    // The fist region should be the build region
    assert(
      regionList.head.getOperators
        .map(_.id)
        .exists(physicalOpId =>
          OperatorIdentity(physicalOpId.logicalOpId.id) == headerlessCsvOpDesc1.operatorIdentifier
        )
    )

    // The second region should be the probe region
    assert(
      regionList(1).getOperators
        .map(_.id)
        .exists(physicalOpId =>
          OperatorIdentity(physicalOpId.logicalOpId.id) == headerlessCsvOpDesc2.operatorIdentifier
        )
    )

  }

  "RegionPlanGenerator" should "correctly find regions in csv->->filter->join workflow" in {
    val headerlessCsvOpDesc1 = TestOperators.headerlessSmallCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("column-1", "Asia")
    val joinOpDesc = TestOperators.joinOpDesc("column-1", "column-1")
    val workflow = buildWorkflow(
      List(
        headerlessCsvOpDesc1,
        keywordOpDesc,
        joinOpDesc
      ),
      List(
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          headerlessCsvOpDesc1.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          joinOpDesc.operatorIdentifier,
          PortIdentity(1)
        )
      ),
      new WorkflowContext()
    )

    val (schedule, _) = new ExpansionGreedyScheduleGenerator(
      workflow.context,
      workflow.physicalPlan
    ).generate()

    // Assuming each level only has one region
    val regionList = schedule.toList.map(level => level.head)
    assert(regionList.size == 2)

    regionList.zip(Iterator(4, 1)).foreach {
      case (region, opCount) =>
        assert(region.getOperators.size == opCount)
    }

    regionList.zip(Iterator(3, 0)).foreach {
      case (region, linkCount) =>
        assert(region.getLinks.size == linkCount)
    }

    regionList.zip(Iterator(6, 3)).foreach {
      case (region, portCount) =>
        assert(region.getPorts.size == portCount)
    }
  }
//
  "RegionPlanGenerator" should "correctly find regions in buildcsv->probecsv->hashjoin->hashjoin workflow" in {
    val buildCsv = TestOperators.headerlessSmallCsvScanOpDesc()
    val probeCsv = TestOperators.smallCsvScanOpDesc()
    val hashJoin1 = TestOperators.joinOpDesc("column-1", "Region")
    val hashJoin2 = TestOperators.joinOpDesc("column-2", "Country")
    val workflow = buildWorkflow(
      List(
        buildCsv,
        probeCsv,
        hashJoin1,
        hashJoin2
      ),
      List(
        LogicalLink(
          buildCsv.operatorIdentifier,
          PortIdentity(),
          hashJoin1.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          probeCsv.operatorIdentifier,
          PortIdentity(),
          hashJoin1.operatorIdentifier,
          PortIdentity(1)
        ),
        LogicalLink(
          buildCsv.operatorIdentifier,
          PortIdentity(),
          hashJoin2.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          hashJoin1.operatorIdentifier,
          PortIdentity(),
          hashJoin2.operatorIdentifier,
          PortIdentity(1)
        )
      ),
      new WorkflowContext()
    )

    val (schedule, _) = new ExpansionGreedyScheduleGenerator(
      workflow.context,
      workflow.physicalPlan
    ).generate()

    // Assuming each level only has one region
    val regionList = schedule.toList.map(level => level.head)
    assert(regionList.size == 2)
    regionList.zip(Iterator(5, 3)).foreach {
      case (region, opCount) =>
        assert(region.getOperators.size == opCount)
    }

    regionList.zip(Iterator(4, 2)).foreach {
      case (region, linkCount) =>
        assert(region.getLinks.size == linkCount)
    }

    regionList.zip(Iterator(7, 7)).foreach {
      case (region, portCount) =>
        assert(region.getPorts.size == portCount)
    }
  }

  "RegionPlanGenerator" should "correctly find regions in csv->split->training-infer workflow" in {
    val csv = TestOperators.headerlessSmallCsvScanOpDesc()
    val split = new SplitOpDesc()
    val training = new PythonUDFOpDescV2()
    val inference = new DualInputPortsPythonUDFOpDescV2()
    val workflow = buildWorkflow(
      List(
        csv,
        split,
        training,
        inference
      ),
      List(
        LogicalLink(
          csv.operatorIdentifier,
          PortIdentity(),
          split.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          split.operatorIdentifier,
          PortIdentity(),
          training.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          training.operatorIdentifier,
          PortIdentity(),
          inference.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          split.operatorIdentifier,
          PortIdentity(1),
          inference.operatorIdentifier,
          PortIdentity(1)
        )
      ),
      new WorkflowContext()
    )

    val (schedule, _) = new ExpansionGreedyScheduleGenerator(
      workflow.context,
      workflow.physicalPlan
    ).generate()

    val regionList = schedule.toList.map(level => level.head)
    assert(regionList.size == 2)
    regionList.zip(Iterator(4, 1)).foreach {
      case (region, opCount) =>
        assert(region.getOperators.size == opCount)
    }

    regionList.zip(Iterator(3, 0)).foreach {
      case (region, linkCount) =>
        assert(region.getLinks.size == linkCount)
    }

    regionList.zip(Iterator(7, 3)).foreach {
      case (region, portCount) =>
        assert(region.getPorts.size == portCount)
    }
  }

}
