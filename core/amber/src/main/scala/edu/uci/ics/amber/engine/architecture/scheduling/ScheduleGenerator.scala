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

import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.engine.architecture.scheduling.ScheduleGenerator.replaceVertex
import edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies.{
  DefaultResourceAllocator,
  ExecutionClusterInfo
}
import org.jgrapht.graph.DirectedAcyclicGraph
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.jdk.CollectionConverters.{CollectionHasAsScala, IteratorHasAsScala}

object ScheduleGenerator {
  def replaceVertex(
      graph: DirectedAcyclicGraph[Region, RegionLink],
      oldVertex: Region,
      newVertex: Region
  ): Unit = {
    if (oldVertex.equals(newVertex)) {
      return
    }
    graph.addVertex(newVertex)
    graph
      .outgoingEdgesOf(oldVertex)
      .asScala
      .toList
      .foreach(oldEdge => {
        val dest = graph.getEdgeTarget(oldEdge)
        graph.removeEdge(oldEdge)
        graph.addEdge(newVertex, dest, RegionLink(newVertex.id, dest.id))
      })
    graph
      .incomingEdgesOf(oldVertex)
      .asScala
      .toList
      .foreach(oldEdge => {
        val source = graph.getEdgeSource(oldEdge)
        graph.removeEdge(oldEdge)
        graph.addEdge(source, newVertex, RegionLink(source.id, newVertex.id))
      })
    graph.removeVertex(oldVertex)
  }
}

abstract class ScheduleGenerator(
    workflowContext: WorkflowContext,
    var physicalPlan: PhysicalPlan
) {
  private val executionClusterInfo = new ExecutionClusterInfo()

  def generate(): (Schedule, PhysicalPlan)

  /**
    * A schedule is a ranking on the regions of a region plan. Currently we use a total order of the regions.
    */
  def generateScheduleFromRegionPlan(regionPlan: RegionPlan): Schedule = {
    val levelSets = regionPlan
      .topologicalIterator()
      .zipWithIndex
      .map(zippedRegionId => {
        zippedRegionId._2 -> Set.apply(regionPlan.getRegion(zippedRegionId._1))
      })
      .toMap
    Schedule.apply(levelSets)
  }

  def allocateResource(
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Unit = {

    val resourceAllocator =
      new DefaultResourceAllocator(
        physicalPlan,
        executionClusterInfo,
        workflowContext.workflowSettings
      )
    // generate the resource configs
    new TopologicalOrderIterator(regionDAG).asScala
      .foreach(region => {
        val (newRegion, _) = resourceAllocator.allocate(region)
        replaceVertex(regionDAG, region, newRegion)
      })
  }

  def getRegions(
      physicalOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Set[Region] = {
    regionDAG
      .vertexSet()
      .asScala
      .filter(region => region.getOperators.map(_.id).contains(physicalOpId))
      .toSet
  }

  /**
    * For a dependee input link, although it connects two regions A->B, we include this link and its toOp in region A
    * so that the dependee link will be completed first.
    */
  def populateDependeeLinks(
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Unit = {

    val dependeeLinks = physicalPlan
      .topologicalIterator()
      .flatMap { physicalOpId =>
        val upstreamPhysicalOpIds = physicalPlan.getUpstreamPhysicalOpIds(physicalOpId)
        upstreamPhysicalOpIds.flatMap { upstreamPhysicalOpId =>
          physicalPlan
            .getLinksBetween(upstreamPhysicalOpId, physicalOpId)
            .filter(link =>
              physicalPlan
                .getOperator(physicalOpId)
                .isInputLinkDependee(link)
            )
        }
      }
      .toSet

    dependeeLinks
      .flatMap { link => getRegions(link.fromOpId, regionDAG).map(region => region -> link) }
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2))
      .foreach {
        case (region, links) =>
          val newRegion = region.copy(
            physicalLinks = region.physicalLinks ++ links,
            physicalOps =
              region.getOperators ++ links.map(_.toOpId).map(id => physicalPlan.getOperator(id)),
            ports = region.getPorts ++ links.map(dependeeLink =>
              GlobalPortIdentity(dependeeLink.toOpId, dependeeLink.toPortId, input = true)
            )
          )
          replaceVertex(regionDAG, region, newRegion)
      }
  }
}
