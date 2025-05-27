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

package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import edu.uci.ics.amber.core.storage.result.ExecutionResourcesMapping
import edu.uci.ics.amber.core.storage.{DocumentFactory, VFSResourceType, VFSURIFactory}
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.virtualidentity._
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import edu.uci.ics.amber.engine.architecture.logreplay.{ReplayDestination, ReplayLogRecord}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.Utils.{maptoStatusCode, stringToAggregatedState}
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.Tables._
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.WorkflowExecutionsDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowExecutions
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.SqlServer.withTransaction
import edu.uci.ics.texera.web.model.http.request.result.ResultExportRequest
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource._
import edu.uci.ics.texera.web.service.{ExecutionsMetadataPersistService, ResultExportService}
import io.dropwizard.auth.Auth
import org.jooq.DSLContext

import java.net.URI
import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object WorkflowExecutionsResource {
  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()
  final private lazy val executionsDao = new WorkflowExecutionsDao(context.configuration)

  def getExecutionById(eId: Integer): WorkflowExecutions = {
    executionsDao.fetchOneByEid(eId)
  }

  def getExpiredExecutionsWithResultOrLog(timeToLive: Int): List[WorkflowExecutions] = {
    val deadline = new Timestamp(
      System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(timeToLive)
    )
    context
      .selectFrom(WORKFLOW_EXECUTIONS)
      .where(
        WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME.isNull
          .and(WORKFLOW_EXECUTIONS.STARTING_TIME.lt(deadline))
          .or(WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME.lt(deadline))
      )
      .and(
        WORKFLOW_EXECUTIONS.RESULT.ne("").or(WORKFLOW_EXECUTIONS.LOG_LOCATION.ne(""))
      )
      .fetchInto(classOf[WorkflowExecutions])
      .asScala
      .toList
  }

  /**
    * This function retrieves the latest execution id of a workflow
    *
    * @param wid workflow id
    * @return Integer
    */
  def getLatestExecutionID(wid: Integer, cuid: Integer): Option[Integer] = {
    val executions = context
      .select(WORKFLOW_EXECUTIONS.EID)
      .from(WORKFLOW_EXECUTIONS)
      .join(WORKFLOW_VERSION)
      .on(WORKFLOW_EXECUTIONS.VID.eq(WORKFLOW_VERSION.VID))
      .where(WORKFLOW_VERSION.WID.eq(wid).and(WORKFLOW_EXECUTIONS.CUID.eq(cuid)))
      .fetchInto(classOf[Integer])
      .asScala
      .toList
    if (executions.isEmpty) {
      None
    } else {
      Some(executions.max)
    }
  }

  def insertOperatorPortResultUri(
      eid: ExecutionIdentity,
      globalPortId: GlobalPortIdentity,
      uri: URI
  ): Unit = {
    if (AmberConfig.isUserSystemEnabled) {
      context
        .insertInto(OPERATOR_PORT_EXECUTIONS)
        .columns(
          OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID,
          OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID,
          OPERATOR_PORT_EXECUTIONS.RESULT_URI
        )
        .values(eid.id.toInt, globalPortId.serializeAsString, uri.toString)
        .execute()
    } else {
      ExecutionResourcesMapping.addResourceUri(eid, uri)
    }
  }

  def insertOperatorExecutions(
      eid: Long,
      opId: String,
      uri: URI
  ): Unit = {
    context
      .insertInto(OPERATOR_EXECUTIONS)
      .columns(
        OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_EXECUTIONS.OPERATOR_ID,
        OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_URI
      )
      .values(eid.toInt, opId, uri.toString)
      .execute()
  }

  def updateRuntimeStatsUri(wid: Long, eid: Long, uri: URI): Unit = {
    context
      .update(WORKFLOW_EXECUTIONS)
      .set(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI, uri.toString)
      .where(
        WORKFLOW_EXECUTIONS.EID
          .eq(eid.toInt)
          .and(
            WORKFLOW_EXECUTIONS.VID.in(
              context
                .select(WORKFLOW_VERSION.VID)
                .from(WORKFLOW_VERSION)
                .where(WORKFLOW_VERSION.WID.eq(wid.toInt))
            )
          )
      )
      .execute()
  }

  def getResultUrisByExecutionId(eid: ExecutionIdentity): List[URI] = {
    if (AmberConfig.isUserSystemEnabled) {
      context
        .select(OPERATOR_PORT_EXECUTIONS.RESULT_URI)
        .from(OPERATOR_PORT_EXECUTIONS)
        .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
        .fetchInto(classOf[String])
        .asScala
        .toList
        .filter(uri => uri != null && uri.nonEmpty)
        .map(URI.create)
    } else {
      ExecutionResourcesMapping.getResourceURIs(eid)
    }
  }

  def getConsoleMessagesUriByExecutionId(eid: ExecutionIdentity): List[URI] =
    if (AmberConfig.isUserSystemEnabled)
      context
        .select(OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_URI)
        .from(OPERATOR_EXECUTIONS)
        .where(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
        .fetchInto(classOf[String])
        .asScala
        .toList
        .filter(uri => uri != null && uri.nonEmpty)
        .map(URI.create)
    else Nil

  def getRuntimeStatsUriByExecutionId(eid: ExecutionIdentity): Option[URI] =
    if (AmberConfig.isUserSystemEnabled)
      Option(
        context
          .select(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI)
          .from(WORKFLOW_EXECUTIONS)
          .where(WORKFLOW_EXECUTIONS.EID.eq(eid.id.toInt))
          .fetchOneInto(classOf[String])
      ).filter(_.nonEmpty)
        .map(URI.create)
    else None

  def getWorkflowExecutions(
      wid: Integer,
      context: DSLContext,
      statusCodes: Set[Byte] = Set.empty
  ): List[WorkflowExecutionEntry] = {
    var condition = WORKFLOW_VERSION.WID.eq(wid)

    if (statusCodes.nonEmpty) {
      condition = condition.and(
        WORKFLOW_EXECUTIONS.STATUS.in(statusCodes.map(Byte.box).asJava)
      )
    }

    context
      .select(
        WORKFLOW_EXECUTIONS.EID,
        WORKFLOW_EXECUTIONS.VID,
        WORKFLOW_EXECUTIONS.CUID,
        USER.NAME,
        USER.GOOGLE_AVATAR,
        WORKFLOW_EXECUTIONS.STATUS,
        WORKFLOW_EXECUTIONS.RESULT,
        WORKFLOW_EXECUTIONS.STARTING_TIME,
        WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME,
        WORKFLOW_EXECUTIONS.BOOKMARKED,
        WORKFLOW_EXECUTIONS.NAME,
        WORKFLOW_EXECUTIONS.LOG_LOCATION
      )
      .from(WORKFLOW_EXECUTIONS)
      .join(WORKFLOW_VERSION)
      .on(WORKFLOW_VERSION.VID.eq(WORKFLOW_EXECUTIONS.VID))
      .join(USER)
      .on(WORKFLOW_EXECUTIONS.UID.eq(USER.UID))
      .where(condition)
      .orderBy(WORKFLOW_EXECUTIONS.EID.desc())
      .fetchInto(classOf[WorkflowExecutionEntry])
      .asScala
      .toList
  }

  def deleteConsoleMessageAndExecutionResultUris(eid: ExecutionIdentity): Unit = {
    if (AmberConfig.isUserSystemEnabled) {
      context
        .delete(OPERATOR_PORT_EXECUTIONS)
        .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
        .execute()
      context
        .delete(OPERATOR_EXECUTIONS)
        .where(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
        .execute()
    } else {
      ExecutionResourcesMapping.removeExecutionResources(eid)
    }
  }

  /**
    * Removes all resources related to the specified execution IDs,
    * including runtime statistics, console messages, result documents, and database records.
    *
    * @param eids Array of execution IDs to be cleaned up.
    */
  def removeAllExecutionFiles(eids: Array[Integer]): Unit = {
    val eIdsLong = eids.map(_.toLong)
    val eIdsList = eIdsLong.toSeq.asJava

    // Collect all related document URIs (runtime stats, console logs, results)
    val uris: Seq[URI] = eIdsLong.flatMap { eid =>
      val execId = ExecutionIdentity(eid)
      WorkflowExecutionsResource
        .getRuntimeStatsUriByExecutionId(execId)
        .toList ++
        WorkflowExecutionsResource.getConsoleMessagesUriByExecutionId(execId) ++
        WorkflowExecutionsResource.getResultUrisByExecutionId(execId)
    }

    // Delete execution-related URIs from database tables
    context
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.EID.in(eIdsList))
      .execute()

    // Clear corresponding Iceberg documents
    uris.foreach { uri =>
      try {
        DocumentFactory.openDocument(uri)._1.clear()
      } catch {
        case _: Throwable =>
        // Document already deleted – safe to ignore
      }
    }
  }

  /**
    * Updates the result size of the corresponding Iceberg document in the database.
    *
    * @param eid          Execution ID associated with the result.
    * @param globalPortId Global port identifier for the operator output.
    * @param size         Size of the result in bytes.
    */
  def updateResultSize(
      eid: ExecutionIdentity,
      globalPortId: GlobalPortIdentity,
      size: Long
  ): Unit = {
    context
      .update(OPERATOR_PORT_EXECUTIONS)
      .set(OPERATOR_PORT_EXECUTIONS.RESULT_SIZE, Integer.valueOf(size.toInt))
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
      .and(OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID.eq(globalPortId.serializeAsString))
      .execute()
  }

  /**
    * Updates the size of the runtime statistics stored via Iceberg document.
    *
    * @param eid Execution ID associated with the runtime statistics document.
    */
  def updateRuntimeStatsSize(eid: ExecutionIdentity): Unit = {
    if (AmberConfig.isUserSystemEnabled) {
      val statsUriOpt = context
        .select(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI)
        .from(WORKFLOW_EXECUTIONS)
        .where(WORKFLOW_EXECUTIONS.EID.eq(eid.id.toInt))
        .fetchOptionalInto(classOf[String])
        .map(URI.create)

      if (statsUriOpt.isPresent) {
        val size = DocumentFactory.openDocument(statsUriOpt.get)._1.getTotalFileSize
        context
          .update(WORKFLOW_EXECUTIONS)
          .set(WORKFLOW_EXECUTIONS.RUNTIME_STATS_SIZE, Integer.valueOf(size.toInt))
          .where(WORKFLOW_EXECUTIONS.EID.eq(eid.id.toInt))
          .execute()
      }
    }
  }

  /**
    * Updates the size of the console message stored via Iceberg document.
    *
    * @param eid  Execution ID associated with the console message.
    * @param opId Operator ID of the corresponding operator.
    */
  def updateConsoleMessageSize(eid: ExecutionIdentity, opId: OperatorIdentity): Unit = {
    if (AmberConfig.isUserSystemEnabled) {
      val uriOpt = context
        .select(OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_URI)
        .from(OPERATOR_EXECUTIONS)
        .where(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
        .and(OPERATOR_EXECUTIONS.OPERATOR_ID.eq(opId.id))
        .fetchOptionalInto(classOf[String])
        .map(URI.create)

      if (uriOpt.isPresent) {
        val size = DocumentFactory.openDocument(uriOpt.get)._1.getTotalFileSize
        context
          .update(OPERATOR_EXECUTIONS)
          .set(OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_SIZE, Integer.valueOf(size.toInt))
          .where(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
          .and(OPERATOR_EXECUTIONS.OPERATOR_ID.eq(opId.id))
          .execute()
      }
    }
  }

  /**
    * This method is mainly used for frontend requests. Given a logicalOpId and an outputPortId of an execution,
    * this method finds the URI for a globalPortId that both: 1. matches the logicalOpId and outputPortId, and
    * 2. is an external port. Currently the lookup is O(n), where n is the number of globalPortIds for this execution.
    * TODO: Optimize the lookup once the frontend also has information about physical operators.
    * TODO: Remove the case of using ExecutionResourceMapping when user system is permenantly enabled even in dev mode.
    */
  def getResultUriByLogicalPortId(
      eid: ExecutionIdentity,
      opId: OperatorIdentity,
      portId: PortIdentity
  ): Option[URI] = {
    def isMatchingExternalPortURI(uri: URI): Boolean = {
      val (_, _, globalPortIdOption, resourceType) = VFSURIFactory.decodeURI(uri)
      globalPortIdOption.exists { globalPortId =>
        !globalPortId.portId.internal &&
        globalPortId.opId.logicalOpId == opId &&
        globalPortId.portId == portId &&
        resourceType == VFSResourceType.RESULT
      }
    }

    val urisOfEid: List[URI] =
      if (AmberConfig.isUserSystemEnabled) {
        context
          .select(OPERATOR_PORT_EXECUTIONS.RESULT_URI)
          .from(OPERATOR_PORT_EXECUTIONS)
          .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
          .fetchInto(classOf[String])
          .asScala
          .toList
          .map(URI.create)
      } else {
        ExecutionResourcesMapping.getResourceURIs(eid)
      }

    urisOfEid.find(isMatchingExternalPortURI)
  }

  /**
    * This method trys to find a URI corresponding to the globalPortId if it exists. If user system is enabled, this
    * method runs in O(1), otherwise O(n) where n is number of URIs in ExecutionResourceMapping.
    * TODO: Remove the case of using ExecutionResourceMapping when user system is permenantly enabled even in dev mode.
    */
  def getResultUriByGlobalPortId(
      eid: ExecutionIdentity,
      globalPortId: GlobalPortIdentity
  ): Option[URI] = {
    if (AmberConfig.isUserSystemEnabled) {
      Option(
        context
          .select(OPERATOR_PORT_EXECUTIONS.RESULT_URI)
          .from(OPERATOR_PORT_EXECUTIONS)
          .where(
            OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID
              .eq(eid.id.toInt)
              .and(OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID.eq(globalPortId.serializeAsString))
          )
          .fetchOneInto(classOf[String])
      ).map(URI.create)
    } else {
      def isMatchingPortURI(uri: URI): Boolean = {
        val (_, _, globalPortIdOption, resourceType) = VFSURIFactory.decodeURI(uri)
        globalPortIdOption.exists { retrievedGlobalPortId =>
          retrievedGlobalPortId == globalPortId &&
          resourceType == VFSResourceType.RESULT
        }
      }
      ExecutionResourcesMapping
        .getResourceURIs(eid)
        .find(isMatchingPortURI)
    }

  }

  case class WorkflowExecutionEntry(
      eId: Integer,
      vId: Integer,
      cuId: Integer,
      userName: String,
      googleAvatar: String,
      status: Byte,
      result: String,
      startingTime: Timestamp,
      completionTime: Timestamp,
      bookmarked: Boolean,
      name: String,
      logLocation: String
  )

  case class WorkflowRuntimeStatistics(
      operatorId: String,
      timestamp: Timestamp,
      inputTupleCount: Long,
      inputTupleSize: Long,
      outputTupleCount: Long,
      outputTupleSize: Long,
      dataProcessingTime: Long,
      controlProcessingTime: Long,
      idleTime: Long,
      numWorkers: Int,
      status: Int
  )
}

case class ExecutionGroupBookmarkRequest(
    wid: Integer,
    eIds: Array[Integer],
    isBookmarked: Boolean
)

case class ExecutionGroupDeleteRequest(wid: Integer, eIds: Array[Integer])

case class ExecutionRenameRequest(wid: Integer, eId: Integer, executionName: String)

@Produces(Array(MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM, "application/zip"))
@Path("/executions")
class WorkflowExecutionsResource {

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{wid}/latest")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def retrieveLatestExecutionEntry(
      @PathParam("wid") wid: Integer,
      @Auth sessionUser: SessionUser
  ): WorkflowExecutionEntry = {

    validateUserCanAccessWorkflow(sessionUser.getUser.getUid, wid)

    withTransaction(context) { ctx =>
      val latestEntryOpt =
        ctx
          .select(
            WORKFLOW_EXECUTIONS.EID,
            WORKFLOW_EXECUTIONS.VID,
            WORKFLOW_EXECUTIONS.CUID,
            USER.NAME,
            USER.GOOGLE_AVATAR,
            WORKFLOW_EXECUTIONS.STATUS,
            WORKFLOW_EXECUTIONS.RESULT,
            WORKFLOW_EXECUTIONS.STARTING_TIME,
            WORKFLOW_EXECUTIONS.LAST_UPDATE_TIME,
            WORKFLOW_EXECUTIONS.BOOKMARKED,
            WORKFLOW_EXECUTIONS.NAME,
            WORKFLOW_EXECUTIONS.LOG_LOCATION
          )
          .from(WORKFLOW_EXECUTIONS)
          .join(WORKFLOW_VERSION)
          .on(WORKFLOW_VERSION.VID.eq(WORKFLOW_EXECUTIONS.VID))
          .join(USER)
          .on(WORKFLOW_EXECUTIONS.UID.eq(USER.UID))
          .where(WORKFLOW_VERSION.WID.eq(wid))
          // sort by latest VID first, then latest start-time
          .orderBy(
            WORKFLOW_EXECUTIONS.VID.desc(),
            WORKFLOW_EXECUTIONS.EID.desc()
          )
          .limit(1)
          .fetchInto(classOf[WorkflowExecutionEntry])
          .asScala
          .headOption

      latestEntryOpt.getOrElse {
        throw new ForbiddenException("Executions doesn't exist")
      }
    }
  }

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{wid}/interactions/{eid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def retrieveInteractionHistory(
      @PathParam("wid") wid: Integer,
      @PathParam("eid") eid: Integer,
      @Auth sessionUser: SessionUser
  ): List[String] = {
    val user = sessionUser.getUser
    if (!WorkflowAccessResource.hasReadAccess(wid, user.getUid)) {
      List()
    } else {
      ExecutionsMetadataPersistService.tryGetExistingExecution(
        ExecutionIdentity(eid.longValue())
      ) match {
        case Some(value) =>
          val logLocation = value.getLogLocation
          if (logLocation != null && logLocation.nonEmpty) {
            val storage =
              SequentialRecordStorage.getStorage[ReplayLogRecord](Some(new URI(logLocation)))
            val result = new mutable.ArrayBuffer[ChannelMarkerIdentity]()
            storage.getReader("CONTROLLER").mkRecordIterator().foreach {
              case destination: ReplayDestination =>
                result.append(destination.id)
              case _ =>
            }
            result.map(_.id).toList
          } else {
            List()
          }
        case None => List()
      }
    }
  }

  /**
    * This method returns the executions of a workflow given by its ID
    *
    * @return executions[]
    */
  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{wid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def retrieveExecutionsOfWorkflow(
      @PathParam("wid") wid: Integer,
      @Auth sessionUser: SessionUser,
      @QueryParam("status") status: String
  ): List[WorkflowExecutionEntry] = {
    val user = sessionUser.getUser
    if (!WorkflowAccessResource.hasReadAccess(wid, user.getUid)) {
      List()
    } else {
      val statusCodes: Set[Byte] =
        Option(status)
          .map(_.trim)
          .filter(_.nonEmpty)
          .map { raw =>
            val tokens = raw.split(',').map(_.trim.toLowerCase).filter(_.nonEmpty)
            try {
              tokens.map(stringToAggregatedState).map(maptoStatusCode).toSet
            } catch {
              case e: IllegalArgumentException =>
                throw new BadRequestException(e.getMessage)
            }
          }
          .getOrElse(Set.empty[Byte])
      getWorkflowExecutions(wid, context, statusCodes)
    }
  }

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{wid}/stats/{eid}")
  def retrieveWorkflowRuntimeStatistics(
      @PathParam("wid") wid: Integer,
      @PathParam("eid") eid: Integer
  ): List[WorkflowRuntimeStatistics] = {
    // Create URI for runtime statistics
    val uriString: String = context
      .select(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI)
      .from(WORKFLOW_EXECUTIONS)
      .where(
        WORKFLOW_EXECUTIONS.EID
          .eq(eid)
          .and(
            WORKFLOW_EXECUTIONS.VID.in(
              context
                .select(WORKFLOW_VERSION.VID)
                .from(WORKFLOW_VERSION)
                .where(WORKFLOW_VERSION.WID.eq(wid))
            )
          )
      )
      .fetchOneInto(classOf[String])

    if (uriString == null || uriString.isEmpty) {
      throw new NoSuchElementException(
        "No runtime statistics URI found for the given execution ID."
      )
    }

    val uri: URI = new URI(uriString)
    val document = DocumentFactory.openDocument(uri)._1

    // Read all records from Iceberg and convert to WorkflowRuntimeStatistics
    document
      .get()
      .map(tuple => {
        val record = tuple.asInstanceOf[Tuple]
        WorkflowRuntimeStatistics(
          operatorId = record.getField(0).asInstanceOf[String],
          timestamp = record.getField(1).asInstanceOf[Timestamp],
          inputTupleCount = record.getField(2).asInstanceOf[Long],
          inputTupleSize = record.getField(3).asInstanceOf[Long],
          outputTupleCount = record.getField(4).asInstanceOf[Long],
          outputTupleSize = record.getField(5).asInstanceOf[Long],
          dataProcessingTime = record.getField(6).asInstanceOf[Long],
          controlProcessingTime = record.getField(7).asInstanceOf[Long],
          idleTime = record.getField(8).asInstanceOf[Long],
          numWorkers = record.getField(9).asInstanceOf[Int],
          status = record.getField(10).asInstanceOf[Int]
        )
      })
      .toList
  }

  /** Sets a group of executions' bookmarks to the payload passed in the body. */
  @PUT
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("/set_execution_bookmarks")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def setExecutionAreBookmarked(
      request: ExecutionGroupBookmarkRequest,
      @Auth sessionUser: SessionUser
  ): Unit = {
    validateUserCanAccessWorkflow(sessionUser.getUser.getUid, request.wid)
    val eIdsList = request.eIds.toSeq.asJava
    if (request.isBookmarked) {
      // If currently bookmarked, un-bookmark (set bookmarked = false)
      context
        .update(WORKFLOW_EXECUTIONS)
        .set(WORKFLOW_EXECUTIONS.BOOKMARKED, java.lang.Boolean.valueOf(false))
        .where(WORKFLOW_EXECUTIONS.EID.in(eIdsList))
        .execute()
    } else {
      // If currently not bookmarked, bookmark (set bookmarked = true)
      context
        .update(WORKFLOW_EXECUTIONS)
        .set(WORKFLOW_EXECUTIONS.BOOKMARKED, java.lang.Boolean.valueOf(true))
        .where(WORKFLOW_EXECUTIONS.EID.in(eIdsList))
        .execute()
    }

  }

  /** Determine if user is authorized to access the workflow, if not raise 401 */
  def validateUserCanAccessWorkflow(uid: Integer, wid: Integer): Unit = {
    if (!WorkflowAccessResource.hasReadAccess(wid, uid))
      throw new WebApplicationException(Response.Status.UNAUTHORIZED)
  }

  /** Delete a group of executions */
  @PUT
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("/delete_executions")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def groupDeleteExecutionsOfWorkflow(
      request: ExecutionGroupDeleteRequest,
      @Auth sessionUser: SessionUser
  ): Unit = {
    validateUserCanAccessWorkflow(sessionUser.getUser.getUid, request.wid)
    removeAllExecutionFiles(request.eIds)
  }

  /** Name a single execution * */
  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Path("/update_execution_name")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def updateWorkflowExecutionsName(
      request: ExecutionRenameRequest,
      @Auth sessionUser: SessionUser
  ): Unit = {
    validateUserCanAccessWorkflow(sessionUser.getUser.getUid, request.wid)
    val execution = getExecutionById(request.eId)
    execution.setName(request.executionName)
    executionsDao.update(execution)
  }

  @POST
  @Path("/result/export")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def exportResult(
      request: ResultExportRequest,
      @Auth user: SessionUser
  ): Response = {

    if (request.operators.size <= 0)
      Response
        .status(Response.Status.BAD_REQUEST)
        .`type`(MediaType.APPLICATION_JSON)
        .entity(Map("error" -> "No operator selected").asJava)
        .build()

    try {
      request.destination match {
        case "local" =>
          // CASE A: multiple operators => produce ZIP
          if (request.operators.size > 1) {
            val resultExportService =
              new ResultExportService(WorkflowIdentity(request.workflowId), request.computingUnitId)
            val (zipStream, zipFileNameOpt) =
              resultExportService.exportOperatorsAsZip(request)

            if (zipStream == null) {
              throw new RuntimeException("Zip stream is null")
            }

            val finalFileName = zipFileNameOpt.getOrElse("operators.zip")
            return Response
              .ok(zipStream, "application/zip")
              .header("Content-Disposition", "attachment; filename=\"" + finalFileName + "\"")
              .build()
          }

          // CASE B: exactly one operator => single file
          if (request.operators.size != 1) {
            return Response
              .status(Response.Status.BAD_REQUEST)
              .`type`(MediaType.APPLICATION_JSON)
              .entity(Map("error" -> "Local download does not support no operator.").asJava)
              .build()
          }
          val singleOp = request.operators.head

          val resultExportService =
            new ResultExportService(WorkflowIdentity(request.workflowId), request.computingUnitId)
          val (streamingOutput, fileNameOpt) =
            resultExportService.exportOperatorResultAsStream(request, singleOp)

          if (streamingOutput == null) {
            return Response
              .status(Response.Status.INTERNAL_SERVER_ERROR)
              .`type`(MediaType.APPLICATION_JSON)
              .entity(Map("error" -> "Failed to export operator").asJava)
              .build()
          }

          val finalFileName = fileNameOpt.getOrElse("download.dat")
          Response
            .ok(streamingOutput, MediaType.APPLICATION_OCTET_STREAM)
            .header("Content-Disposition", "attachment; filename=\"" + finalFileName + "\"")
            .build()
        case _ =>
          // destination = "dataset" by default
          val resultExportService =
            new ResultExportService(WorkflowIdentity(request.workflowId), request.computingUnitId)
          val exportResponse =
            resultExportService.exportAllOperatorsResultToDataset(user.user, request)
          Response.ok(exportResponse).build()
      }
    } catch {
      case ex: Exception =>
        Response
          .status(Response.Status.INTERNAL_SERVER_ERROR)
          .`type`(MediaType.APPLICATION_JSON)
          .entity(Map("error" -> ex.getMessage).asJava)
          .build()
    }
  }
}
