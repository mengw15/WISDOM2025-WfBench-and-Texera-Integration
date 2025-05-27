/**
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

import { ExecutionState, LogicalPlan } from "../../types/execute-workflow.interface";
import { fakeAsync, flush, inject, TestBed, tick } from "@angular/core/testing";

import { ExecuteWorkflowService, FORM_DEBOUNCE_TIME_MS } from "./execute-workflow.service";

import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { Observable, of } from "rxjs";
import { Role, User } from "../../../common/type/user";

import { mockLogicalPlan_scan_result, mockWorkflowPlan_scan_result } from "./mock-workflow-plan";
import { HttpClient } from "@angular/common/http";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { WorkflowSnapshotService } from "../../../dashboard/service/user/workflow-snapshot/workflow-snapshot.service";
import { DOCUMENT } from "@angular/common";
import { WorkflowSettings } from "src/app/common/type/workflow";
import { ComputingUnitStatusService } from "../computing-unit-status/computing-unit-status.service";
import { JWT_OPTIONS, JwtHelperService } from "@auth0/angular-jwt";
import { AuthService } from "src/app/common/service/user/auth.service";
import { StubAuthService } from "src/app/common/service/user/stub-auth.service";
import { UserService } from "src/app/common/service/user/user.service";
import { StubUserService } from "src/app/common/service/user/stub-user.service";
import { MockComputingUnitStatusService } from "../computing-unit-status/mock-computing-unit-status.service";

class StubHttpClient {
  public post(): Observable<string> {
    return of("a");
  }
}

describe("ExecuteWorkflowService", () => {
  let service: ExecuteWorkflowService;
  let mockWorkflowSnapshotService: WorkflowSnapshotService;
  let mockDocument: Document;

  beforeEach(() => {
    mockDocument = {
      location: {
        origin: "https://texera.example.com",
      },
    } as Document;

    TestBed.configureTestingModule({
      providers: [
        ExecuteWorkflowService,
        WorkflowActionService,
        WorkflowUtilService,
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        UndoRedoService,
        JointUIService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        { provide: HttpClient, useClass: StubHttpClient },
        { provide: DOCUMENT, useValue: mockDocument },
        { provide: AuthService, useClass: StubAuthService },
        { provide: UserService, useClass: StubUserService },
      ],
    });

    service = TestBed.inject(ExecuteWorkflowService);
    mockWorkflowSnapshotService = TestBed.inject(WorkflowSnapshotService);
  });

  it("should be created", inject([ExecuteWorkflowService], (injectedService: ExecuteWorkflowService) => {
    expect(injectedService).toBeTruthy();
  }));

  it("should generate a logical plan request based on the workflow graph that is passed to the function", () => {
    const newLogicalPlan: LogicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(mockWorkflowPlan_scan_result);
    expect(newLogicalPlan).toEqual(mockLogicalPlan_scan_result);
  });

  it("should msg backend when executing workflow", fakeAsync(() => {
    const logicalPlan: LogicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(mockWorkflowPlan_scan_result);
    const wsSendSpy = spyOn((service as any).workflowWebsocketService, "send");
    const settings = service["workflowActionService"].getWorkflowSettings();
    service.sendExecutionRequest("", logicalPlan, settings, false, undefined);
    tick(FORM_DEBOUNCE_TIME_MS + 1);
    flush();
    expect(wsSendSpy).toHaveBeenCalledTimes(1);
  }));

  it("it should raise an error when pauseWorkflow() is called without an execution state", () => {
    (service as any).currentState = { state: ExecutionState.Uninitialized };
    expect(function () {
      service.pauseWorkflow();
    }).toThrowError(
      new RegExp("cannot pause workflow, the current execution state is " + (service as any).currentState.state)
    );
  });

  it("it should raise an error when resumeWorkflow() is called without an execution state", () => {
    (service as any).currentState = { state: ExecutionState.Uninitialized };
    expect(function () {
      service.resumeWorkflow();
    }).toThrowError(
      new RegExp("cannot resume workflow, the current execution state is " + (service as any).currentState.state)
    );
  });

  it("should execute workflow with email notification successfully", () => {
    const executionName = "Test Execution";
    const emailNotificationEnabled = true;
    const targetOperatorId = "test-operator-id";

    const logicalPlanSpy = spyOn(ExecuteWorkflowService, "getLogicalPlanRequest").and.returnValue({} as LogicalPlan);
    const settingsSpy = spyOn(service["workflowActionService"], "getWorkflowSettings").and.returnValue(
      {} as WorkflowSettings
    );
    const resetExecutionStateSpy = spyOn(service, "resetExecutionState");
    const resetStatusSpy = spyOn(service["workflowStatusService"], "resetStatus");
    const sendExecutionRequestSpy = spyOn(service, "sendExecutionRequest");

    service.executeWorkflowWithEmailNotification(executionName, emailNotificationEnabled, targetOperatorId);

    expect(logicalPlanSpy).toHaveBeenCalledWith(service["workflowActionService"].getTexeraGraph(), targetOperatorId);
    expect(settingsSpy).toHaveBeenCalled();
    expect(resetExecutionStateSpy).toHaveBeenCalled();
    expect(resetStatusSpy).toHaveBeenCalled();
    expect(sendExecutionRequestSpy).toHaveBeenCalledWith(
      executionName,
      jasmine.any(Object),
      jasmine.any(Object),
      emailNotificationEnabled
    );
  });

  it("should handle failure when executing workflow with email notification", () => {
    const executionName = "Test Execution";
    const emailNotificationEnabled = true;
    const targetOperatorId = "test-operator-id";

    const logicalPlanSpy = spyOn(ExecuteWorkflowService, "getLogicalPlanRequest").and.throwError("Logical plan error");
    const resetExecutionStateSpy = spyOn(service, "resetExecutionState");
    const resetStatusSpy = spyOn(service["workflowStatusService"], "resetStatus");
    const sendExecutionRequestSpy = spyOn(service, "sendExecutionRequest");

    expect(() => {
      service.executeWorkflowWithEmailNotification(executionName, emailNotificationEnabled, targetOperatorId);
    }).toThrowError("Logical plan error");

    expect(logicalPlanSpy).toHaveBeenCalledWith(service["workflowActionService"].getTexeraGraph(), targetOperatorId);
    expect(resetExecutionStateSpy).not.toHaveBeenCalled();
    expect(resetStatusSpy).not.toHaveBeenCalled();
    expect(sendExecutionRequestSpy).not.toHaveBeenCalled();
  });
});
