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

import { AfterViewInit, Component, Inject, OnInit, Optional } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { EXECUTION_STATUS_CODE, WorkflowExecutionsEntry } from "../../../../type/workflow-executions-entry";
import { WorkflowExecutionsService } from "../../../../service/user/workflow-executions/workflow-executions.service";
import { ExecutionState } from "../../../../../workspace/types/execute-workflow.interface";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { WorkflowActionService } from "../../../../../workspace/service/workflow-graph/model/workflow-action.service";
import Fuse from "fuse.js";
import { ceil } from "lodash";
import { NZ_MODAL_DATA, NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { WorkflowRuntimeStatisticsComponent } from "./workflow-runtime-statistics/workflow-runtime-statistics.component";
import * as Plotly from "plotly.js-basic-dist-min";
import { ActivatedRoute } from "@angular/router";

const MAX_TEXT_SIZE = 20;
const MAX_RGB = 255;
const MAX_USERNAME_SIZE = 5;

@UntilDestroy()
@Component({
  selector: "texera-ngbd-modal-workflow-executions",
  templateUrl: "./workflow-execution-history.component.html",
  styleUrls: ["./workflow-execution-history.component.scss"],
})
export class WorkflowExecutionHistoryComponent implements OnInit, AfterViewInit {
  wid: number = 0;
  public static readonly USERNAME_PIE_CHART_ID = "#execution-userName-pie-chart";
  public static readonly STATUS_PIE_CHART_ID = "#execution-status-pie-chart";
  public static readonly PROCESS_TIME_BAR_CHART = "#execution-average-process-time-bar-chart";
  public static readonly WIDTH = 450;
  public static readonly HEIGHT = 450;
  public static readonly BARCHARTSIZE = 600;

  // Instance properties referencing the static ones
  public usernamePieChartId = WorkflowExecutionHistoryComponent.USERNAME_PIE_CHART_ID;
  public statusPieChartId = WorkflowExecutionHistoryComponent.STATUS_PIE_CHART_ID;
  public processTimeBarChart = WorkflowExecutionHistoryComponent.PROCESS_TIME_BAR_CHART;
  public workflowExecutionsDisplayedList: WorkflowExecutionsEntry[] | undefined;
  public workflowExecutionsIsEditingName: number[] = [];
  public currentlyHoveredExecution: WorkflowExecutionsEntry | undefined;
  public executionsTableHeaders: string[] = [
    "",
    "Avatar",
    "Name (ID)",
    "Computing Unit ID",
    "Execution Start Time",
    "Execution Completion Time",
    "Status",
    "Runtime Statistics",
    "",
  ];
  /*Tooltip for each header in execution table*/
  public executionTooltip: Record<string, string> = {
    "Name (ID)": "Execution Name",
    "Computing Unit ID": "ID of the Computing Unit that ran the Workflow",
    Username: "The User Who Ran This Execution",
    "Execution Start Time": "Start Time of Workflow Execution",
    "Execution Completion Time": "Latest Status Updated Time of Workflow Execution",
    Status: "Current Status of Workflow Execution",
    "Runtime Statistics": "Runtime Statistics of Workflow Execution",
    "Group Bookmarking": "Mark or Unmark the Selected Entries",
    "Group Deletion": "Delete the Selected Entries",
  };

  /*custom column width*/
  public customColumnWidth: Record<string, string> = {
    "": "0%",
    "Name (ID)": "7%",
    "Computing Unit ID": "7%",
    "Workflow Version Sample": "10%",
    Avatar: "5.5%",
    "Execution Start Time": "9%",
    "Execution Completion Time": "10.5%",
    Status: "2.5%",
    "Runtime Statistics": "6%",
  };

  /** variables related to executions filtering
   */
  public allExecutionEntries: WorkflowExecutionsEntry[] = [];
  public filteredExecutionInfo: Array<string> = [];
  public executionSearchValue: string = "";
  public searchCriteria: string[] = ["user", "status"];
  public fuse = new Fuse([] as ReadonlyArray<WorkflowExecutionsEntry>, {
    shouldSort: true,
    threshold: 0.2,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["name", "userName", "status"],
  });

  // Pagination attributes
  public currentPageIndex: number = 1;
  public pageSize: number = 10;
  public pageSizeOptions: number[] = [5, 10, 20, 30, 40];
  public paginatedExecutionEntries: WorkflowExecutionsEntry[] = [];

  public searchCriteriaPathMapping: Map<string, string[]> = new Map([
    ["executionName", ["name"]],
    ["user", ["userName"]],
    ["status", ["status"]],
  ]);
  public statusMapping: Map<string, number> = new Map([
    ["initializing", 0],
    ["running", 1],
    ["paused", 2],
    ["completed", 3],
    ["failed", 4],
    ["killed", 5],
  ]);
  public showORhide: boolean[] = [false, false, false, false, true];
  public avatarColors: { [key: string]: string } = {};
  public checked: boolean = false;
  public setOfEid = new Set<number>();
  public setOfExecution = new Set<WorkflowExecutionsEntry>();
  public averageProcessingTimeDivider: number = 10;
  modalRef?: NzModalRef;

  constructor(
    private workflowExecutionsService: WorkflowExecutionsService,
    private notificationService: NotificationService,
    private runtimeStatisticsModal: NzModalService,
    private workflowActionService: WorkflowActionService,
    private route: ActivatedRoute,
    @Optional() @Inject(NZ_MODAL_DATA) private modalData: any
  ) {}

  ngOnInit(): void {
    this.wid = this.modalData?.wid || this.route.snapshot.params["id"] || 0;
    // gets the workflow executions and display the runs in the table on the form
    this.displayWorkflowExecutions();
  }

  ngAfterViewInit() {
    this.workflowExecutionsService
      .retrieveWorkflowExecutions(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(workflowExecutions => {
        // generate charts data
        let userNameData: { [key: string]: [string, number] } = {};
        let statusData: { [key: string]: [string, number] } = {};

        workflowExecutions.forEach(execution => {
          if (userNameData[execution.userName] === undefined) {
            userNameData[execution.userName] = [execution.userName, 0];
          }
          userNameData[execution.userName][1] += 1;
          if (statusData[EXECUTION_STATUS_CODE[execution.status]] === undefined) {
            statusData[EXECUTION_STATUS_CODE[execution.status]] = [EXECUTION_STATUS_CODE[execution.status], 0];
          }
          statusData[EXECUTION_STATUS_CODE[execution.status]][1] += 1;
        });

        this.generatePieChart(
          Object.values(userNameData),
          "Users who ran the execution",
          WorkflowExecutionHistoryComponent.USERNAME_PIE_CHART_ID
        );

        this.generatePieChart(
          Object.values(statusData),
          "Executions status",
          WorkflowExecutionHistoryComponent.STATUS_PIE_CHART_ID
        );
        // generate an average processing time bar chart
        const processTimeData: Array<[string, ...number[]]> = [["processing time"]];
        const processTimeCategory: string[] = [];
        Object.entries(this.getBarChartProcessTimeData(workflowExecutions)).forEach(([eId, processTime]) => {
          processTimeData[0].push(processTime);
          processTimeCategory.push(eId);
        });
        this.generateBarChart(
          processTimeData,
          processTimeCategory,
          "Execution Numbers",
          "Average Processing Time (m)",
          "Execution performance",
          WorkflowExecutionHistoryComponent.PROCESS_TIME_BAR_CHART
        );
      });
  }

  generatePieChart(dataToDisplay: Array<[string, ...number[]]>, title: string, chart: string) {
    var data = [
      {
        values: dataToDisplay.map(d => d[1]),
        labels: dataToDisplay.map(d => d[0]),
        type: "pie" as const,
      },
    ];
    var layout = {
      height: WorkflowExecutionHistoryComponent.HEIGHT,
      width: WorkflowExecutionHistoryComponent.WIDTH,
      title: {
        text: title,
      },
    };
    Plotly.newPlot(chart, data, layout);
  }

  generateBarChart(
    dataToDisplay: Array<[string, ...number[]]>,
    category: string[],
    x_label: string,
    y_label: string,
    title: string,
    chart: string
  ) {
    var data = [
      {
        x: category.map(c => `${c}`),
        y: dataToDisplay[0].slice(1),
        type: "bar" as const,
      },
    ];

    var layout = {
      title: title,
      xaxis: {
        title: x_label,
      },
      yaxis: {
        title: y_label,
      },
      autosize: false,
      width: WorkflowExecutionHistoryComponent.BARCHARTSIZE,
      height: WorkflowExecutionHistoryComponent.BARCHARTSIZE,
    };

    Plotly.newPlot(chart, data, layout);
  }

  /**
   * calls the service to display the workflow executions on the table
   */
  displayWorkflowExecutions(): void {
    this.workflowExecutionsService
      .retrieveWorkflowExecutions(this.wid)
      .pipe(untilDestroyed(this))
      .subscribe(workflowExecutions => {
        this.allExecutionEntries = workflowExecutions;
        this.dscSort("Execution Start Time");
        this.updatePaginatedExecutions();
      });
  }

  /**
   * display icons corresponding to workflow execution status
   *
   * NOTES: Colors match with gui/src/app/workspace/service/joint-ui/joint-ui.service.ts line 347
   * TODO: Move colors to a config file for changing them once for many files
   */
  getExecutionStatus(statusCode: number): string[] {
    switch (statusCode) {
      case 0:
        return [ExecutionState.Initializing.toString(), "sync", "#a6bd37"];
      case 1:
        return [ExecutionState.Running.toString(), "play-circle", "orange"];
      case 2:
        return [ExecutionState.Paused.toString(), "pause-circle", "magenta"];
      case 3:
        return [ExecutionState.Completed.toString(), "check-circle", "green"];
      case 4:
        return [ExecutionState.Failed.toString(), "exclamation-circle", "gray"];
      case 5:
        return [ExecutionState.Killed.toString(), "minus-circle", "red"];
    }
    return ["", "question-circle", "gray"];
  }

  onBookmarkToggle(row: WorkflowExecutionsEntry) {
    const wasPreviouslyBookmarked = row.bookmarked;
    // Update bookmark state locally.
    row.bookmarked = !wasPreviouslyBookmarked;

    // Update on the server.
    this.workflowExecutionsService
      .groupSetIsBookmarked(this.wid, [row.eId], wasPreviouslyBookmarked)
      .pipe(untilDestroyed(this))
      .subscribe({
        error: (_: unknown) => (row.bookmarked = wasPreviouslyBookmarked),
      });
  }

  setBookmarked(): void {
    if (this.setOfExecution !== undefined) {
      // isBookmarked: true if all the execution are bookmarked, false if there is one that is unbookmarked
      const isBookmarked = !Array.from(this.setOfExecution).some(execution => {
        return execution.bookmarked === null || !execution.bookmarked;
      });
      // update the bookmark locally
      this.setOfExecution.forEach(execution => {
        execution.bookmarked = !isBookmarked;
      });
      this.workflowExecutionsService
        .groupSetIsBookmarked(this.wid, Array.from(this.setOfEid), isBookmarked)
        .pipe(untilDestroyed(this))
        .subscribe({});
    }
  }

  /* delete a single execution */

  onDelete(row: WorkflowExecutionsEntry) {
    this.workflowExecutionsService
      .groupDeleteWorkflowExecutions(this.wid, [row.eId])
      .pipe(untilDestroyed(this))
      .subscribe({
        complete: () => {
          this.allExecutionEntries?.splice(this.allExecutionEntries.indexOf(row), 1);
          this.handlePaginationAfterDeletingExecutions();
        },
      });
  }

  onGroupDelete() {
    this.workflowExecutionsService
      .groupDeleteWorkflowExecutions(this.wid, Array.from(this.setOfEid))
      .pipe(untilDestroyed(this))
      .subscribe({
        complete: () => {
          this.allExecutionEntries = this.allExecutionEntries?.filter(
            execution => !Array.from(this.setOfExecution).includes(execution)
          );
          this.handlePaginationAfterDeletingExecutions();
          this.setOfEid.clear();
          this.setOfExecution.clear();
        },
      });
  }

  /* rename a single execution */

  confirmUpdateWorkflowExecutionsCustomName(row: WorkflowExecutionsEntry, name: string, index: number): void {
    // if name doesn't change, no need to call API
    if (name === row.name) {
      this.workflowExecutionsIsEditingName = this.workflowExecutionsIsEditingName.filter(
        entryIsEditingIndex => entryIsEditingIndex != index
      );
      return;
    }

    this.workflowExecutionsService
      .updateWorkflowExecutionsName(this.wid, row.eId, name)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.workflowExecutionsDisplayedList === undefined) {
          return;
        }
        // change the execution name globally
        this.allExecutionEntries[this.allExecutionEntries.indexOf(this.workflowExecutionsDisplayedList[index])].name =
          name;
        this.paginatedExecutionEntries[
          this.paginatedExecutionEntries.indexOf(this.workflowExecutionsDisplayedList[index])
        ].name = name;
        this.workflowExecutionsDisplayedList[index].name = name;
        this.fuse.setCollection(this.paginatedExecutionEntries);
      })
      .add(() => {
        this.workflowExecutionsIsEditingName = this.workflowExecutionsIsEditingName.filter(
          entryIsEditingIndex => entryIsEditingIndex != index
        );
      });
  }

  /* sort executions by name/username/start time/update time
   based in ascending alphabetical order */

  ascSort(type: string): void {
    if (type === "Name (ID)") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) => exe1.name.toLowerCase().localeCompare(exe2.name.toLowerCase()));
    } else if (type === "Username") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) => exe1.userName.toLowerCase().localeCompare(exe2.userName.toLowerCase()));
    } else if (type === "Execution Start Time") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) =>
          exe1.startingTime > exe2.startingTime ? 1 : exe2.startingTime > exe1.startingTime ? -1 : 0
        );
    } else if (type == "Execution Completion Time") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) =>
          exe1.completionTime > exe2.completionTime ? 1 : exe2.completionTime > exe1.completionTime ? -1 : 0
        );
    } else if (type === "Computing Unit ID") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((a, b) => a.cuId - b.cuId);
    }
  }

  /* sort executions by name/username/start time/update time
   based in descending alphabetical order */

  dscSort(type: string): void {
    if (type === "Name (ID)") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) => exe2.name.toLowerCase().localeCompare(exe1.name.toLowerCase()));
    } else if (type === "Username") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) => exe2.userName.toLowerCase().localeCompare(exe1.userName.toLowerCase()));
    } else if (type === "Execution Start Time") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) =>
          exe1.startingTime < exe2.startingTime ? 1 : exe2.startingTime < exe1.startingTime ? -1 : 0
        );
    } else if (type == "Execution Completion Time") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) =>
          exe1.completionTime < exe2.completionTime ? 1 : exe2.completionTime < exe1.completionTime ? -1 : 0
        );
    } else if (type === "Computing Unit ID") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((a, b) => b.cuId - a.cuId);
    }
  }

  /**
   *
   * @param name
   * @param nameFlag true for execution name and false for username
   */
  abbreviate(name: string, nameFlag: boolean): string {
    let maxLength = nameFlag ? MAX_TEXT_SIZE : MAX_USERNAME_SIZE;
    if (name.length <= maxLength) {
      return name;
    } else {
      return name.slice(0, maxLength);
    }
  }

  onHit(column: string, index: number): void {
    if (this.showORhide[index]) {
      this.ascSort(column);
    } else {
      this.dscSort(column);
    }
    this.showORhide[index] = !this.showORhide[index];
  }

  setAvatarColor(userName: string): string {
    if (userName in this.avatarColors) {
      return this.avatarColors[userName];
    } else {
      this.avatarColors[userName] = this.getRandomColor();
      return this.avatarColors[userName];
    }
  }

  getRandomColor(): string {
    const r = Math.floor(Math.random() * MAX_RGB);
    const g = Math.floor(Math.random() * MAX_RGB);
    const b = Math.floor(Math.random() * MAX_RGB);
    return "rgba(" + r + "," + g + "," + b + ",0.8)";
  }

  /**
   * Update the eId set to keep track of the status of the checkbox
   * @param eId
   * @param checked true if checked false if unchecked
   */
  updateEidSet(eId: number, checked: boolean): void {
    if (checked) {
      this.setOfEid.add(eId);
    } else {
      this.setOfEid.delete(eId);
    }
  }

  /**
   * Update the row set to keep track of the status of the checkbox
   * @param row
   * @param checked true if checked false if unchecked
   */
  updateRowSet(row: WorkflowExecutionsEntry, checked: boolean): void {
    if (checked) {
      this.setOfExecution.add(row);
    } else {
      this.setOfExecution.delete(row);
    }
  }

  /**
   * Mark all the checkboxes checked and check the status of the all check
   * @param value true if we need to check all false if we need to uncheck all
   */
  onAllChecked(value: boolean): void {
    if (this.paginatedExecutionEntries !== undefined) {
      for (let execution of this.paginatedExecutionEntries) {
        this.updateEidSet(execution.eId, value);
        this.updateRowSet(execution, value);
      }
    }
    this.refreshCheckedStatus();
  }

  /**
   * Update the eId and row set, and check the status of the all check
   * @param row
   * @param checked true if checked false if unchecked
   */
  onItemChecked(row: WorkflowExecutionsEntry, checked: boolean) {
    this.updateEidSet(row.eId, checked);
    this.updateRowSet(row, checked);
    this.refreshCheckedStatus();
  }

  /**
   * Check the status of the all check
   */
  refreshCheckedStatus(): void {
    if (this.paginatedExecutionEntries !== undefined) {
      this.checked = this.paginatedExecutionEntries.length === this.setOfEid.size;
    }
  }

  public searchInputOnChange(value: string): void {
    const searchConditionsSet = [...new Set(value.trim().split(/ +(?=(?:(?:[^"]*"){2})*[^"]*$)/g))];
    searchConditionsSet.forEach((condition, index) => {
      const preCondition = searchConditionsSet.slice(0, index);
      var executionSearchField = "";
      var executionSearchValue = "";
      if (condition.includes(":")) {
        const conditionArray = condition.split(":");
        executionSearchField = conditionArray[0];
        executionSearchValue = conditionArray[1];
      } else {
        executionSearchField = "executionName";
        executionSearchValue = preCondition
          ? value.slice(preCondition.map(c => c.length).reduce((a, b) => a + b, 0) + preCondition.length)
          : value;
      }
      const filteredExecutionInfo: string[] = [];
      this.paginatedExecutionEntries.forEach(executionEntry => {
        const searchField = this.searchCriteriaPathMapping.get(executionSearchField);
        var executionInfo = "";
        if (searchField === undefined) {
          return;
        } else {
          executionInfo =
            searchField[0] === "status"
              ? [...this.statusMapping.entries()]
                  .filter(({ 1: val }) => val === executionEntry.status)
                  .map(([key]) => key)[0]
              : Object.values(executionEntry)[Object.keys(executionEntry).indexOf(searchField[0])];
        }
        if (executionInfo.toLowerCase().indexOf(executionSearchValue.toLowerCase()) !== -1) {
          let filterQuery: string;
          if (preCondition.length !== 0) {
            filterQuery =
              executionSearchField === "executionName"
                ? preCondition.join(" ") + " " + executionInfo
                : preCondition.join(" ") + " " + executionSearchField + ":" + executionInfo;
          } else {
            filterQuery =
              executionSearchField === "executionName" ? executionInfo : executionSearchField + ":" + executionInfo;
          }
          filteredExecutionInfo.push(filterQuery);
        }
      });
      this.filteredExecutionInfo = [...new Set(filteredExecutionInfo)];
    });
  }

  // check https://fusejs.io/api/query.html#logical-query-operators for logical query operators rule
  public buildAndPathQuery(
    executionSearchField: string,
    executionSearchValue: string
  ): {
    $path: ReadonlyArray<string>;
    $val: string;
  } {
    return {
      $path: this.searchCriteriaPathMapping.get(executionSearchField) as ReadonlyArray<string>,
      $val: executionSearchValue,
    };
  }

  /**
   * Search executions by execution name, user name, or status
   * Use fuse.js https://fusejs.io/ as the tool for searching
   */
  public searchExecution(): void {
    // empty search value, return all execution entries
    if (this.executionSearchValue.trim() === "") {
      this.workflowExecutionsDisplayedList = this.paginatedExecutionEntries;
      return;
    }
    let andPathQuery: Object[] = [];
    const searchConditionsSet = new Set(this.executionSearchValue.trim().split(/ +(?=(?:(?:[^"]*"){2})*[^"]*$)/g));
    searchConditionsSet.forEach(condition => {
      // field search
      if (condition.includes(":")) {
        const conditionArray = condition.split(":");
        if (conditionArray.length !== 2) {
          this.notificationService.error("Please check the format of the search query");
          return;
        }
        const executionSearchField = conditionArray[0];
        const executionSearchValue = conditionArray[1].toLowerCase();
        if (!this.searchCriteria.includes(executionSearchField)) {
          this.notificationService.error("Cannot search by " + executionSearchField);
          return;
        }
        if (executionSearchField === "status") {
          var statusSearchValue = this.statusMapping.get(executionSearchValue)?.toString();
          // check if user type correct status
          if (statusSearchValue === undefined) {
            this.notificationService.error("Status " + executionSearchValue + " is not available to execution");
            return;
          }
          andPathQuery.push(this.buildAndPathQuery(executionSearchField, statusSearchValue));
        } else {
          // handle all other searches
          andPathQuery.push(this.buildAndPathQuery(executionSearchField, executionSearchValue));
        }
      } else {
        //search by execution name
        andPathQuery.push(this.buildAndPathQuery("executionName", condition));
      }
    });
    this.workflowExecutionsDisplayedList = this.fuse.search({ $and: andPathQuery }).map(res => res.item);
  }

  /* Pagination handler */

  /* Assign new page index and change current list */
  onPageIndexChange(pageIndex: number): void {
    this.currentPageIndex = pageIndex;
    this.updatePaginatedExecutions();
  }

  /* Assign new page size and change current list */
  onPageSizeChange(pageSize: number): void {
    this.pageSize = pageSize;
    this.updatePaginatedExecutions();
  }

  /**
   * Change current page list everytime the page change
   */
  changePaginatedExecutions(): WorkflowExecutionsEntry[] {
    this.executionSearchValue = "";
    return this.allExecutionEntries?.slice(
      (this.currentPageIndex - 1) * this.pageSize,
      this.currentPageIndex * this.pageSize
    );
  }

  getBarChartProcessTimeData(rows: WorkflowExecutionsEntry[]) {
    let processTimeData: { [key: string]: number } = {};
    let divider: number = ceil(rows.length / this.averageProcessingTimeDivider);
    let tracker = 0;
    let totProcessTime = 0;
    let category = "";
    let eIdToNumber = 1;
    rows.forEach(execution => {
      tracker++;

      let processTime = execution.completionTime - execution.startingTime;
      processTime = processTime / 60000;
      totProcessTime += processTime;
      if (tracker === 1) {
        category += String(eIdToNumber);
      }
      if (tracker === divider) {
        category += "~" + String(eIdToNumber);
        processTimeData[category] = totProcessTime / divider;
        tracker = 0;
        totProcessTime = 0;
        category = "";
      }
      eIdToNumber++;
    });
    return processTimeData;
  }

  showRuntimeStatistics(eId: number, cuid: number): void {
    this.workflowExecutionsService
      .retrieveWorkflowRuntimeStatistics(this.wid, eId, cuid)
      .pipe(untilDestroyed(this))
      .subscribe(workflowRuntimeStatistics => {
        this.modalRef = this.runtimeStatisticsModal.create({
          nzTitle: "Runtime Statistics",
          nzStyle: { top: "5px", width: "98vw", height: "92vh" },
          nzFooter: null, // null indicates that the footer of the window would be hidden
          nzBodyStyle: { width: "98vw", height: "92vh" },
          nzContent: WorkflowRuntimeStatisticsComponent,
          nzData: { workflowRuntimeStatistics: workflowRuntimeStatistics },
        });
      });
  }

  private updatePaginatedExecutions(): void {
    this.paginatedExecutionEntries = this.changePaginatedExecutions();
    this.workflowExecutionsDisplayedList = this.paginatedExecutionEntries;
    this.fuse.setCollection(this.paginatedExecutionEntries);
  }

  private handlePaginationAfterDeletingExecutions(): void {
    this.updatePaginatedExecutions();
    /* If a current page index has 0 number of execution entries after deletion (e.g., deleting all the executions in the last page),
     * the following code will decrement the current page index by 1. */
    if (this.currentPageIndex > 1 && this.paginatedExecutionEntries.length === 0) {
      this.onPageIndexChange(this.currentPageIndex - 1);
    }
  }
}
