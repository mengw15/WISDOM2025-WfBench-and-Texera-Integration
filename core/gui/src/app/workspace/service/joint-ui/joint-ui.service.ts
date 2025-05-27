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

import { Injectable } from "@angular/core";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { OperatorSchema } from "../../types/operator-schema.interface";
import { abbreviateNumber } from "js-abbreviation-number";
import { CommentBox, OperatorLink, OperatorPredicate, Point } from "../../types/workflow-common.interface";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import * as joint from "jointjs";
import { fromEventPattern, Observable } from "rxjs";
import { Coeditor } from "../../../common/type/user";
import { OperatorResultCacheStatus } from "../../types/workflow-websocket.interface";
/**
 * Defines the SVG path for the delete button
 */
export const deleteButtonPath =
  "M14.59 8L12 10.59 9.41 8 8 9.41 10.59 12 8 14.59 9.41 16 12 13.41" +
  " 14.59 16 16 14.59 13.41 12 16 9.41 14.59 8zM12 2C6.47 2 2 6.47 2" +
  " 12s4.47 10 10 10 10-4.47 10-10S17.53 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z";

/**
 * Defines the HTML SVG element for the delete button and customizes the look
 */
export const deleteButtonSVG = `
  <svg class="delete-button" height="24" width="24">
    <path d="M0 0h24v24H0z" fill="none" pointer-events="visible" />
    <path d="${deleteButtonPath}"/>
    <title>delete operator</title>
  </svg>`;

export const addPortButtonPath = `
<path d="M215.037,36.846c-49.129-49.128-129.063-49.128-178.191,0c-49.127,49.127-49.127,129.063,0,178.19
c24.564,24.564,56.83,36.846,89.096,36.846s64.531-12.282,89.096-36.846C264.164,165.909,264.164,85.973,215.037,36.846z
 M49.574,202.309c-42.109-42.109-42.109-110.626,0-152.735c21.055-21.054,48.711-31.582,76.367-31.582s55.313,10.527,76.367,31.582
c42.109,42.109,42.109,110.626,0,152.735C160.199,244.417,91.683,244.417,49.574,202.309z"/>
<path d="M194.823,116.941h-59.882V57.059c0-4.971-4.029-9-9-9s-9,4.029-9,9v59.882H57.059c-4.971,0-9,4.029-9,9s4.029,9,9,9h59.882
v59.882c0,4.971,4.029,9,9,9s9-4.029,9-9v-59.882h59.882c4.971,0,9-4.029,9-9S199.794,116.941,194.823,116.941z"/>
`;

export const removePortButtonPath = `
<path d="M215.037,36.846c-49.129-49.128-129.063-49.128-178.191,0c-49.127,49.127-49.127,129.063,0,178.19
c24.564,24.564,56.83,36.846,89.096,36.846s64.531-12.282,89.096-36.846C264.164,165.909,264.164,85.973,215.037,36.846z
 M49.574,202.309c-42.109-42.109-42.109-110.626,0-152.735c21.055-21.054,48.711-31.582,76.367-31.582s55.313,10.527,76.367,31.582
c42.109,42.109,42.109,110.626,0,152.735C160.199,244.417,91.683,244.417,49.574,202.309z"/>
<path d="M194.823,116.941H57.059c-4.971,0-9,4.029-9,9s4.029,9,9,9h137.764c4.971,0,9-4.029,9-9S199.794,116.941,194.823,116.941z"
/>`;

export const addInputPortButtonSVG = `
  <svg class="add-input-port-button">
    <g transform="scale(0.075)">${addPortButtonPath}</g>
    <title>add port</title>
  </svg>
`;

export const removeInputPortButtonSVG = `
  <svg class="remove-input-port-button">
  <g transform="scale(0.075)">${removePortButtonPath}</g>
    <title>remove port</title>
  </svg>
`;

export const addOutputPortButtonSVG = `
  <svg class="add-output-port-button">
    <g transform="scale(0.075)">${addPortButtonPath}</g>
    <title>add port</title>
  </svg>
`;

export const removeOutputPortButtonSVG = `
  <svg class="remove-output-port-button">
    <g transform="scale(0.075)">${removePortButtonPath}</g>
    <title>remove port</title>
  </svg>
`;

/**
 * Defines the handle (the square at the end) of the source operator for a link
 */
export const sourceOperatorHandle = "M 0 0 L 0 8 L 8 8 L 8 0 z";

/**
 * Defines the handle (the arrow at the end) of the target operator for a link
 */
export const targetOperatorHandle = "M 12 0 L 0 6 L 12 12 z";

export const operatorReuseCacheTextClass = "texera-operator-result-reuse-text";
export const operatorReuseCacheIconClass = "texera-operator-result-reuse-icon";
export const operatorViewResultIconClass = "texera-operator-view-result-icon";
export const operatorStateClass = "texera-operator-state";
export const operatorProcessedCountClass = "texera-operator-processed-count";
export const operatorOutputCountClass = "texera-operator-output-count";
export const operatorAbbreviatedCountClass = "texera-operator-abbreviated-count";
export const operatorCoeditorEditingClass = "texera-operator-coeditor-editing";
export const operatorCoeditorChangedPropertyClass = "texera-operator-coeditor-changed-property";

export const operatorIconClass = "texera-operator-icon";
export const operatorNameClass = "texera-operator-name";
export const operatorFriendlyNameClass = "texera-operator-friendly-name";

export const linkPathStrokeColor = "#919191";

/**
 * Extends a basic Joint operator element and adds our own HTML markup.
 * Our own HTML markup includes the SVG element for the delete button,
 *   which will show a red delete button on the top right corner
 */
class TexeraCustomJointElement extends joint.shapes.devs.Model {
  static getMarkup(dynamicInputPorts: boolean, dynamicOutputPorts: boolean): string {
    return `
    <g class="element-node">
      <rect class="body"></rect>
      <image class="${operatorIconClass}"></image>
      <text class="${operatorFriendlyNameClass}"></text>
      <text class="${operatorNameClass}"></text>
      <text class="${operatorProcessedCountClass}"></text>
      <text class="${operatorOutputCountClass}"></text>
      <text class="${operatorAbbreviatedCountClass}"></text>
      <text class="${operatorStateClass}"></text>
      <text class="${operatorReuseCacheTextClass}"></text>
      <text class="${operatorCoeditorEditingClass}"></text>
      <text class="${operatorCoeditorChangedPropertyClass}"></text>
      <image class="${operatorViewResultIconClass}"></image>
      <image class="${operatorReuseCacheIconClass}"></image>
      <text class="${operatorCoeditorEditingClass}"></text>
      <text class="${operatorCoeditorChangedPropertyClass}"></text>
      <image class="${operatorViewResultIconClass}"></image>
      <rect class="boundary"></rect>
      <path class="left-boundary"></path>
      <path class="right-boundary"></path>
      ${deleteButtonSVG}
      ${dynamicInputPorts ? addInputPortButtonSVG : ""}
      ${dynamicInputPorts ? removeInputPortButtonSVG : ""}
      ${dynamicOutputPorts ? addOutputPortButtonSVG : ""}
      ${dynamicOutputPorts ? removeOutputPortButtonSVG : ""}
    </g>
    `;
  }
}

class TexeraCustomCommentElement extends joint.shapes.devs.Model {
  markup = `<g class = "element-node">
  <rect class = "body"></rect>
  ${deleteButtonSVG}
  <image></image>
  </g>`;
}
/**
 * JointUIService controls the shape of an operator and a link
 *  when they are displayed by JointJS.
 *
 * This service alters the basic JointJS element by:
 *  - setting the ID of the JointJS element to be the same as Texera's OperatorID
 *  - changing the look of the operator box (size, colors, lines, etc..)
 *  - adding input and output ports to the box based on the operator metadata
 *  - changing the SVG element and CSS styles of operators, links, ports, etc..
 *  - adding a new delete button and the callback function of the delete button,
 *      (original JointJS element doesn't have a built-in delete button)
 *
 * @author Henry Chen
 * @author Zuozhi Wang
 */
@Injectable({
  providedIn: "root",
})
export class JointUIService {
  public static readonly DEFAULT_OPERATOR_WIDTH = 60;
  public static readonly DEFAULT_OPERATOR_HEIGHT = 60;
  public static readonly DEFAULT_GROUP_MARGIN = 50;
  public static readonly DEFAULT_GROUP_MARGIN_BOTTOM = 40;
  public static readonly DEFAULT_COMMENT_WIDTH = 32;
  public static readonly DEFAULT_COMMENT_HEIGHT = 32;

  private operatorSchemas: ReadonlyArray<OperatorSchema> = [];

  constructor(private operatorMetadataService: OperatorMetadataService) {
    // initialize the operator information
    // subscribe to operator metadata observable
    this.operatorMetadataService.getOperatorMetadata().subscribe(value => (this.operatorSchemas = value.operators));
  }

  /**
   * Gets the JointJS UI Element object based on the operator predicate.
   * A JointJS Element could be added to the JointJS graph to let JointJS display the operator accordingly.
   *
   * The function checks if the operatorType exists in the metadata,
   *  if it doesn't, the program will throw an error.
   *
   * The function returns an element that has our custom style,
   *  which are specified in getCustomOperatorStyleAttrs() and getCustomPortStyleAttrs()
   *
   *
   * @param operator OperatorPredicate, the type of the operator
   * @param point Point, the top-left-originated position of the operator element (relative to JointJS paper, not absolute position)
   *
   * @returns JointJS Element
   */

  public getJointOperatorElement(operator: OperatorPredicate, point: Point): joint.dia.Element {
    // check if the operatorType exists in the operator metadata
    const operatorSchema = this.operatorSchemas.find(op => op.operatorType === operator.operatorType);
    if (operatorSchema === undefined) {
      throw new Error(`operator type ${operator.operatorType} doesn't exist`);
    }

    // construct a custom Texera JointJS operator element
    //   and customize the styles of the operator box and ports
    const operatorElement = new TexeraCustomJointElement({
      position: point,
      size: {
        width: JointUIService.DEFAULT_OPERATOR_WIDTH,
        height: JointUIService.DEFAULT_OPERATOR_HEIGHT,
      },
      attrs: JointUIService.getCustomOperatorStyleAttrs(
        operator,
        operator.customDisplayName ?? operatorSchema.additionalMetadata.userFriendlyName,
        operatorSchema.operatorType,
        operatorSchema.additionalMetadata.userFriendlyName
      ),
      ports: {
        groups: {
          in: { attrs: JointUIService.getCustomPortStyleAttrs() },
          out: { attrs: JointUIService.getCustomPortStyleAttrs() },
        },
      },
      markup: TexeraCustomJointElement.getMarkup(
        operator.dynamicInputPorts ?? false,
        operator.dynamicOutputPorts ?? false
      ),
    });

    // set operator element ID to be operator ID
    operatorElement.set("id", operator.operatorID);

    // set the input ports and output ports based on operator predicate
    operator.inputPorts.forEach(port =>
      operatorElement.addPort({
        group: "in",
        id: port.portID,
        attrs: {
          ".port-label": {
            text: port.displayName ?? "",
            event: "input-port-label:pointerdown",
          },
        },
      })
    );
    operator.outputPorts.forEach(port =>
      operatorElement.addPort({
        group: "out",
        id: port.portID,
        attrs: {
          ".port-label": {
            text: port.displayName ?? "",
            event: "output-port-label:pointerdown",
          },
        },
      })
    );

    return operatorElement;
  }

  public changeOperatorStatistics(
    jointPaper: joint.dia.Paper,
    operatorID: string,
    statistics: OperatorStatistics | undefined,
    isSource: boolean,
    isSink: boolean
  ): void {
    if (!statistics) {
      jointPaper.getModelById(operatorID).attr({
        [`.${operatorProcessedCountClass}`]: { text: "" },
        [`.${operatorOutputCountClass}`]: { text: "" },
        [`.${operatorAbbreviatedCountClass}`]: { text: "" },
      });
      this.changeOperatorState(jointPaper, operatorID, OperatorState.Uninitialized);
      return;
    }

    this.changeOperatorState(jointPaper, operatorID, statistics.operatorState);

    const processedText = isSource ? "" : "Processed: " + statistics.aggregatedInputRowCount.toLocaleString();
    const outputText = isSink ? "" : "Output: " + statistics.aggregatedOutputRowCount.toLocaleString();
    const processedCountText = isSource ? "" : abbreviateNumber(statistics.aggregatedInputRowCount);
    const outputCountText = isSink ? "" : abbreviateNumber(statistics.aggregatedOutputRowCount);
    const abbreviatedText = processedCountText + (isSource || isSink ? "" : " → ") + outputCountText;
    jointPaper.getModelById(operatorID).attr({
      [`.${operatorProcessedCountClass}`]: isSink ? { text: processedText, "ref-y": -30 } : { text: processedText },
      [`.${operatorOutputCountClass}`]: { text: outputText },
      [`.${operatorAbbreviatedCountClass}`]: { text: abbreviatedText },
    });
  }
  public foldOperatorDetails(jointPaper: joint.dia.Paper, operatorID: string): void {
    jointPaper.getModelById(operatorID).attr({
      [`.${operatorAbbreviatedCountClass}`]: { visibility: "visible" },
      [`.${operatorProcessedCountClass}`]: { visibility: "hidden" },
      [`.${operatorOutputCountClass}`]: { visibility: "hidden" },
      [`.${operatorStateClass}`]: { visibility: "hidden" },
      ".delete-button": { visibility: "hidden" },
      ".add-input-port-button": { visibility: "hidden" },
      ".add-output-port-button": { visibility: "hidden" },
      ".remove-input-port-button": { visibility: "hidden" },
      ".remove-output-port-button": { visibility: "hidden" },
    });
  }

  public unfoldOperatorDetails(jointPaper: joint.dia.Paper, operatorID: string): void {
    jointPaper.getModelById(operatorID).attr({
      [`.${operatorAbbreviatedCountClass}`]: { visibility: "hidden" },
      [`.${operatorProcessedCountClass}`]: { visibility: "visible" },
      [`.${operatorOutputCountClass}`]: { visibility: "visible" },
      [`.${operatorStateClass}`]: { visibility: "visible" },
      ".delete-button": { visibility: "visible" },
      ".add-input-port-button": { visibility: "visible" },
      ".add-output-port-button": { visibility: "visible" },
      ".remove-input-port-button": { visibility: "visible" },
      ".remove-output-port-button": { visibility: "visible" },
    });
  }

  public changeOperatorState(jointPaper: joint.dia.Paper, operatorID: string, operatorState: OperatorState): void {
    let fillColor: string;
    switch (operatorState) {
      case OperatorState.Ready:
        fillColor = "#a6bd37";
        break;
      case OperatorState.Completed:
        fillColor = "green";
        break;
      case OperatorState.Pausing:
      case OperatorState.Paused:
        fillColor = "magenta";
        break;
      case OperatorState.Running:
        fillColor = "orange";
        break;
      default:
        fillColor = "gray";
        break;
    }
    jointPaper.getModelById(operatorID).attr({
      [`.${operatorStateClass}`]: { text: operatorState.toString() },
      [`.${operatorStateClass}`]: { fill: fillColor },
      "rect.body": { stroke: fillColor },
      [`.${operatorAbbreviatedCountClass}`]: { fill: fillColor },
      [`.${operatorProcessedCountClass}`]: { fill: fillColor },
      [`.${operatorOutputCountClass}`]: { fill: fillColor },
    });
  }

  /**
   * This method will change the operator's color based on the validation status
   *  valid  : default color
   *  invalid: red
   *
   * @param jointPaper
   * @param operatorID
   * @param isOperatorValid
   */
  public changeOperatorColor(jointPaper: joint.dia.Paper, operatorID: string, isOperatorValid: boolean): void {
    if (isOperatorValid) {
      jointPaper.getModelById(operatorID).attr("rect.body/stroke", "#CFCFCF");
    } else {
      jointPaper.getModelById(operatorID).attr("rect.body/stroke", "red");
    }
  }

  public changeOperatorDisableStatus(jointPaper: joint.dia.Paper, operator: OperatorPredicate): void {
    jointPaper.getModelById(operator.operatorID).attr("rect.body/fill", JointUIService.getOperatorFillColor(operator));
  }

  public changeOperatorViewResultStatus(
    jointPaper: joint.dia.Paper,
    operator: OperatorPredicate,
    viewResult?: boolean
  ): void {
    const icon = JointUIService.getOperatorViewResultIcon(operator);
    jointPaper.getModelById(operator.operatorID).attr(`.${operatorViewResultIconClass}/xlink:href`, icon);
  }

  public changeOperatorReuseCacheStatus(
    jointPaper: joint.dia.Paper,
    operator: OperatorPredicate,
    cacheStatus?: OperatorResultCacheStatus
  ): void {
    JointUIService.getOperatorCacheDisplayText(operator, cacheStatus);
    const cacheIcon = JointUIService.getOperatorCacheIcon(operator, cacheStatus);

    jointPaper.getModelById(operator.operatorID).attr(`.${operatorReuseCacheIconClass}/xlink:href`, cacheIcon);
    const icon = JointUIService.getOperatorViewResultIcon(operator);
    jointPaper.getModelById(operator.operatorID).attr(`.${operatorViewResultIconClass}/xlink:href`, icon);
  }

  public changeOperatorJointDisplayName(
    operator: OperatorPredicate,
    jointPaper: joint.dia.Paper,
    displayName: string
  ): void {
    jointPaper.getModelById(operator.operatorID).attr(`.${operatorNameClass}/text`, displayName);
  }

  public getBreakpointButton(): new () => joint.linkTools.Button {
    return joint.linkTools.Button.extend({
      name: "info-button",
      options: {
        markup: [
          {
            tagName: "circle",
            selector: "info-button",
            attributes: {
              r: 10,
              fill: "#001DFF",
              cursor: "pointer",
            },
          },
          {
            tagName: "path",
            selector: "icon",
            attributes: {
              d: "M -2 4 2 4 M 0 3 0 0 M -2 -1 1 -1 M -1 -4 1 -4",
              fill: "none",
              stroke: "#FFFFFF",
              "stroke-width": 2,
              "pointer-events": "none",
            },
          },
        ],
        distance: 60,
        offset: 0,
        action: function (event: JQuery.Event, linkView: joint.dia.LinkView) {
          // when this button is clicked, it triggers an joint paper event
          if (linkView.paper) {
            linkView.paper.trigger("tool:breakpoint", linkView, event);
          }
        },
      },
    });
  }

  public getCommentElement(commentBox: CommentBox): joint.dia.Element {
    const basic = new joint.shapes.standard.Rectangle();
    if (commentBox.commentBoxPosition) basic.position(commentBox.commentBoxPosition.x, commentBox.commentBoxPosition.y);
    else basic.position(0, 0);
    basic.resize(120, 50);
    const commentElement = new TexeraCustomCommentElement({
      position: commentBox.commentBoxPosition || { x: 0, y: 0 },
      size: {
        width: JointUIService.DEFAULT_COMMENT_WIDTH,
        height: JointUIService.DEFAULT_COMMENT_HEIGHT,
      },
      attrs: JointUIService.getCustomCommentStyleAttrs(),
    });
    commentElement.set("id", commentBox.commentBoxID);
    return commentElement;
  }
  /**
   * This function converts a Texera source and target OperatorPort to
   *   a JointJS link cell <joint.dia.Link> that could be added to the JointJS.
   *
   * @param link
   * @returns JointJS Link Cell
   */
  public static getJointLinkCell(link: OperatorLink): joint.dia.Link {
    const jointLinkCell = JointUIService.getDefaultLinkCell();
    jointLinkCell.set("source", {
      id: link.source.operatorID,
      port: link.source.portID,
    });
    jointLinkCell.set("target", {
      id: link.target.operatorID,
      port: link.target.portID,
    });
    jointLinkCell.set("id", link.linkID);
    return jointLinkCell;
  }

  /**
   * This function will creates a custom JointJS link cell using
   *  custom attributes / styles to display the operator.
   *
   * This function defines the svg properties for each part of link, such as the
   *   shape of the arrow or the link. Other styles are defined in the
   *   "app/workspace/component/workflow-editor/workflow-editor.component.scss".
   *
   * The reason for separating styles in svg and css is that while we can
   *   change the shape of the operators in svg, according to JointJS official
   *   website, https://resources.jointjs.com/tutorial/element-styling ,
   *   CSS properties have higher precedence over SVG attributes.
   *
   * As a result, a separate css/scss file is required to override the default
   * style of the operatorLink.
   *
   * @returns JointJS Link
   */
  public static getDefaultLinkCell(): joint.dia.Link {
    return new joint.dia.Link({
      router: {
        name: "manhattan",
      },
      connector: {
        name: "rounded",
      },
      toolMarkup: `<g class="link-tool">
          <g class="tool-remove" event="tool:remove">
          <circle r="11" />
            <path transform="scale(.8) translate(-16, -16)" d="M24.778,21.419 19.276,15.917 24.777
            10.415 21.949,7.585 16.447,13.087 10.945,7.585 8.117,10.415 13.618,15.917 8.116,21.419
            10.946,24.248 16.447,18.746 21.948,24.248z"/>
            <title>Remove link.</title>
           </g>
         </g>`,
      attrs: {
        ".connection": {
          stroke: linkPathStrokeColor,
          "stroke-width": "2px",
        },
        ".connection-wrap": {
          "stroke-width": "0px",
          // 'display': 'inline'
        },
        ".marker-source": {
          d: sourceOperatorHandle,
          stroke: "none",
          fill: "#919191",
        },
        ".marker-arrowhead-group-source .marker-arrowhead": {
          d: sourceOperatorHandle,
        },
        ".marker-target": {
          d: targetOperatorHandle,
          stroke: "none",
          fill: "#919191",
        },
        ".marker-arrowhead-group-target .marker-arrowhead": {
          d: targetOperatorHandle,
        },
        ".tool-remove": {
          fill: "#D8656A",
          width: 24,
          display: "none",
        },
        ".tool-remove path": {
          d: deleteButtonPath,
        },
        ".tool-remove circle": {},
      },
    });
  }

  /**
   * This function changes the default svg of the operator ports.
   * It hides the port label that will display 'out/in' beside the operators.
   *
   * @returns the custom attributes of the ports
   */
  public static getCustomPortStyleAttrs(): joint.attributes.SVGAttributes {
    return {
      ".port-body": {
        fill: "#A0A0A0",
        r: 5,
        stroke: "none",
      },
      ".port-label": {
        event: "input-label:evt",
        dblclick: "input-label:dbclick",
        pointerdblclick: "input-label:pointerdblclick",
      },
    };
  }

  /**
   * This function create a custom svg style for the operator
   * @returns the custom attributes of the tooltip.
   */
  public static getCustomOperatorStatusTooltipStyleAttrs(): joint.shapes.devs.ModelSelectors {
    return {
      "element-node": {
        style: { "pointer-events": "none" },
      },
      polygon: {
        fill: "#FFFFFF",
        "follow-scale": true,
        stroke: "purple",
        "stroke-width": "2",
        rx: "5px",
        ry: "5px",
        refPoints: "0,30 150,30 150,120 85,120 75,150 65,120 0,120",
        display: "none",
        style: { "pointer-events": "none" },
      },
      "#operatorCount": {
        fill: "#595959",
        "font-size": "12px",
        ref: "polygon",
        "y-alignment": "middle",
        "x-alignment": "left",
        "ref-x": 0.05,
        "ref-y": 0.2,
        display: "none",
        style: { "pointer-events": "none" },
      },
    };
  }

  /**
   * This function creates a custom svg style for the operator.
   * This function also makes the delete button defined above to emit the delete event that will
   *   be captured by JointJS paper using event name *element:delete*
   *
   * @param operator
   * @param operatorDisplayName the name of the operator that will display on the UI
   * @param operatorType
   * @param operatorFriendlyName
   * @returns the custom attributes of the operator
   */
  public static getCustomOperatorStyleAttrs(
    operator: OperatorPredicate,
    operatorDisplayName: string,
    operatorType: string,
    operatorFriendlyName: string
  ): joint.shapes.devs.ModelSelectors {
    return {
      ".texera-operator-coeditor-editing": {
        text: "",
        "font-size": "14px",
        "font-weight": "bold",
        visibility: "hidden",
        "ref-x": -50,
        "ref-y": 100,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "start",
      },
      ".texera-operator-coeditor-changed-property": {
        text: "",
        "font-weight": "bold",
        "font-size": "14px",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": 120,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-state": {
        text: "",
        "font-size": "14px",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": 100,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-abbreviated-count": {
        text: "",
        fill: "green",
        "font-size": "14px",
        visibility: "visible",
        "ref-x": 0.5,
        "ref-y": -30,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-processed-count": {
        text: "",
        fill: "green",
        "font-size": "14px",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": -50,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-output-count": {
        text: "",
        fill: "green",
        "font-size": "14px",
        visibility: "hidden",
        "ref-x": 0.5,
        "ref-y": -30,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      "rect.body": {
        fill: JointUIService.getOperatorFillColor(operator),
        "follow-scale": true,
        stroke: "red",
        "stroke-width": "2",
        rx: "5px",
        ry: "5px",
      },
      "rect.boundary": {
        fill: "rgba(0, 0, 0, 0)",
        width: this.DEFAULT_OPERATOR_WIDTH + 20,
        height: this.DEFAULT_OPERATOR_HEIGHT + 20,
        ref: "rect.body",
        "ref-x": -10,
        "ref-y": -10,
      },
      "path.right-boundary": {
        ref: "rect.body",
        d: "M 20 80 C 0 60 0 20 20 0",
        stroke: "rgba(0,0,0,0)",
        "stroke-width": "10",
        fill: "transparent",
        "ref-x": 70,
        "ref-y": -10,
      },
      "path.left-boundary": {
        ref: "rect.body",
        d: "M 0 80 C 20 60 20 20 0 0",
        stroke: "rgba(0,0,0,0)",
        "stroke-width": "10",
        fill: "transparent",
        "ref-x": -30,
        "ref-y": -10,
      },
      ".texera-operator-name": {
        text: operatorDisplayName,
        fill: "#595959",
        "font-size": "14px",
        "ref-x": 0.5,
        "ref-y": 80,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-friendly-name": {
        text: operatorFriendlyName,
        fill: "#888888",
        "font-size": "10px",
        "ref-x": 0.5,
        "ref-y": -12,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".delete-button": {
        x: 60,
        y: -20,
        cursor: "pointer",
        fill: "#D8656A",
        event: "element:delete",
        visibility: "hidden",
      },
      ".add-input-port-button": {
        x: -22,
        y: 40,
        cursor: "pointer",
        fill: "#565656",
        event: "element:add-input-port",
        visibility: "hidden",
      },
      ".remove-input-port-button": {
        x: -22,
        y: 60,
        cursor: "pointer",
        fill: "#565656",
        event: "element:remove-input-port",
        visibility: "hidden",
      },
      ".add-output-port-button": {
        x: 62,
        y: 40,
        cursor: "pointer",
        fill: "#565656",
        event: "element:add-output-port",
        visibility: "hidden",
      },
      ".remove-output-port-button": {
        x: 62,
        y: 60,
        cursor: "pointer",
        fill: "#565656",
        event: "element:remove-output-port",
        visibility: "hidden",
      },
      ".texera-operator-icon": {
        "xlink:href": "assets/operator_images/" + operatorType + ".png",
        width: 35,
        height: 35,
        "ref-x": 0.5,
        "ref-y": 0.5,
        ref: "rect.body",
        "x-alignment": "middle",
        "y-alignment": "middle",
      },
      ".texera-operator-result-reuse-text": {
        text: JointUIService.getOperatorCacheDisplayText(operator) === "" ? "" : "cache",
        fill: "#595959",
        "font-size": "14px",
        visible: true,
        "ref-x": 80,
        "ref-y": 60,
        ref: "rect.body",
        "y-alignment": "middle",
        "x-alignment": "middle",
      },
      ".texera-operator-result-reuse-icon": {
        "xlink:href": JointUIService.getOperatorCacheIcon(operator),
        width: 40,
        height: 40,
        "ref-x": 75,
        "ref-y": 50,
        ref: "rect.body",
        "x-alignment": "middle",
        "y-alignment": "middle",
      },
      ".texera-operator-view-result-icon": {
        "xlink:href": JointUIService.getOperatorViewResultIcon(operator),
        width: 20,
        height: 20,
        "ref-x": 75,
        "ref-y": 20,
        ref: "rect.body",
        "x-alignment": "middle",
        "y-alignment": "middle",
      },
    };
  }

  public static getOperatorFillColor(operator: OperatorPredicate): string {
    const isDisabled = operator.isDisabled ?? false;
    return isDisabled ? "#E0E0E0" : "#FFFFFF";
  }

  public static getOperatorCacheDisplayText(
    operator: OperatorPredicate,
    cacheStatus?: OperatorResultCacheStatus
  ): string {
    if (cacheStatus === undefined || !operator.markedForReuse) {
      return "";
    }
    return cacheStatus;
  }

  public static getOperatorCacheIcon(operator: OperatorPredicate, cacheStatus?: OperatorResultCacheStatus): string {
    if (!operator.markedForReuse) {
      return "";
    }
    if (cacheStatus === "cache valid") {
      return "assets/svg/operator-reuse-cache-valid.svg";
    } else {
      return "assets/svg/operator-reuse-cache-invalid.svg";
    }
  }

  public static getOperatorViewResultIcon(operator: OperatorPredicate): string {
    if (operator.viewResult) {
      return "assets/svg/operator-view-result.svg";
    } else {
      return "";
    }
  }

  public static getCustomCommentStyleAttrs(): joint.shapes.devs.ModelSelectors {
    return {
      rect: {
        fill: "#F2F4F5",
        "follow-scale": true,
        stroke: "#CED4D9",
        "stroke-width": "0",
        rx: "5px",
        ry: "5px",
      },
      image: {
        "xlink:href": "assets/operator_images/icons8-chat_bubble.png",
        width: 32,
        height: 32,
        "ref-x": 0.5,
        "ref-y": 0.5,
        ref: "rect",
        "x-alignment": "middle",
        "y-alignment": "middle",
      },
      ".delete-button": {
        x: 22,
        y: -16,
        cursor: "pointer",
        fill: "#D8656A",
        event: "element:delete",
      },
    };
  }

  public static getJointUserPointerCell(coeditor: Coeditor, position: Point, color: string): joint.dia.Element {
    const userCursor = new joint.shapes.standard.Circle({
      id: this.getJointUserPointerName(coeditor),
    });
    userCursor.resize(15, 15);
    userCursor.position(position.x, position.y);
    userCursor.attr("body/fill", color);
    userCursor.attr("body/stroke", color);
    userCursor.attr("text", {
      text: coeditor.name,
      "ref-x": 15,
      "ref-y": 20,
      stroke: coeditor.color,
    });
    return userCursor;
  }

  public static getJointUserPointerName(coeditor: Coeditor) {
    return "pointer_" + coeditor.clientId;
  }
}

export function fromJointPaperEvent<T extends keyof joint.dia.Paper.EventMap = keyof joint.dia.Paper.EventMap>(
  paper: joint.dia.Paper,
  eventName: T,
  context?: any
): Observable<Parameters<joint.dia.Paper.EventMap[T]>> {
  return fromEventPattern(
    handler => paper.on(eventName, handler, context), // addHandler
    (handler, signal) => paper.off(eventName as string, handler, context) // removeHandler
  );
}
