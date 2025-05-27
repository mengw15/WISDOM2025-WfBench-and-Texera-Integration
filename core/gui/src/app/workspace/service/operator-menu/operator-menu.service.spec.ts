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

import { TestBed } from "@angular/core/testing";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";

import { OperatorMenuService } from "./operator-menu.service";
import { HttpClientModule } from "@angular/common/http";
import { ComputingUnitStatusService } from "../computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../computing-unit-status/mock-computing-unit-status.service";

describe("OperatorMenuService", () => {
  let service: OperatorMenuService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
      ],
      imports: [HttpClientModule],
    });
    service = TestBed.inject(OperatorMenuService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
