package com.crunchydata.model;

/*
 * Copyright 2012-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DCTableColumn {
    private Integer tid;
    private String columnAlias;
    private String columnType;
    private String columnName;
    private String dataType;
    private String dataClass;
    private Integer dataLength;
    private Integer numberPrecission;
    private Integer numberScale;
    private Boolean columnNullable;
    private Boolean columnPrimaryKey;
    private String mapExpression;
    private Boolean supported;
    private Boolean preserveCase;
}
