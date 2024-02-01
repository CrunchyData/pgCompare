/*
 * Copyright 2012-2023 the original author or authors.
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

package com.crunchydata.model;

public class DataCompare {
    private String tableName;
    private String pkHash;
    private String columnHash;
    private String pk;

    private String compareResult;

    private Integer threadNbr;

    private Integer batchNbr;

    public DataCompare(String tableName, String pkHash, String columnHash, String pk, String compareResult, Integer threadNbr, Integer batchNbr) {
        this.tableName = tableName;
        this.pkHash = pkHash;
        this.columnHash = columnHash;
        this.pk = pk;
        this.compareResult = compareResult;
        this.threadNbr = threadNbr;
        this.batchNbr = batchNbr;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPkHash() {
        return pkHash;
    }

    public void setPkHash(String pkHash) {
        this.pkHash = pkHash;
    }

    public String getColumnHash() {
        return columnHash;
    }

    public void setColumnHash(String columnHash) {
        this.columnHash = columnHash;
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public String getCompareResult() {
        return compareResult;
    }

    public void setCompareResult(String compareResult) {
        this.compareResult = compareResult;
    }

    public Integer getThreadNbr() {
        return threadNbr;
    }

    public void setThreadNbr(Integer threadNbr) {
        this.threadNbr = threadNbr;
    }

    public Integer getBatchNbr() {
        return batchNbr;
    }

    public void setBatchNbr(Integer batchNbr) {
        this.batchNbr = batchNbr;
    }

    @Override
    public String toString() {
        return "DataCompare{" +
                "tableName=" + tableName +
                ", pkHash=" + pkHash +
                ", columnHash='" + columnHash +
                ", pk=" + pk +
                ", compareResult=" + compareResult +
                ", threadNbr=" + threadNbr +
                '}';
    }

    public String toDelimited(String delimiter) {
        return String.join(delimiter,  tableName , pkHash , columnHash , pk , threadNbr.toString() , batchNbr.toString() ) + "\n";
    }
}
