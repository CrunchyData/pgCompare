package com.crunchydata.model;

public class ColumnInfo {
    public String columnList;
    public Integer nbrColumns;
    public Integer nbrPKColumns;
    public String oraColumn;
    public String oraPK;
    public String pgColumn;
    public String pgPK;
    public String pkList;
    public String pkJSON;
    public ColumnInfo(String columnList, Integer nbrColumns, Integer nbrPKColumns, String oraColumn, String oraPK, String pgColumn, String pgPK, String pkList, String pkJSON) {
        this.columnList = columnList;
        this.nbrColumns = nbrColumns;
        this.nbrPKColumns = nbrPKColumns;
        this.oraColumn = oraColumn;
        this.oraPK = oraPK;
        this.pgColumn = pgColumn;
        this.pgPK = pgPK;
        this.pkList = pkList;
        this.pkJSON = pkJSON;
    }
}
