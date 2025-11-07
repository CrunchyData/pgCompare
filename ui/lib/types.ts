// Database types for pgCompare

export interface DBCredentials {
  host: string;
  port: number;
  database: string;
  schema: string;
  user: string;
  password: string;
}

export interface Project {
  pid: number;
  project_name: string;
  project_config: Record<string, any>;
}

export interface Table {
  tid: number;
  pid: number;
  table_alias: string;
  enabled: boolean;
  batch_nbr: number;
  parallel_degree: number;
}

export interface TableMap {
  tid: number;
  dest_type: string;
  schema_name: string;
  table_name: string;
  mod_column?: string;
  table_filter?: string;
  schema_preserve_case?: boolean;
  table_preserve_case?: boolean;
}

export interface TableColumn {
  column_id: number;
  tid: number;
  column_alias: string;
  enabled: boolean;
}

export interface TableColumnMap {
  tid: number;
  column_id: number;
  column_origin: string;
  column_name: string;
  data_type: string;
  data_class?: string;
  data_length?: number;
  number_precision?: number;
  number_scale?: number;
  column_nullable?: boolean;
  column_primarykey?: boolean;
  map_expression?: string;
  supported?: boolean;
  preserve_case?: boolean;
  map_type: string;
}

export interface Result {
  cid: number;
  rid?: number;
  tid?: number;
  table_name?: string;
  status?: string;
  compare_start?: Date;
  equal_cnt?: number;
  missing_source_cnt?: number;
  missing_target_cnt?: number;
  not_equal_cnt?: number;
  source_cnt?: number;
  target_cnt?: number;
  compare_end?: Date;
}

