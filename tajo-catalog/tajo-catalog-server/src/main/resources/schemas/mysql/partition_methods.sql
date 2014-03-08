-- Partition Method Table
CREATE TABLE PARTITION_METHODS (
  TABLE_ID VARCHAR(128) PRIMARY KEY,
  PARTITION_TYPE VARCHAR(10) NOT NULL,
  EXPRESSION TEXT NOT NULL,
  expression_schema VARBINARY(1024) NOT NULL,
  FOREIGN KEY (TABLE_ID) REFERENCES TABLES (TABLE_ID) ON DELETE CASCADE
)