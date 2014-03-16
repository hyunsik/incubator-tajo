CREATE TABLE TABLES (
  TID int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
  DB_ID int NOT NULL REFERENCES DATABASES_ (DB_ID),
  TABLE_NAME VARCHAR(128) NOT NULL,
  TABLE_TYPE VARCHAR(128) NOT NULL,
  PATH VARCHAR(4096),
  STORE_TYPE CHAR(16),
  CONSTRAINT TABLES_PK PRIMARY KEY (TID),
  CONSTRAINT C_TABLE_ID_UNIQ UNIQUE (TABLE_NAME)
)