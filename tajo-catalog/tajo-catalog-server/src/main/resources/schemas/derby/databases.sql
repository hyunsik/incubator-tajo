CREATE TABLE DATABASES_ (
  DB_ID int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
  DB_NAME VARCHAR(128) NOT NULL CONSTRAINT DB_NAME_UNIQ UNIQUE,
  SPACE_ID INT NOT NULL REFERENCES TABLESPACES (SPACE_ID),
  CONSTRAINT DATABASES_PK PRIMARY KEY (DB_ID)
)