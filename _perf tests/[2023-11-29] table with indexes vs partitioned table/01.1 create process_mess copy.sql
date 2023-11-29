drop sequence my_log_SEQ;
drop table t_my_log purge;

-- Create table
create table t_my_log
(
  id               NUMBER(18) not null,
  process_log__oid NUMBER(18),
  object_type      VARCHAR2(32),
  doc__id          NUMBER(18),
  message_date     DATE default sysdate,
  mess_dict__id    NUMBER(18),
  message_type     VARCHAR2(1),
  message_name     VARCHAR2(4000),
  synch_tag        VARCHAR2(1),
  partition_key    VARCHAR2(32) default 'X'
)
tablespace users
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 64K
    minextents 1
    maxextents unlimited
    pctincrease 0
  );
-- Create/Recreate indexes 
create index my_log_DATE on t_my_log (MESSAGE_DATE, ID)
  tablespace users
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 64K
    minextents 1
    maxextents unlimited
    pctincrease 0
  );
create index my_log_DESC on t_my_log (MESS_DICT__ID, MESSAGE_DATE, ID)
  tablespace users
  pctfree 10
  initrans 6
  maxtrans 255
  storage
  (
    initial 64K
    next 64K
    minextents 1
    maxextents unlimited
    pctincrease 0
  );
create index my_log_LOG on t_my_log (PROCESS_LOG__OID, MESSAGE_DATE, ID)
  tablespace users
  pctfree 10
  initrans 6
  maxtrans 255
  storage
  (
    initial 64K
    next 64K
    minextents 1
    maxextents unlimited
    pctincrease 0
  );
create index my_log_OBJ on t_my_log (DOC__ID, OBJECT_TYPE, ID)
  tablespace users
  pctfree 10
  initrans 6
  maxtrans 255
  storage
  (
    initial 64K
    next 64K
    minextents 1
    maxextents unlimited
    pctincrease 0
  );
-- Create/Recreate primary, unique and foreign key constraints 
alter table t_my_log
  add constraint PK_my_log primary key (ID)
  using index 
  tablespace users
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 64K
    minextents 1
    maxextents unlimited
    pctincrease 0
  );

----------------------------------------------------------------------------------------------------

-- Create sequence 
create sequence my_log_SEQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 1
increment by 10
cache 20;

----------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRIGGER my_log_TIBS
  BEFORE INSERT ON t_my_log
  for each row
begin
  IF :new.ID IS NULL THEN
    SELECT my_log_SEQ.NEXTVAL INTO :new.ID FROM DUAL;
  END IF;
end;
/
