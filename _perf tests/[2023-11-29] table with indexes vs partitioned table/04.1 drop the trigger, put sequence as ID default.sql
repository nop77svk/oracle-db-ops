alter table my_log modify id default my_log_seq.nextval;

drop trigger my_log_TIBS;
