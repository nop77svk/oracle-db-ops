drop index my_log_date;

alter table my_log
modify
partition by range (message_date)
    interval (interval '1' day)
(
    partition pt_init values less than (date'2022-01-01')
)
online;
