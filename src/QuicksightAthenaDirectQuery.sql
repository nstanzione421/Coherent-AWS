SELECT *
    , cast(date_parse(timestamp, '%Y-%m-%d %H:%i:%s') as date) as rundate
    , cast(date_parse(timestamp, '%Y-%m-%d %H:%i:%s') as time) as runtime
FROM awsdatacatalog.testdb.update1