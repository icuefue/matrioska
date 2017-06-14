select today.* 
  from
      (select *
        from {$1}.$2
       where year=$4 and month=$5 and day=$6) today
  left join
      (select distinct nb
        from {$1}.$3
       where year=$4 and month=$5 and day=$6) api1
    on today.nb=api1.nb
 where api1.nb is null
union all
select yesterday.* 
  from
      (select $8
         from {$1}.$2
        where year=$4 and month=$5 and day=$7) yesterday
  join
      (select distinct nb
         from {$1}.$3
        where year=$4 and month=$5 and day=$6) api2
    on yesterday.nb=api2.nb