ALTER TABLE {md_gsds}.t_pri_bonus DROP IF EXISTS PARTITION (fechacarga=$1) PURGE