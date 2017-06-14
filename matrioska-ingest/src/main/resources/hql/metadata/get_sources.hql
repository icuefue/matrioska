select s.dir_name as dir_name, s.check_qa as check_qa1,
m.cod_mask as cod_mask, m.mask as mask, m.check_qa as check_qa2, m.active as active, 
t.schema as schema_name, t.table as table_name, 
t.default_partitionning as default_partitionning, t.format as file_format, t.compression_codec as compression_codec,
f.filetype as file_type, f.field_delimiter as field_delimiter, f.line_delimiter as line_delimiter, f.ends_with_delimiter as ends_with_delimiter,
f.header as header, f.date_format as date_format, f.line_pattern as line_pattern, f.enclosed_by as enclosed_by, f.escaped_by as escaped_by,
mf.cod_tolerance as cod_tolerance, mf.parameters as tolerance_parameters
from {rd_ebdmgv}.t_ebdmgv10_sources s 
left join {rd_ebdmgv}.t_ebdmgv10_masks m on s.cod_source = m.cod_source
left join {rd_ebdmgv}.t_ebdmgv10_tables t on m.cod_table = t.cod_table
left join {rd_ebdmgv}.t_ebdmgv10_filetypes f on m.cod_filetype = f.cod_filetype
left join {rd_ebdmgv}.t_ebdmgv10_mask_tolerance mf on m.cod_mask = mf.cod_mask