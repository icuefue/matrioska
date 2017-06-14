select
items_table.cod_item as cod_item
,items_table.cod_usecase as cod_usecase
,items_table.source as source
,items_table.source_schema as source_schema
,items_table.source_table as source_table
,items_table.target_schema as target_schema
,items_table.target_table as target_table
,items_table.target_dir as target_dir
,partition_types.name as partition_type
,delimiters_field.char_representation as field_delimiter
,delimiters_field.hex_representation as field_delimiter_hex
,delimiters_line.char_representation as line_delimiter
,delimiters_line.hex_representation as line_delimiter_hex
,items_table.format as format
,items_table.compression_codec as compression_codec
,items_table.generate_flag as generate_flag
,items_table.import_to_preraw as import_to_preraw
,items_table.enclosed_by_quotes as enclosed_by_quotes
,items_table.escaped_by_backslash as escaped_by_backslash
from {rd_ebdmgv}.t_ebdmgv11_items items_table
join {rd_ebdmgv}.t_ebdmgv11_usecases usecases on usecases.name = "$1" and usecases.cod_usecase = items_table.cod_usecase
left join {rd_ebdmgv}.t_ebdmgv11_partition_types partition_types on items_table.cod_partition_type = partition_types.cod_partition
left join {rd_ebdmgv}.t_ebdmgv11_delimiters delimiters_field on items_table.cod_field_delimiter = delimiters_field.cod_delimiter
left join {rd_ebdmgv}.t_ebdmgv11_delimiters delimiters_line on items_table.cod_line_delimiter = delimiters_line.cod_delimiter