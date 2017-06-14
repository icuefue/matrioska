select * 
  from 
        (select concat(des_padre, '/', des_objeto) as full_path, nav.* 
           from {rd_ebdmgv}.t_ebdmgv05_tekevnav nav
         UNION ALL
         select des_destino1 as full_path, des1.* 
            from {rd_ebdmgv}.t_ebdmgv05_tekevnav des1
           where des_destino1 <> 'null'
         UNION ALL
         select des_destino2 as full_path, des2.* 
            from {rd_ebdmgv}.t_ebdmgv05_tekevnav des2
           where des_destino2 <> 'null'
         UNION ALL
         select des_destino3 as full_path, des3.* 
            from {rd_ebdmgv}.t_ebdmgv05_tekevnav des3
           where des_destino3 <> 'null'
         UNION ALL
         select des_destino4 as full_path, des4.* 
            from {rd_ebdmgv}.t_ebdmgv05_tekevnav des4
           where des_destino4 <> 'null') metainformacion 
  left join
           (select diccionario.des_nomcfun, 
                   diccionario.des_confunc, 
                   diccionario.des_corfunc, 
                   concat(relaciones.cod_platdic,relaciones.xti_origen), 
                   relaciones.fec_alconcbd, 
                   diccionario.des_ambitobd,
                   concat(
                       split(relaciones.des_url_proc,'\\.')[0], 
                       '/', 
                       split(relaciones.des_url_proc,'\\.')[1]
                   ) as esquema_tabla,
                   split(relaciones.des_url_proc,'\\.')[2] as columna,
                   relaciones.aud_tim as aud_time_tekevptn,
                   diccionario.aud_tim as aud_time_tekevdin
              from {rd_ebdmgv}.t_ebdmgv01_tekevptn relaciones 
              join {rd_ebdmgv}.t_ebdmgv01_tekevdin diccionario 
                on relaciones.cod_platdic = diccionario.cod_platdic 
               and relaciones.xti_origen = diccionario.xti_origen) reladicci 
    on metainformacion.des_objeto = reladicci.columna 
   and metainformacion.des_padre = reladicci.esquema_tabla
  left join
           (select distinct 
                   concat(substr(parentpath,2),'/',originalname) as full_path,
                   '1' as error_integridad
              from {rd_ebdmgv}.v_ebdmgv_clusterintegrity_checks integrity
              join (select max(date_created) as max_date_created 
                      from {rd_ebdmgv}.v_ebdmgv_clusterintegrity_checks) max_fec
                on integrity.date_created=max_fec.max_date_created
             where errortype='ERROR_COLUMNA_SIN_DESCRIPCION_FUNCIONAL' or
                   errortype='ERROR_TABLA_CHECK_PROPERTIES' or
                   errortype='ERROR_COLUMNA_SIN_DESCRIPCION_TECNICA' or
                   errortype='ERROR_COLUMNA_SIN_AMBITO') errores
    on    metainformacion.full_path=errores.full_path
where (metainformacion.aud_tim > '$1' 
   or reladicci.aud_time_tekevptn > '$1' 
   or reladicci.aud_time_tekevdin > '$1')
   or errores.error_integridad = '1'