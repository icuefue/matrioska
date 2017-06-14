CREATE OR REPLACE VIEW {MD_EVA}.v_tenspdov
(
    cod_piclient COMMENT 'Identificador del Codigo de cliente de la visita. (Incluye Codigos de cliente, grupos, grupo sieble, contrapartidas, clientes ficticios PROS o confidenciales.) [[12540]]'
    ,des_piclient COMMENT 'Descripcion que incluye el Codigo de cliente visitado, relacionado con su cod cliente. [[5647]]'
    ,des_piaccion COMMENT 'Descripcion que identifica si el cliente es grupo (Parent) o cliente (subsidiary) [[17869]]'
    ,des_industri COMMENT 'Descripcion de la industria a la que pertenece el cliente.  [[2181]]'
    ,des_pisector COMMENT 'Representa un nivel por encima de Industria (subsector). Es el sector Cib (Actividad agrupada) al que pertenece el cliente.'
    ,des_paiscl COMMENT 'Descripcion del pais del cliente.  [[12575]]'
    ,cod_pigrupo COMMENT 'Codigo identificativo del grupo al que pertenece el cliente.  [[3290]]'
    ,des_pigrupo COMMENT 'Nombre del grupo al que pertenece el cliente. [[3291]]'
    ,des_paisgrup COMMENT 'Descripcion del pais al que pertenece el cliente. [[12575]]'
    ,des_paisges COMMENT 'DESCRIPCION DEL PAIS DE GESTION DE UN GRUPO EMPRESARIAL [[17036]]'
    ,des_pisegmen COMMENT 'Descripcion del segmento al que pertenece el grupo. [[3297]]'
    ,des_pisubseg COMMENT 'Descripcion del subsegmento. Procedente de su aplicacion, no coincide con plataforma. No usado.'
    ,des_tier COMMENT 'Descripcion del nivel al que pertenece el grupo. Ej: CIB-PLATINUM, CIB-GOLD  [[17048]]'
    ,cod_mvuser COMMENT 'Codigo de usuario que modifico la visita.'
    ,des_mvuser COMMENT 'Descripcion del nombre del usuario que modifica la visita.'
    ,des_fecmvus COMMENT 'Descriptivo de la fecha en la que se modifico la visita.'
    ,cod_dvisita COMMENT 'Identificador unico del Codigo de visita del sistema origen. Puede contener el valor Confidencial.    [[17784]]'
    ,xti_visicall COMMENT 'Flag Visitas/Llamadas.  V= Visitas C= Call [[20597]]'
    ,des_catprod COMMENT 'Proporciona informacion de la categoria.  [[3]]'
    ,des_vasunto COMMENT 'Descripcion con el detalle del asunto de la visita. (Subject)  [[17788]]'
    ,cod_piorgani COMMENT 'Codiigo de usuario del organizador de la visita.'
    ,des_piorgani COMMENT 'Nombre del organizador de la entrevista.'
    ,des_coduser COMMENT 'Desripcion de los Codigos de usuario que facilita la visita.'
    ,des_asistein COMMENT 'Concatenacion de todos los asistentes internos.  = [[17790]]'
    ,des_asisteex COMMENT 'Concatenacion de todos los asistentes externos.   [[17789]]'
    ,des_desentry COMMENT 'Descriptivo de Fecha en al que registro la visita en el sistema de origen.'
    ,des_fecinic COMMENT 'Dia en la que se inicio la visita.  [[17786]]'
    ,cod_pihorain COMMENT 'Hora de inicio de la visita. [[17786]]'
    ,des_pifecfin COMMENT 'Dia en la que se finalizo  la visita.  [[17787]]'
    ,cod_pihorafi COMMENT 'Hora de fin de la visita. Ejemplo: 12:36 [[17787]]'
    ,des_pizona COMMENT 'Zona horaria en la que se produce la visita. Formato Ejemplo:  (GMT+01:00)'
    ,cod_dvispadr COMMENT 'Codigo identificativo asociado a la visita generica.  [[17784]]'
    ,cod_dvishija COMMENT 'Otras visitas asociadas. Actualemente no se informa.  [[17784]]'
    ,des_dboportu COMMENT 'Identificador tecnico de la oportunidad de negocio asociada a un grupo empresarial o subsidiaria. Suelen venir varias ooprtunidades para este campo  ej. 5412, 3844, 4545  [[17763]]'
    ,des_picoment COMMENT 'Comentario con amplia Descripcion del resumen de la visita.  [[17874]]'
    ,des_copostcv COMMENT 'Comentarios despues de la llamada o la visita. [[20598]]'
    ,des_piestado COMMENT 'Descripcion del estado de la visita, si es prevista, o si se ha obtenido o no feedback de la misma. Ejemplo: - Completada con feedback - Completada con feedback - Visita futura [[17791]]'
    ,cod_pipconfi COMMENT 'Codigo Descripcion confidencial que Yes/No que indica si la Pipeline es confidencial.'
    ,des_tipoconf COMMENT 'Descripcion que identifica si la oportunidad de negocio es confidencial o no.'
    ,des_nproycon COMMENT 'Si la oportunidad de negocio es confidencial se asigna un nombre de proyecto.'
    ,des_pipartic COMMENT 'Participantes si la oportunidad es confidencial.'
    ,cod_pubcompl COMMENT 'Indica si la publicacion a pasado el cumplimiento. Yes o no.'
    ,des_visiturl COMMENT 'Url que redirige a la pAgina de detalle de escritorio CIB de la Visita.  [[18089]]'
    ,des_regivisi COMMENT 'Informacion de la region en la que se encuentra la oportunidad en visitas. [[20941]]'
    ,des_entiofic COMMENT 'Codigo - Descripcion de oficina implicada en la visita [[20940]]'
    ,aud_usuario COMMENT 'NOMBRE DEL PROCESO DE CARGA'
    ,aud_tim COMMENT 'FECHA DE PROCESO DE CARGA DEL REGISTRO'
    ,year COMMENT 'year'
    ,month COMMENT 'month'
    ,day COMMENT 'day'

) COMMENT 'Informacion procedente del fichero de visitas de la aplicacion Desktop pipeline con informacion sobre visitas realizadas a clientes o grupos con Oportunidades de Negocio.'
AS
SELECT 
cod_piclient,des_piclient,des_piaccion,des_industri,des_pisector,des_paiscl,cod_pigrupo,des_pigrupo,
des_paisgrup,des_paisges,des_pisegmen,des_pisubseg,des_tier, cod_mvuser,des_mvuser,des_fecmvus,
cod_dvisita,xti_visicall,des_catprod,des_vasunto,cod_piorgani,des_piorgani,des_coduser,des_asistein,
des_asisteex,des_desentry,des_fecinic,cod_pihorain,des_pifecfin,cod_pihorafi,des_pizona,cod_dvispadr,
cod_dvishija,des_dboportu,des_picoment,des_copostcv,des_piestado,cod_pipconfi,des_tipoconf,des_nproycon,
des_pipartic,cod_pubcompl,des_visiturl,des_regivisi,des_entiofic,aud_usuario,aud_tim,year,month,day
FROM {rd_informacional}.tenspdov WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1