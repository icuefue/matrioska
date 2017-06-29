CREATE OR REPLACE VIEW {md_netcash_analytics}.usuarios
(
	 fec_process COMMENT 'Fecha datos(YYYY-MM-DD)'
    ,cod_contract COMMENT 'Contrato telematico, identificado como: contrapartida (0525)'
    ,cod_cif COMMENT 'CIF'
    ,cod_tokenref COMMENT 'Referencia y usuario tokenizado'
    ,cod_reference COMMENT 'Referencia'
    ,cod_user COMMENT 'Codigo de usuario'
    ,des_user COMMENT 'Nombre y Apellidos'
    ,des_email COMMENT 'Email'
    ,des_phone COMMENT 'Movil'
    ,xti_status COMMENT 'Estado del usuario'
    ,xti_usertype COMMENT 'Tipo de usuario (Administrador o usuario)'
    ,cod_admtype COMMENT 'Tipo de administracion'
    ,xti_profile COMMENT 'Usuarios - Perfil (firmante / no firmante)'
    ,xti_profile2 COMMENT 'Usuarios - Perfil 2 (Apoderado / Auditor / Firmante(solo firma host))'
    ,qty_tokens COMMENT 'Tokens - Numero'
    ,xty_tokentype1 COMMENT 'Token1 - Tipo'
    ,cod_tokenstatus1 COMMENT 'Token1 - Estado'
    ,xty_tokentype2 COMMENT 'Token2 - Tipo'
    ,cod_tokenstatus2 COMMENT 'Token2 - Estado'
    ,xti_signaturetype COMMENT 'Tipo de firma (Clave / Formula)'
    ,fec_lastaccess COMMENT 'Fecha ultimo acceso(YYYY-MM-DD)'
    ,des_username COMMENT 'Usuario Contrato Telematicos (Cifrado)'
    ,year COMMENT 'Campo particion YYYYY que indica la fecha de ingesta del fichero correspondiente al anio'
    ,month COMMENT 'Campo particion MM que indica la fecha de ingesta del fichero correspondiente al mes'
    ,day COMMENT 'Campo particion DD que indica la fecha de ingesta del fichero correspondiente al dia'
)
COMMENT 'AdmUsuarios - RefExterna'
AS SELECT
fec_process
,cod_contract
,cod_cif
,cod_tokenref
,cod_reference
,cod_user
,des_user
,des_email
,des_phone
,xti_status
,xti_usertype
,cod_admtype
,xti_profile
,xti_profile2
,cast(qty_tokens as decimal) as qty_tokens
,xty_tokentype1
,cod_tokenstatus1
,xty_tokentype2
,cod_tokenstatus2
,xti_signaturetype
,fec_lastaccess
,des_username
,year
,month
,day
FROM {rd_netcash}.t_administracion_usuarios
--    WHERE year=$1 and month=$2 and day=$3