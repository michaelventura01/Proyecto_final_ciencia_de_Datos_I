
select 
    code_module,code_presentation,id_student,gender,region,highest_education,COALESCE(imd_band,'0%'),age_band,num_of_prev_attempts,studied_credits,disability,final_result,ord_gender,ord_region,COALESCE(ord_imd_band,'0'),ord_highest_education,ord_final_result,ord_age_band 
from staging.stg_studentinfo_ord;


create table dwh.reporte_studentinfo_ord(
    id serial primary key
    ,fecha_carga date not null
    ,code_module varchar(250) not null
    ,code_presentation varchar(250) not null
    ,id_student varchar(250) not null
    ,gender varchar(250) not null
    ,region varchar(250) not null
    ,highest_education varchar(250) not null
    ,imd_band varchar(250) not null
    ,age_band varchar(250) not null
    ,num_of_prev_attempts varchar(250) not null
    ,studied_credits varchar(250) not null
    ,disability varchar(250) not null
    ,final_result varchar(250) not null
    ,ord_gender varchar(250) not null
    ,ord_region varchar(250) not null
    ,ord_imd_band varchar(250) not null
    ,ord_highest_education varchar(250) not null
    ,ord_final_result varchar(250) not null
    ,ord_age_band varchar(250) not null
)

CREATE OR REPLACE PROCEDURE dwh.sp_cargar_reporte_studentinfo_ord()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.reporte_studentinfo_ord (
        code_module, code_presentation, id_student, gender, region,
        highest_education, imd_band, age_band, num_of_prev_attempts,
        studied_credits, disability, final_result, ord_gender,
        ord_region, ord_imd_band, ord_highest_education,
        ord_final_result, ord_age_band, fecha_carga
    )
    SELECT 
        code_module, code_presentation, id_student, gender, region,
        highest_education, COALESCE(imd_band, '0%'), age_band,
        num_of_prev_attempts, studied_credits, disability,
        final_result, ord_gender, ord_region, COALESCE(ord_imd_band, '0'),
        ord_highest_education, ord_final_result, ord_age_band,
        NOW()
    FROM staging.stg_studentinfo_ord;

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging.stg_studentinfo_ord;

    -- Log the load
    CALL metadata.sp_agregar_carga('reporte_studentinfo_ord', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;

call dwh.sp_cargar_reporte_studentinfo_ord()

truncate table dwh.reporte_studentinfo_ord
select * from dwh.reporte_studentinfo_ord


select * from staging."stg_studentVle" limit 3;

create table dwh.reporte_studentVle(
    id serial primary key
    ,fecha_carga date not null
    ,code_module varchar(250) not null
    ,code_presentation varchar(250) not null
    ,id_site varchar(250) not null
    ,date varchar(250) not null
    ,sum_click varchar(250) not null
    ,id_student varchar(250) not null
    ,gender varchar(250) not null
    ,region varchar(250) not null
    ,highest_education varchar(250) not null
    ,imd_band varchar(250) not null
    ,age_band varchar(250) not null
    ,num_of_prev_attempts varchar(250) not null
    ,studied_credits varchar(250) not null
    ,disability varchar(250) not null
    ,final_result varchar(250) not null
)

CREATE OR REPLACE PROCEDURE dwh.sp_cargar_reporte_studentVle()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.reporte_studentVle (
        code_module ,code_presentation ,id_site ,date ,sum_click ,id_student ,
        gender ,region ,highest_education ,imd_band ,age_band ,num_of_prev_attempts ,
        studied_credits ,disability ,final_result,fecha_carga
    )
    SELECT 
        code_module ,code_presentation ,id_site ,date ,sum_click ,id_student ,
        gender ,region ,highest_education ,COALESCE(imd_band,'0%') ,age_band ,num_of_prev_attempts ,
        studied_credits ,disability ,final_result, NOW()
    FROM staging."stg_studentVle";

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging."stg_studentVle";

    -- Log the load
    CALL metadata.sp_agregar_carga('reporte_studentVle', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;

call dwh.sp_cargar_reporte_studentVle()

truncate table dwh.reporte_studentVle
select * from dwh.reporte_studentVle limit 100




select * from staging."stg_studentAssessment" limit 3

create table dwh.reporte_studentAssessment(
    id serial primary key
    ,fecha_carga date not null
    ,id_assessment varchar(250) not null
    ,code_presentation varchar(250) not null
    ,assessment_type varchar(250) not null
    ,date varchar(250) not null
    ,weight varchar(250) not null
    ,code_module varchar(250) not null
    ,date_submitted varchar(250) not null
    ,is_banked varchar(250) not null
    ,score varchar(250) not null
    ,id_student varchar(250) not null
    ,gender varchar(250) not null
    ,region varchar(250) not null
    ,highest_education varchar(250) not null
    ,imd_band varchar(250) not null
    ,age_band varchar(250) not null
    ,num_of_prev_attempts varchar(250) not null
    ,studied_credits varchar(250) not null
    ,disability varchar(250) not null
    ,final_result varchar(250) not null
)


CREATE OR REPLACE PROCEDURE dwh.sp_cargar_reporte_studentAssessment()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.reporte_studentAssessment (
        id_assessment ,code_presentation ,assessment_type ,date ,weight ,code_module 
        ,date_submitted ,is_banked ,score ,id_student ,gender ,region ,highest_education 
        ,imd_band ,age_band ,num_of_prev_attempts ,studied_credits ,disability ,final_result, fecha_carga )
    SELECT 
        id_assessment ,code_presentation ,assessment_type ,COALESCE(date,0) ,weight ,code_module 
        ,date_submitted ,is_banked ,COALESCE(score,'0') ,id_student ,gender ,region ,highest_education 
        ,COALESCE(imd_band,'0%') ,age_band ,num_of_prev_attempts ,studied_credits ,disability ,final_result, NOW()
    FROM staging."stg_studentAssessment";

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging."stg_studentAssessment";

    -- Log the load
    CALL metadata.sp_agregar_carga('reporte_studentAssessment', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;

call dwh.sp_cargar_reporte_studentAssessment()

truncate table dwh.reporte_studentAssessment
select * from dwh.reporte_studentAssessment limit 100




select * from staging.stg_dim_assessments limit 3

create table dwh.dim_assessments(
    id serial not null
    ,fecha_carga date not null
    ,code_module  varchar(250) not null
    ,code_presentation varchar(250) not null
    ,id_assessment  varchar(250) primary key
    ,assessment_type varchar(250) not null
    ,date varchar(250) not null
    ,weight varchar(250) not null
)

CREATE OR REPLACE PROCEDURE dwh.sp_cargar_dim_assessments()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.dim_assessments (
        code_module ,code_presentation ,id_assessment  
        ,assessment_type ,date ,weight , fecha_carga 
    )
    SELECT 
        code_module ,code_presentation ,id_assessment  
        ,assessment_type ,COALESCE(date,'0') ,weight, NOW()
    FROM staging.stg_dim_assessments;

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging.stg_dim_assessments;

    -- Log the load
    CALL metadata.sp_agregar_carga('dim_assessments', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;

call dwh.sp_cargar_dim_assessments()
truncate table dwh.dim_assessments
select * from dwh.dim_assessments


select * from staging."stg_dim_studentInfo" limit 3

create table dwh.dim_studentInfo(
    id serial not null
    ,fecha_carga date not null
    ,code_module varchar(250) not null
    ,code_presentation varchar(250) not null
    ,id_student varchar(250) primary key
    ,gender varchar(250) not null
    ,region varchar(250) not null
    ,highest_education varchar(250) not null
    ,imd_band varchar(250) not null
    ,age_band varchar(250) not null
    ,num_of_prev_attempts varchar(250) not null
    ,studied_credits varchar(250) not null
    ,disability varchar(250) not null default '0'
    ,final_result varchar(250) not null
)


CREATE OR REPLACE PROCEDURE dwh.sp_cargar_dim_studentInfo()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.dim_studentInfo (
        code_module ,code_presentation ,id_student ,gender 
        ,region ,highest_education ,imd_band ,age_band 
        ,num_of_prev_attempts ,studied_credits ,disability
        ,final_result, fecha_carga 
    )
    SELECT 
        code_module ,code_presentation ,id_student ,gender 
        ,region ,highest_education ,imd_band ,age_band 
        ,num_of_prev_attempts ,studied_credits ,disability
        ,final_result, FECHA_CARGA
    FROM (
        SELECT 
            code_module ,code_presentation ,id_student ,gender 
            ,region ,highest_education ,COALESCE(imd_band,'0%') imd_band ,age_band 
            ,num_of_prev_attempts ,studied_credits ,disability
            ,final_result, NOW() FECHA_CARGA, 
            ROW_NUMBER() OVER (
                PARTITION BY id_student
                ORDER BY code_presentation
            ) AS row_num
        FROM staging."stg_dim_studentInfo"
    ) STUDENTS
    where row_num = 1;

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging."stg_dim_studentInfo";

    -- Log the load
    CALL metadata.sp_agregar_carga('dim_studentInfo', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;

call dwh.sp_cargar_dim_studentInfo()
truncate table dwh.dim_studentInfo
select * from dwh.dim_studentInfo





select * from staging."stg_fact_studentAssessment" limit 3

create table dwh.fact_studentAssessment(
    id serial primary key
    ,fecha_carga date not null
    ,id_assessment varchar(250) not null
    ,id_student varchar(250) not null
    ,date_submitted varchar(250) not null
    ,is_banked varchar(250) not null default '0'
    ,score varchar(250) not null
    ,CONSTRAINT fk_assessment FOREIGN KEY (id_assessment) REFERENCES dwh.dim_assessments(id_assessment)
    ,CONSTRAINT fk_student FOREIGN KEY (id_student) REFERENCES dwh.dim_studentInfo(id_student)
)


CREATE OR REPLACE PROCEDURE dwh.sp_cargar_fact_studentAssessment()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.fact_studentAssessment (
        id_assessment ,id_student ,date_submitted 
        ,is_banked ,score, fecha_carga 
    )
    SELECT 
        id_assessment ,id_student ,date_submitted 
        ,is_banked ,COALESCE(score,'0'), NOW()
    FROM staging."stg_fact_studentAssessment";

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging."stg_fact_studentAssessment";

    -- Log the load
    CALL metadata.sp_agregar_carga('fact_studentAssessment', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;


call dwh.sp_cargar_fact_studentAssessment()
truncate table dwh.fact_studentAssessment
select * from dwh.fact_studentAssessment



select * from staging."stg_fact_studentVle" limit 3

create table dwh.fact_studentVle(
    id serial primary key
    ,fecha_carga date not null
    ,code_module varchar(250) not null
    ,code_presentation varchar(250) not null
    ,id_student varchar(250) not null
    ,id_site varchar(250) not null
    ,date varchar(250) not null
    ,sum_click varchar(250) not null
    ,CONSTRAINT fk_student FOREIGN KEY (id_student) REFERENCES dwh.dim_studentInfo(id_student)
)

CREATE OR REPLACE PROCEDURE dwh.sp_cargar_fact_studentVle()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.fact_studentVle (
        code_module ,code_presentation ,id_student
        ,id_site ,date ,sum_click, fecha_carga 
    )
    SELECT 
        code_module ,code_presentation ,id_student
        ,id_site ,date ,sum_click, NOW()
    FROM staging."stg_fact_studentVle";

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging."stg_fact_studentVle";

    -- Log the load
    CALL metadata.sp_agregar_carga('fact_studentVle', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;

call dwh.sp_cargar_fact_studentVle()
truncate table dwh.fact_studentVle
select * from dwh.fact_studentVle


select * from staging.stg_dim_courses limit 3


create table dwh.dim_courses(
    id serial primary key
    ,fecha_carga date not null
    ,code_module varchar(250) not null
    ,code_presentation varchar(250) not null
    ,module_presentation_length varchar(250) not null
)


CREATE OR REPLACE PROCEDURE dwh.sp_cargar_dim_courses()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.dim_courses (
        code_module ,code_presentation
        ,module_presentation_length, fecha_carga 
    )
    SELECT 
        code_module ,code_presentation
        ,module_presentation_length, NOW()
    FROM staging.stg_dim_courses;

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging.stg_dim_courses;

    -- Log the load
    CALL metadata.sp_agregar_carga('dim_courses', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;


call dwh.sp_cargar_dim_courses()
truncate table dwh.dim_courses
select * from dwh.dim_courses




select * from dwh.assess_plan 



CREATE OR REPLACE PROCEDURE dwh.sp_cargar_assess_plan()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.assess_plan (
        fecha_carga,code_module,code_presentation,guid_assess_id,assessment_type,days,weight
    )
    SELECT 
        
        NOW(), code_module,code_presentation,guid_assess_id,assessment_type,days,weight
    FROM staging.stg_assess_plan;

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging.stg_assess_plan;

    -- Log the load
    CALL metadata.sp_agregar_carga('assess_plan', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;


call dwh.sp_cargar_assess_plan()
truncate table dwh.assess_plan
select * from dwh.assess_plan



select * from dwh.assesss_detail

CREATE OR REPLACE PROCEDURE dwh.sp_cargar_assesss_detail()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.assesss_detail (
        fecha_carga,guid_student_id,guid_assess_id,date_submitted,is_banked,score,assessment_type,
       "date",weight,gender,region,highest_education,imd_band,age_band,num_of_prev_attempts,
        studied_credits,disability,final_result,status,"module",presentation,date_real_days,id_assetcode
    )
    SELECT 
        
        NOW(),guid_student_id,guid_assess_id,date_submitted,is_banked,score,assessment_type,"date",
        weight,gender,region,highest_education,imd_band,age_band,num_of_prev_attempts,studied_credits,
        disability,final_result,status,"module",presentation,date_real_days,id_assetcode
    FROM staging.stg_assesss_detail;

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging.stg_assesss_detail;

    -- Log the load
    CALL metadata.sp_agregar_carga('assesss_detail', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;


call dwh.sp_cargar_assesss_detail()
truncate table dwh.assesss_detail
select * from dwh.assesss_detail


---************************************************************

select * from dwh.cursos

CREATE OR REPLACE PROCEDURE dwh.sp_cargar_cursos()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.cursos (
        fecha_carga,code_module,code_presentation,module_presentation_length
    )
    SELECT 
        
        NOW(),code_module,code_presentation,module_presentation_length
    FROM staging.stg_cursos;

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging.stg_cursos;

    -- Log the load
    CALL metadata.sp_agregar_carga('cursos', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;


call dwh.sp_cargar_cursos()
truncate table dwh.cursos
select * from dwh.cursos

----***************************************


select * from dwh.registration

CREATE OR REPLACE PROCEDURE dwh.sp_cargar_registration()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.registration (
        fecha_carga,guid_studente_id,code_module,code_presentation,date_registration,date_unregistration
    )
    SELECT 
        
        NOW(),guid_studente_id,code_module,code_presentation,date_registration,date_unregistration
    FROM staging.stg_registration;

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging.stg_registration;

    -- Log the load
    CALL metadata.sp_agregar_carga('registration', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;


call dwh.sp_cargar_registration()
truncate table dwh.registration
select * from dwh.registration



----***************************************


select * from dwh.vle_clickstream

CREATE OR REPLACE PROCEDURE dwh.sp_cargar_vle_clickstream()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.vle_clickstream (
        fecha_carga,guid_student_id,guid_site_id,"date",sum_clics,type_assign,week_from,weel_to,
        disability,modulo,week1,week2,days,presentation
    )
    SELECT 
        
        NOW(),fecha_carga,guid_student_id,guid_site_id,"date",sum_clics,type_assign,week_from,weel_to,
        disability,modulo,week1,week2,days,presentation
    FROM staging.stg_vle_clickstream;

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging.stg_vle_clickstream;

    -- Log the load
    CALL metadata.sp_agregar_carga('vle_clickstream', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;


call dwh.sp_cargar_vle_clickstream()
truncate table dwh.vle_clickstream
select * from dwh.vle_clickstream

----*******************


select * from dwh.vle_modules

CREATE OR REPLACE PROCEDURE dwh.sp_cargar_vle_modules()
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_registros INT;
BEGIN
    -- Insert transformed data into the DWH table
    INSERT INTO dwh.vle_modules (
        fecha_carga,fecha_carga,guid_site_id,code_module,code_presentation,activity_type,week_from,week_to
    )
    SELECT 
        
        NOW(),fecha_carga,guid_site_id,code_module,code_presentation,activity_type,week_from,week_to
    FROM staging.stg_vle_modules;

    -- Count rows inserted
    SELECT COUNT(*) INTO v_total_registros FROM staging.stg_vle_modules;

    -- Log the load
    CALL metadata.sp_agregar_carga('vle_modules', 'dwh', 'COMPLETADO', v_total_registros);
END;
$$;


call dwh.sp_cargar_vle_modules()
truncate table dwh.vle_modules
select * from dwh.vle_modules