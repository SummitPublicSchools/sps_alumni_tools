CREATE OR REPLACE TABLE main.public.college_degrees
AS
WITH
    graduates_with_info AS (
        SELECT
            g.student_id
          , graduation_date
          , s.first_name
          , s.last_name
          , s.federal_race
          , s.gender
          , s.is_sed AS was_sed
          , s.is_ell AS was_ell
          , s.has_iep AS had_iep
          , g.site_short_name AS high_school_name
          , g.site_id AS high_school_id
          , g.graduation_year AS hs_graduation_year
        FROM
            main.public.graduates AS g
            LEFT JOIN main.public.students_historical AS s
                ON g.student_id = s.student_id
        WHERE
            s.school_date = g.graduation_date
    )
  , raw_nsc_1 AS (
    SELECT *
         , '2018/11/28' AS date_last_updated
    FROM
        main.national_student_clearinghouse.raw_data_2018_11_28
    WHERE
        graduated = 'Y'
)
  , raw_nsc_2 AS (
    SELECT *
         , '2019/11/25' AS date_last_updated
    FROM
        main.national_student_clearinghouse.raw_data_2019_11_25
    WHERE
        graduated = 'Y'
)
  , raw_nsc_3 AS (
    SELECT *
         , '2019/08/17' AS date_last_updated
    FROM
        main.national_student_clearinghouse.raw_data_2019_08_17
    WHERE
        graduated = 'Y'
)
  , raw_nsc_4 AS (
    SELECT *
         , '2019/04/15' AS date_last_updated
    FROM
        main.national_student_clearinghouse.raw_data_2019_04_15
    WHERE
        graduated = 'Y'
)
  , raw_nsc_5 AS (
    SELECT *
         , '2020/04/22' AS date_last_updated
    FROM
        main.national_student_clearinghouse.raw_data_2020_04_22
    WHERE
        graduated = 'Y'
)
  , raw_nsc_6 AS (
    SELECT *
         , '2021/04/16' AS date_last_updated
    FROM
        main.national_student_clearinghouse.raw_data_2021_04_16
    WHERE
        graduated = 'Y'
)
  , raw_nsc_7 AS (
    SELECT *
         , '2022/12/05' AS date_last_updated
    FROM
        main.national_student_clearinghouse.raw_data_2022_12_05
    WHERE
        graduated = 'Y'
)
  , joined_enrollments AS (
    SELECT *
    FROM
        raw_nsc_1
    UNION
    SELECT *
    FROM
        raw_nsc_2
    UNION
    SELECT *
    FROM
        raw_nsc_3
    UNION
    SELECT *
    FROM
        raw_nsc_4
    UNION
    SELECT *
    FROM
        raw_nsc_5
    UNION
    SELECT *
    FROM
        raw_nsc_6
    UNION
    SELECT *
    FROM
        raw_nsc_7
)
----at this point, the challenge is when students have two rows for graduation: one with degree, one without.
---I think what I can do is use some sort of logic in a grouped version of the data:  --

  , remove_duplicate_nsc_records AS (
    SELECT
        MAX(date_last_updated) AS date_last_updated
      , TRIM(your_unique_identifier, '_') AS student_id
      , MAX(college_name) AS college_name
      , MAX(college_code_branch) AS college_code_branch
      , MAX(TO_DATE(graduation_date, 'YYYYMMDD')) AS graduation_date
      , MAX(public_private) AS public_private
      , MAX(degree_title) AS degree_title
      , MAX(major) AS major
      , MAX(_2_year_4_year) AS _2_year_4_year
    FROM
        joined_enrollments
    GROUP BY
        student_id
      , college_name
      , degree_title
)
  , add_grad_row_number AS (
    SELECT *
         , ROW_NUMBER() OVER (PARTITION BY student_id, college_name ORDER BY student_id, college_name, degree_title) AS rn
    FROM
        remove_duplicate_nsc_records
)
  , degree_recode_duplicate_grad_drop AS (
    SELECT DISTINCT *
                  , CASE
                        WHEN LIKE(degree_title, 'CERT%') OR
                        LIKE(degree_title, '%CERT%') THEN 'Certificate'
                        WHEN LIKE(degree_title, 'B%') OR
                            LIKE(degree_title, 'ACHELOR OF ARTS') THEN 'Bachelors'
                        WHEN LIKE(degree_title, 'ASSO%') OR
                            LIKE(degree_title, 'AA%') THEN 'Associates'
                        WHEN LIKE(degree_title, 'MAST%')
                        OR LIKE(degree_title, 'MB%')
                        OR LIKE(degree_title, 'MS%')
                            THEN 'Masters'
                        WHEN LIKE(degree_title, 'JD%') <> 0 THEN 'Juris Doctor (Law)'
                        WHEN _2_year_4_year = '4-year'
                        AND degree_title IS NULL
                            THEN 'Bachelors'
                        WHEN _2_year_4_year = '2-year'
                        AND degree_title IS NULL
                            THEN 'Associates'
        ---doesn't currently include a number of higher ed prof degrees (md, doo, etc) or phd
                        ELSE 'Some Other Post-Secondary Education'
                    END AS degree_type
    FROM
        add_grad_row_number
    WHERE
        NOT (rn > 1 AND degree_title IS NULL AND major IS NULL)
)

  SELECT date_last_updated as as_of
,   grads.student_id as student_id
,   first_name
,   last_name
, federal_race
  , gender
  , was_sed
  , was_ell
  , had_iep
  , high_school_name
  , high_school_id
  , hs_graduation_year
  , public.GET_ACADEMIC_YEAR(nsc.graduation_date) as college_graduation_year
  , college_name
  , _2_year_4_year AS college_type
  , public_private AS college_funding
  , degree_type
  , degree_title as degree_text
  , major
  , college_code_branch as college_id
    FROM
        graduates_with_info as grads
        LEFT JOIN degree_recode_duplicate_grad_drop as nsc
            ON grads.student_id = nsc.student_id

