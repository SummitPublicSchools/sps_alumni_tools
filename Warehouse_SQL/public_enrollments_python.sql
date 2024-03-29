CREATE OR REPLACE TABLE public.college_enrollments AS
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
  , joined_enrollments AS (
    {union_sql}
    SELECT *
    FROM
        main.national_student_clearinghouse.naviance_clean_data_all
)
  , removing_duplicates AS (
    SELECT DISTINCT *
    FROM
        joined_enrollments
)
  , disaggregated_no_long_enrollments AS (
    SELECT *
         , CASE
               WHEN (MONTH(start_date__c) BETWEEN 8 AND 12
               AND MONTH(end_date__c) BETWEEN 8 AND 12) THEN 'Semester 1'
               WHEN (MONTH(start_date__c) BETWEEN 1 AND 6
               AND MONTH(end_date__c) BETWEEN 1 AND 6) THEN 'Semester 2'
               ELSE 'Summer'
           END AS semester
         , IFF(semester = 'Semester 1', CONCAT(semester, ' ', YEAR(DATEADD(YEAR, 1, start_date__c))),
               CONCAT(semester, ' ', YEAR(start_date__c))) AS semester_ay
         , IFF(semester = 'Semester 1', YEAR(DATEADD(YEAR, 1, start_date__c)),
               YEAR(start_date__c)) AS academic_year
    FROM
        removing_duplicates
    WHERE
        DATEDIFF(MONTHS, start_date__c, end_date__c) < 6
)
  , disaggregated_long_enrollments AS (
    SELECT *
    FROM
        removing_duplicates
    WHERE
        DATEDIFF(MONTHS, start_date__c, end_date__c) > 6
)
  , splitting_long_enrollments AS (
    SELECT
        id
      , your_unique_identifier
      , college_text__c
      , college_code_branch
      , public_private AS college_funding
      , _2_year_4_year AS college_type
      , previous_end_date__c
      , start_date__c
      , end_date__c
      , next_start_date__c
      , daysgap
      , date_last_verified__c
      , status__c
      , data_source__c
      , degree_text__c
      , major_text__c
      , index_for_debugging
    FROM
        disaggregated_long_enrollments
    UNION ALL
    SELECT
        id
      , your_unique_identifier
      , college_text__c
      , college_code_branch
      , college_funding
      , college_type
      , previous_end_date__c
      , DATEADD(MONTH, 4.5, start_date__c) AS newstart
      , end_date__c
      , next_start_date__c
      , daysgap
      , date_last_verified__c
      , status__c
      , data_source__c
      , degree_text__c
      , major_text__c
      , index_for_debugging
    FROM
        splitting_long_enrollments
    WHERE
        newstart < end_date__c
)
  , rejoining_long_enrollments AS (
    SELECT
        id
      , TRIM(your_unique_identifier, '_') AS your_unique_identifier
      , college_text__c
      , college_code_branch
      , college_funding
      , college_type
      , previous_end_date__c
      , start_date__c
      , DATEADD(MONTH, 4.5, start_date__c) AS end_date__c
      , next_start_date__c
      , daysgap
      , date_last_verified__c
      , status__c
      , data_source__c
      , degree_text__c
      , major_text__c
      , index_for_debugging
      , CASE
            WHEN MONTH(start_date__c) BETWEEN 8 AND 12 THEN 'Semester 1'
            WHEN MONTH(start_date__c) BETWEEN 1 AND 6 THEN 'Semester 2'
            ELSE 'Summer'
        END AS semester
      , IFF(semester = 'Semester 1', CONCAT(semester, ' ', YEAR(DATEADD(YEAR, 1, start_date__c))),
            CONCAT(semester, ' ', YEAR(start_date__c))) AS semester_ay
      , IFF(semester = 'Semester 1', YEAR(DATEADD(YEAR, 1, start_date__c)),
            YEAR(start_date__c)) AS academic_year
    FROM
        splitting_long_enrollments
    UNION ALL
    SELECT
        id
      , TRIM(your_unique_identifier, '_') AS your_unique_identifier
      , college_text__c
      , college_code_branch
      , public_private AS college_funding
      , _2_year_4_year AS college_type
      , previous_end_date__c
      , start_date__c
      , end_date__c
      , next_start_date__c
      , daysgap
      , date_last_verified__c
      , status__c
      , data_source__c
      , degree_text__c
      , major_text__c
      , index_for_debugging
      , semester
      , semester_ay
      , academic_year
    FROM
        disaggregated_no_long_enrollments
)
  , semester_level_aggregation
        AS
        (
            SELECT
                MAX(date_last_verified__c) AS date_last_verified__c
              , your_unique_identifier AS student_id
              , semester_ay
              , MAX(college_text__c) AS college_text__c
              , MAX(college_code_branch) AS college_code
              , MAX(college_type) AS college_type
              , MAX(college_funding) AS college_funding
              , MIN(start_date__c) AS start_date
              , MAX(end_date__c) AS end_date
              , MAX(previous_end_date__c) AS previous_end_date__c
              , MIN(next_start_date__c) AS next_start_date__c
              , MAX(status__c) AS status__c
              , MAX(degree_text__c) AS degree_text__c
              , MAX(major_text__c) AS major_text__c
              , MAX(academic_year) AS academic_year
              , MAX(index_for_debugging) AS index_for_debugging
            FROM
                rejoining_long_enrollments
            GROUP BY
                your_unique_identifier
              , semester_ay
        )
  , recomputed_enrollments_semester_level_step1 AS (
    SELECT *
         , ROW_NUMBER() OVER
        (PARTITION BY student_id, college_text__c ORDER BY start_date) AS rn
    FROM
        semester_level_aggregation
)
  , recomputed_enrollments_semester_level_step2 AS
        (
            SELECT *
                 , MAX
                (rn) OVER (PARTITION BY student_id, college_text__c
                ORDER BY start_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_rn
                 , IFF(rn < max_rn AND status__c
            NOT IN
                (
                 'Enrolled Full-time',
                 'Enrolled Half-time',
                 'Enrolled Three Quarter-time',
                 'Withdrew',
                 'Enrolled (No Detail)',
                 'On Leave',
                 'Enrolled Less Than Half-time'
                    ),
                       'Enrolled (No Detail)', status__c) AS nsc_enrollment_status
            FROM
                recomputed_enrollments_semester_level_step1
        )
    SELECT
    sla.date_last_verified__c AS as_of
  , g.student_id
  , g.first_name
  , g.last_name
  , g.federal_race
  , g.gender
  , was_sed
  , was_ell
  , had_iep
  , high_school_name
  , high_school_id
  , hs_graduation_year
  , sla.academic_year
  , semester_ay AS term
  , sla.nsc_enrollment_status AS nsc_enrollment_status
  , college_text__c AS college_name
  , college_type
  , college_funding AS public_private
  , college_code AS college_code
FROM
    recomputed_enrollments_semester_level_step2 AS sla
    LEFT JOIN graduates_with_info AS g
ON sla.student_id = g.student_id
ORDER BY
    student_id
  , academic_year
  , term