WITH
    joined_enrollments AS (
        SELECT *
        FROM
            main.national_student_clearinghouse.clean_data_2022_12_05
        UNION
        SELECT *
        FROM
            main.national_student_clearinghouse.clean_data_2021_04_16
        UNION
        SELECT *
        FROM
            main.national_student_clearinghouse.clean_data_2020_04_22
        UNION
        SELECT *
        FROM
            main.national_student_clearinghouse.clean_data_2019_04_15
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
      , previous_end_date__c
      , start_date__c
      , end_date__c
      , next_start_date__c
      , daysgap
      , date_last_verified__c
      , status__c
      , degree_type__c
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
      , previous_end_date__c
      , DATEADD(MONTH, 4.5, start_date__c) AS newstart
      , end_date__c
      , next_start_date__c
      , daysgap
      , date_last_verified__c
      , status__c
      , degree_type__c
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
      , your_unique_identifier
      , college_text__c
      , previous_end_date__c
      , start_date__c
      , DATEADD(MONTH, 4.5, start_date__c) AS end_date__c
      , next_start_date__c
      , daysgap
      , date_last_verified__c
      , status__c
      , degree_type__c
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
      , your_unique_identifier
      , college_text__c
      , previous_end_date__c
      , start_date__c
      , end_date__c
      , next_start_date__c
      , daysgap
      , date_last_verified__c
      , status__c
      , degree_type__c
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
                MAX(
                id)
              , MAX(
                date_last_verified__c) AS date_last_verified__c
              , MAX(
                your_unique_identifier) AS your_unique_identifier
              , semester_ay
              , MAX(
                college_text__c) AS college_text__c
              , MIN(
                start_date__c) AS start_date
              , MAX(
                end_date__c) AS end_date
              , MAX(
                previous_end_date__c) AS previous_end_date__c
              , MIN(
                next_start_date__c) AS next_start_date__c
              , MAX(
                status__c) AS status__c
              , MAX(
                degree_type__c) AS degree_type__c
              , MAX(
                degree_text__c) AS degree_text__c
              , MAX(
                major_text__c) AS major_text__c
              , MAX(
                index_for_debugging) AS index_for_debugging
            FROM
                rejoining_long_enrollments
            GROUP BY
                your_unique_identifier
              , semester_ay
        )
SELECT *
FROM
    semester_level_aggregation