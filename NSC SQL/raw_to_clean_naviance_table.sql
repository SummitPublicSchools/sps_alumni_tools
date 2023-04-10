---SET 131=131; This doesn't work for the "view approach"


---SET BA_schools_without_degrees = ;
---SET Assoc_schools_without_degrees = () ;
/* Creates a table with dates converted and generates column for previous date of enrollment.
   Notice that there are no start/end dates for rows with graduation details. To get around this, I replace the
   begin/end date to the graduation date for the rows with grad details. */
create or replace table main.national_student_clearinghouse.naviance_clean_data_all /*
my naming convention is a little uninformative--could call "clean",
continuous enrollments instead*/
AS WITH
    step1_datetransforms AS (
        SELECT *
             , CASE
                   WHEN graduated = 'Y'
                       THEN TO_DATE(graduation_date, 'YYYYMMDD')
                   WHEN graduated = 'N'
                       THEN TO_DATE(enrollment_begin, 'YYYYMMDD')
                   ELSE ''
               END AS begin
             , CASE
                   WHEN graduated = 'Y'
                       THEN TO_DATE(graduation_date, 'YYYYMMDD')
                   WHEN graduated = 'N'
                       THEN TO_DATE(enrollment_end, 'YYYYMMDD')
                   ELSE ''
               END AS end
             , ROW_NUMBER() OVER (ORDER BY begin,end) AS rn
             , LAG(end)
                   OVER (PARTITION BY your_unique_identifier, college_name ORDER BY end ASC) AS previous_end_date__c ---grabs the latest end date over identifier x college partition
        FROM
          main.national_student_clearinghouse.naviance_data_all

    )
  ,
/*step 2 creates island ids so we can roll-up to the right identifier. */
    step2_islandid AS (
        SELECT *
             , LEAD(begin, 1) OVER (PARTITION BY your_unique_identifier ORDER BY begin, end) AS next_start_date__c
             , DATEADD(DAY,131, previous_end_date__c) AS gap
             , IFF(DATEDIFF(DAY, previous_end_date__c, begin) >=  131,
                   TRUE, FALSE) AS islandstart
             , SUM(islandstart::INTEGER)
                   OVER (PARTITION BY your_unique_identifier, college_name ORDER BY rn) AS islandid
        FROM
            step1_datetransforms
    ) -- generates grad status over islands.
   , gradstatus
        AS
        (
            SELECT
                CONCAT(your_unique_identifier, college_code_branch, islandid) AS id2
              , /* row-level id */
                MAX(CASE
                        WHEN graduated = 'Y'
                            THEN 'Y'
                        WHEN graduated = 'N'
                            THEN 'N'
                        ELSE ''
                    END) AS grad_correct
            FROM
                step2_islandid
            GROUP BY
                id2
        )
/* step 3 does a bunch:
   - pares down the table to fewer columns
   - generates standardized degree variable,
   - generates a few other useful vars */
 ,   step3 AS (
        SELECT
            --1. generates the row-level identifier
            CONCAT(your_unique_identifier, college_code_branch, islandid) AS id
            -- 2.  sets up the date variable */
          , TO_DATE(high_school_grad_date, 'YYYYMMDD') AS hs_end
            -- 3. grabs the earliest start and latest end date per student x school combo */
          , MIN(begin) OVER (PARTITION BY id) AS start_date__c
          , MAX(end) OVER (PARTITION BY id) AS end_date__c
            -- 4. grabs the current date of the machine */
          , date_updated AS date_last_verified__c
      ,      graduated
            /* 5. change data source if that's ever necessary */
        ,    'National Student Clearinghouse' AS data_source__c
            /* 6. grabs full text of degree and major */
          , max(degree_title) over (partition by id) AS degree_text__c
          , major AS major_text__c
          , MAX(college_name) OVER (PARTITION BY id) AS college_text__c
          , nsc_enrollment_status
          , your_unique_identifier
          , MAX(college_code_branch) OVER (PARTITION BY id) AS college_code_branch
          , MAX(_2_year_4_year) OVER (PARTITION BY id) AS _2_year_4_year
          , MAX(public_private) OVER (PARTITION BY id) AS public_private
          , degree_title
        FROM
            step2_islandid
        ORDER BY
            id
          , begin
          , end
    )
/*step 4 drops extra rows and rolls info up to select the most recent row for enrollment-bout*student*school*/
,    step4_unique_records AS (
        SELECT*
        FROM
            (
                SELECT
                    id
                  , nsc_enrollment_status
                  , your_unique_identifier
                  , college_code_branch
                  , public_private
                  , _2_year_4_year
                  , degree_title
                  , college_text__c
                  , start_date__c
                  , end_date__c
                  , date_last_verified__c
                  , data_source__c
                  , degree_text__c
                  , major_text__c
                  , ROW_NUMBER() OVER (PARTITION BY id
                    ORDER BY end_date__c DESC) AS rn
                FROM
                    step3
            )
        WHERE
            rn = 1
    )
  , step5_joining_grad_status
        AS (
        SELECT *
        FROM
            step4_unique_records
            LEFT JOIN gradstatus
                ON step4_unique_records.id = gradstatus.id2
    )
  , step6_islands_overlaps AS (
    SELECT *
         , ROW_NUMBER() OVER (ORDER BY your_unique_identifier, college_text__c, start_date__c) AS masterordering
         , LAG(start_date__c)
               OVER (PARTITION BY your_unique_identifier, college_text__c ORDER BY start_date__c ASC) AS previousstart
         , LAG(end_date__c)
               OVER (PARTITION BY your_unique_identifier, college_text__c ORDER BY start_date__c ASC) AS previousend
    FROM
        step5_joining_grad_status
)
  , step7_overlapping_dates AS (
    SELECT *
         , CASE
               WHEN previousstart IS NULL AND previousend IS NULL THEN masterordering
               WHEN previousend NOT BETWEEN start_date__c AND end_date__c THEN masterordering
               ELSE 0
           END AS parent --- if has value other than 0, it's a start date or stand-alone for some bout of enrollment
    FROM
        step6_islands_overlaps
)
  , step8_overlap_parents AS ---pulls out just records that are starts---
        (
            SELECT
                masterordering
              , id
              , grad_correct
              , nsc_enrollment_status
              , your_unique_identifier
              , college_text__c
              , college_code_branch
              , public_private
              , _2_year_4_year
              , start_date__c
              , end_date__c
              , previousstart
              , previousend
              , parent
              , date_last_verified__c
              , data_source__c
              , degree_text__c
              , major_text__c
            FROM
                step7_overlapping_dates
            WHERE
                parent > 0
        )
  , step9_union_for_overlaps AS --a recursive eof that's supposed to stack 'parent' records
---and 'child' records.
        (
            SELECT *
            FROM
                step8_overlap_parents
            UNION ALL
            SELECT
                a.masterordering
              , a.id
              , a.grad_correct
              , a.nsc_enrollment_status
              , a.your_unique_identifier
              , a.college_text__c
              , a.college_code_branch
              , a.public_private
              , a._2_year_4_year
              , a.start_date__c
              , a.end_date__c
              , a.previousstart
              , a.previousend
              , f.parent
              , a.date_last_verified__c
              , a.data_source__c
              , a.degree_text__c
              , a.major_text__c
            FROM
                step7_overlapping_dates AS a
                INNER JOIN step8_overlap_parents AS f
                    ON (a.masterordering = f.masterordering + 1
                AND a.parent = 0)
        )
  , step10_overlapfinish AS (
    SELECT
        MAX(date_last_verified__c) AS date_last_verified__c
      , your_unique_identifier
      , MAX(id) AS id
      , MAX(nsc_enrollment_status) AS nsc_enrollment_status
      , MAX(grad_correct) AS grad_correct
      , MAX(college_text__c) AS college_text__c
      , MAX(college_code_branch) AS college_code_branch
      , MAX(public_private) AS public_private
      , MAX(_2_year_4_year) AS _2_year_4_year
      , MAX(data_source__c) AS data_source__c
      , MAX(degree_text__c) AS degree_text__c
      , MAX(major_text__c) AS major_text__c
      , MIN(masterordering) AS masterordering
      , MIN(start_date__c) AS start_date__c
      , MAX(end_date__c) AS end_date__c
      , MAX(previousstart) AS previous_start_date__c
      , MIN(previousend) AS previous_end_date__c
    FROM
        step9_union_for_overlaps
    GROUP BY
        your_unique_identifier
      , parent
    ORDER BY
        masterordering
)
/* just handling the lead and lag dates and associated information to
    determine enrollment status         */
  , step11_enrollmentstatus AS (
    SELECT *
         , LEAD(start_date__c, 1)
                OVER (PARTITION BY your_unique_identifier ORDER BY start_date__c, end_date__c) AS next_start_date__c
         , LAG(college_text__c)
               OVER (PARTITION BY your_unique_identifier ORDER BY start_date__c, end_date__c) AS previous_end_name__c
         , LEAD(college_text__c, 1)
                OVER (PARTITION BY your_unique_identifier ORDER BY start_date__c, end_date__c) AS next_start_name__c
         , DATEDIFF(DAYS, end_date__c, next_start_date__c) AS daysgap
         , CASE
               WHEN grad_correct = 'Y' THEN 'Graduated'
               WHEN nsc_enrollment_status = 'W' THEN 'Withdrew'
               WHEN grad_correct = 'N' AND daysgap < 131
                   AND next_start_name__c NOT LIKE college_text__c
                   THEN 'Transferred Within 131 days' /* option to change days that count as extended gap */
               WHEN grad_correct = 'N' AND daysgap > 131
                   AND next_start_name__c NOT LIKE college_text__c
                   THEN 'Transferred After 131 days'
               WHEN grad_correct = 'N' AND nsc_enrollment_status = 'F' THEN 'Enrolled Full-time'
               WHEN grad_correct = 'N' AND nsc_enrollment_status = 'H' THEN 'Enrolled Half-time'
               WHEN grad_correct = 'N' AND nsc_enrollment_status = 'Q' THEN 'Enrolled Three Quarter-time'
               WHEN grad_correct = 'N' AND nsc_enrollment_status = 'L' THEN 'Enrolled Less Than Half-time'
               WHEN grad_correct = 'N' AND nsc_enrollment_status = 'A' THEN 'On Leave'
               ELSE 'Enrolled (No Detail)'
           END AS status__c
    FROM
        step10_overlapfinish
)
SELECT
    id AS id
  , your_unique_identifier
  , college_text__c
  , college_code_branch
  , public_private
  , _2_year_4_year
  , previous_end_date__c
  , start_date__c AS start_date__c
  , end_date__c AS end_date__c
  , next_start_date__c
  , daysgap
  , date_last_verified__c
  , status__c
  , data_source__c
  , degree_text__c
  , major_text__c
  , ROW_NUMBER() OVER (ORDER BY id, start_date__c, end_date__c) AS index_for_debugging
FROM
    step11_enrollmentstatus
ORDER BY
    your_unique_identifier
  , start_date__c
