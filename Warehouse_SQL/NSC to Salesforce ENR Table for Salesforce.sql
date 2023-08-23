SET date_updated = '2021/04/16';
SET enrollment_gap=131;
---SET BA_schools_without_degrees = ;
---SET Assoc_schools_without_degrees = () ;
/* Creates a table with dates converted and generates column for previous date of enrollment.
   Notice that there are no start/end dates for rows with graduation details. To get around this, I replace the
   begin/end date to the graduation date for the rows with grad details. */

WITH
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
            main.national_student_clearinghouse.raw_data_2021_04_16
    )
  ,
/*step 2 creates island ids so we can roll-up to the right identifier. */
    step2_islandid AS (
        SELECT *
             , LEAD(begin, 1) OVER (PARTITION BY your_unique_identifier ORDER BY begin, end) AS next_start_date__c
             , IFF(DATEADD(DAY, $enrollment_gap, previous_end_date__c) >= begin,
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
  ,
/* step 3 does a bunch:
   - pares down the table to fewer columns
   - generates standardized degree variable,
   - generates a few other useful vars */
    step3 AS (
        SELECT
            --1. generates the row-level identifier
            CONCAT(your_unique_identifier, college_code_branch, islandid) AS id
            -- 2.  sets up the date variable and computes the total days enrolled in any school */
          , TO_DATE(high_school_grad_date, 'YYYYMMDD') AS hs_end
            -- 3. grabs the earliest start and latest end date per student x school combo */
          , MIN(begin) OVER (PARTITION BY id) AS start_date__c
          , MAX(end) OVER (PARTITION BY id) AS end_date__c
            -- 4. grabs the current date of the machine */
          , $date_updated AS date_last_verified__c
          ,
            /* 5 determines degree type using basic regexp/contains function
            For now, these are the types represented, may need to add EdD, MD, PhD etc later*/
            graduated
          , MAX(
            CASE
                WHEN graduated = 'Y'
                AND _2_year_4_year = '4-year'
                AND NOT LIKE(degree_title, 'JD%')
                AND NOT LIKE(degree_title, 'MB%')
                AND NOT LIKE(degree_title, 'MS%')
                AND NOT LIKE(degree_title, 'MAST%')
                AND NOT LIKE(degree_title, 'CERT%')
                AND NOT LIKE(degree_title, 'ASSO%')
                    THEN 'Bachelors'
                WHEN graduated = 'Y'
                AND _2_year_4_year = '2-year'
                AND NOT LIKE(degree_title, 'CERT%')
                    THEN 'Associates'
                WHEN LIKE(degree_title, 'B%') THEN 'Bachelors'
                WHEN LIKE(degree_title, 'CERT%') THEN 'Certificate'
                WHEN LIKE(degree_title, 'ASSO%') THEN 'Associates'
                WHEN LIKE(degree_title, 'MAST%')
                OR LIKE(degree_title, 'MB%')
                OR LIKE(degree_title, 'MS%')
                    THEN 'Masters'
                WHEN LIKE(degree_title, 'JD%') <> 0 THEN 'Juris Doctor (Law)'
                ELSE ''
            END)
            OVER (PARTITION BY id) AS degree_type__c
          ,
            /* 5. change data source if that's ever necessary */
            'National Student Clearinghouse' AS data_source__c
          ,
            /* 6. grabs full text of degree and major */
            MAX(degree_title) OVER (PARTITION BY id) AS degree_text__c
          , MAX(major) OVER (PARTITION BY id) AS major_text__c
          , MAX(college_name) OVER (PARTITION BY id) AS college_text__c
          , enrollment_status
          , your_unique_identifier
          , college_code_branch
          , degree_title
        FROM
            step2_islandid
        ORDER BY
            id
          , begin
          , end
    )
  ,
/*step 4 drops extra rows and rolls info up to select the most recent row for enrollment-bout*student*school*/
    step4_unique_records AS (
        SELECT*
        FROM
            (
                SELECT
                    id
                  , enrollment_status
                  , your_unique_identifier
                  , college_code_branch
                  , degree_title
                  , college_text__c
                  , start_date__c
                  , end_date__c
                  , date_last_verified__c
                  , data_source__c
                  , degree_text__c
                  , degree_type__c
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
             ,
            /* this is annoying, but some student/school pairs don't have a degree listed
               though have data for graduated-- So we have to manually add in degree-type*/
            CASE
                WHEN degree_type__c = ''
                AND grad_correct = 'Y'
                AND college_text__c IN ('POST UNIVERSITY - ACCELERATED DEGREE',
                                        'GRAND CANYON UNIVERSITY-TRADITIONAL', 'MARIST COLLEGE',
                                        'WESLEYAN UNIVERSITY', 'UNIVERSITY OF CALIFORNIA-LOS ANGELES',
                                        'SOKA UNIVERSITY OF AMERICA')
                    THEN 'Bachelors'
                WHEN degree_type__c = '' AND
                    grad_correct = 'Y' AND
                    college_text__c IN ('CARRINGTON COLLEGE OF CALIFORNIA-SAN JOS', 'FOOTHILL COLLEGE')
                    THEN 'Associates'
                ELSE degree_type__c
            END AS degree_type__c2
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
              , enrollment_status
              , your_unique_identifier
              , college_text__c
              , start_date__c
              , end_date__c
              , previousstart
              , previousend
              , parent
              , date_last_verified__c
              , degree_type__c2 AS degree_type__c
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
              , a.enrollment_status
              , a.your_unique_identifier
              , a.college_text__c
              , a.start_date__c
              , a.end_date__c
              , a.previousstart
              , a.previousend
              , f.parent
              , a.date_last_verified__c
              , a.degree_type__c
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
      , MAX(enrollment_status) AS enrollment_status
      , MAX(grad_correct) AS grad_correct
      , MAX(college_text__c) AS college_text__c
      , MAX(degree_type__c) AS degree_type__c
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
               WHEN enrollment_status = 'W' THEN 'Withdrew'
               WHEN grad_correct = 'N' AND daysgap < $enrollment_gap AND next_start_name__c NOT LIKE college_text__c
                   THEN 'Transferred Within 131 days' /* option to change days that count as extended gap */
               WHEN grad_correct = 'N' AND daysgap > $enrollment_gap THEN 'Transferred After 131 days'
               WHEN grad_correct = 'N' AND next_start_date__c IS NULL THEN 'Withdrew'
               WHEN grad_correct = 'N' AND enrollment_status = 'F' THEN 'Enrolled Full-time'
               WHEN grad_correct = 'N' AND enrollment_status = 'H' THEN 'Enrolled Half-time'
               WHEN grad_correct = 'N' AND enrollment_status = 'Q' THEN 'Enrolled Three Quarter-time'
               WHEN grad_correct = 'N' AND enrollment_status = 'L' THEN 'Enrolled Less Than Half-time'
               WHEN grad_correct = 'N' AND enrollment_status = 'A' THEN 'On Leave'
               ELSE 'Enrolled (No Detail)'
           END AS status__c
    FROM
        step10_overlapfinish
)
SELECT
    id AS id
  , your_unique_identifier
  , college_text__c
  , previous_end_date__c
  , start_date__c AS start_date__c
  , end_date__c AS end_date__c
  , next_start_date__c
  , daysgap
  , date_last_verified__c
  , status__c
  , degree_type__c
  , data_source__c
  , degree_text__c
  , major_text__c
  , ROW_NUMBER() OVER (ORDER BY id, start_date__c, end_date__c) AS index_for_debugging
FROM
    step11_enrollmentstatus
ORDER BY
    your_unique_identifier
  , start_date__c
