/* enr_update_12_21_2022 */
/* lead and lag for some students gets messed up... these two should be part of the same island I think. s
10853_004480-001	DE ANZA COLLEGE	6/30/2017	4/9/2018	9/26/2018
10853_004480-002	DE ANZA COLLEGE	4/24/2018	4/9/2018	9/26/2018
*/
--set the definition of continuous enrollment by setting the max number of days spanning bouts of enrollment.
SET enrollment_gap = 131;
---SET BA_schools_without_degrees = ;
---SET Assoc_schools_without_degrees = () ;

WITH
/* Creates a table with dates converted and generates column for previous date of enrollment.
   Notice that there are no start/end dates for rows with graduation details. To get around this, I replace the
   begin/end date to the graduation date for the rows with grad details. */
step1_datetransforms AS (
    SELECT *,
        CASE WHEN graduated = 'Y' THEN TO_DATE(graduation_date, 'YYYYMMDD')
            WHEN graduated = 'N' THEN TO_DATE(enrollment_begin, 'YYYYMMDD') ELSE ''
        END                                                   AS begin,
        CASE WHEN graduated = 'Y' THEN TO_DATE(graduation_date, 'YYYYMMDD')
            WHEN graduated = 'N' THEN TO_DATE(enrollment_end, 'YYYYMMDD') ELSE ''
        END                                                   AS end,
        ROW_NUMBER() OVER (ORDER BY begin,end)                    AS rn,
        MAX(end) OVER (PARTITION BY your_unique_identifier, college_name
            ORDER BY begin, end ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
        AS Previous_End_Date__c
    FROM MAIN.NATIONAL_STUDENT_CLEARINGHOUSE.RAW_DATA_2021_04_16),
/*step 2 creates island ids so we can roll-up to the right identifier. */
step2_islandid AS
    (SELECT *,
        LEAD(begin, 1) OVER (PARTITION BY your_unique_identifier ORDER BY begin, end)
        AS Next_Start_Date__c,
        CASE WHEN DATEADD(day, $enrollment_gap, Previous_End_Date__c) >= begin THEN 0 ELSE 1 END AS islandstart,
        SUM(CASE WHEN DATEADD(day, $enrollment_gap, Previous_End_Date__c) >= begin THEN 0 ELSE 1 END)
            OVER (PARTITION BY your_unique_identifier, college_name ORDER BY rn)                AS islandid
    FROM step1_datetransforms),
-- generates grad status over islands.
GradStatus AS
    (SELECT
        CONCAT(your_unique_identifier, college_code_branch, islandid) as id2, /* row-level id */
        MAX(CASE WHEN graduated = 'Y' THEN 'Y'
                WHEN graduated = 'N' THEN 'N'
        ELSE '' END)                                          AS grad_correct
    FROM step2_islandid
        GROUP BY id2),
/* step 3 does a bunch:
   - pares down the table to fewer columns
   - generates standardized degree variable,
   - generates a few other useful vars */
step3 AS (
    SELECT
        --1. generates the row-level identifier
        CONCAT(your_unique_identifier, college_code_branch, islandid) as id,
        -- 2.  sets up the date variable and computes the total days enrolled in any school */
        TO_DATE(high_school_grad_date, 'YYYYMMDD')                    AS hs_end,
        -- 3. grabs the earliest start and latest end date per student x school combo */
        MIN(begin) OVER (PARTITION BY id)                             AS start_date__c,
        MAX(end) OVER (PARTITION BY id)                               AS End_Date__c,
        -- 4. grabs the current date of the machine */
        CURRENT_DATE                                                  as Date_Last_Verified__c,
        /* 5 determines degree type using basic regexp/contains function
        For now, these are the types represented, may need to add EdD, MD, PhD etc later*/
        graduated,
        MAX(
            CASE WHEN graduated = 'Y'
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
            ELSE '' END)
        OVER (PARTITION BY id)                                    AS Degree_Type__c,
        /* 5. change data source if that's ever necessary */
        'National Student Clearinghouse'                              AS Data_Source__c,
        /* 6. grabs full text of degree and major */
        MAX(degree_title) OVER (PARTITION BY id)                      AS Degree_Text__c,
        MAX(major) OVER (PARTITION BY id)                             AS major_Text__c,
        MAX(college_name) OVER (PARTITION BY id)                      AS College_Text__c,
        enrollment_status,
        your_unique_identifier,
        college_code_branch,
        degree_title
    FROM step2_islandid
    ORDER BY id, begin, end),
/*step 4 drops extra rows and rolls info up to select the most recent row for enrollment-bout*student*school*/
step4_unique_records AS (
    SELECT*
    FROM (SELECT
              id,
              enrollment_status,
              your_unique_identifier,
              college_code_branch,
              degree_title,
              College_Text__c,
              Start_Date__c,
              End_Date__c,
              Date_Last_Verified__c,
              Data_Source__c,
              Degree_Text__c,
              Degree_Type__c,
              major_Text__c,
              row_number() OVER (PARTITION BY id
              ORDER BY End_Date__c DESC) AS rn
        FROM step3)
        WHERE rn = 1),
step5_joining_grad_status AS (
    SELECT *,
        /* this is annoying, but some student/school pairs don't have a degree listed
           though have data for graduated-- So we have to manually add in degree-type*/
          CASE WHEN Degree_Type__c = ''
                AND grad_correct = 'Y'
                AND COLLEGE_TEXT__C IN ('POST UNIVERSITY - ACCELERATED DEGREE',
                  'GRAND CANYON UNIVERSITY-TRADITIONAL', 'MARIST COLLEGE',
                  'WESLEYAN UNIVERSITY', 'UNIVERSITY OF CALIFORNIA-LOS ANGELES',
                  'SOKA UNIVERSITY OF AMERICA')
              THEN 'Bachelors'
              WHEN Degree_Type__c = '' AND
                grad_correct = 'Y' AND
                COLLEGE_TEXT__C IN ('CARRINGTON COLLEGE OF CALIFORNIA-SAN JOS', 'FOOTHILL COLLEGE')
              THEN 'Associates'
          ELSE Degree_Type__c END AS Degree_Type__c2
    FROM step4_unique_records
    LEFT JOIN GradStatus
        ON step4_unique_records.id = GradStatus.id2),
/* just handling the lead and lag dates and associated information to
    determine enrollment status         */
step6_date_islands AS (
    SELECT *,
        MAX(End_Date__c) OVER (PARTITION BY your_unique_identifier
            ORDER BY Start_Date__c, End_Date__c
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS Previous_End_Date__c,
        LEAD(Start_Date__c, 1) OVER (PARTITION BY your_unique_identifier
        ORDER BY Start_Date__c, End_Date__c)              AS Next_Start_Date__c,
        MAX(College_Text__c) OVER (PARTITION BY your_unique_identifier
        ORDER BY Start_Date__c, End_Date__c ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS Previous_End_Name__c,
        LEAD(College_Text__c, 1) OVER (PARTITION BY your_unique_identifier
        ORDER BY Start_Date__c, End_Date__c) AS Next_Start_Name__c,
        CASE WHEN grad_correct = 'Y' THEN 'graduated'
             WHEN enrollment_status = 'W' THEN 'Withdrew'
             WHEN grad_correct = 'N'
                AND DATEDIFF(days, Next_Start_Date__c, End_Date__c) < $enrollment_gap
             THEN 'Transferred Within 131 days' /* option to change days that count as extended gap */
             WHEN grad_correct = 'N' AND
                  DATEDIFF(days, Next_Start_Date__c, End_Date__c) > $enrollment_gap
             THEN 'Transferred After 131 days'
             WHEN grad_correct = 'N' AND Next_Start_Date__c IS NULL THEN 'Withdrew'
             WHEN grad_correct = 'N' AND enrollment_status = 'F' THEN 'Enrolled Full-time'
             WHEN grad_correct = 'N' AND enrollment_status = 'H' THEN 'Enrolled Half-time'
             WHEN grad_correct = 'N' AND enrollment_status = 'Q' THEN 'Enrolled Three Quarter-time'
             WHEN grad_correct = 'N' AND enrollment_status = 'L' THEN 'Enrolled Less Than Half-time'
             WHEN grad_correct = 'N' AND enrollment_status = 'A' THEN 'On Leave'
        ELSE '' END                                               AS Status__c
    FROM step5_joining_grad_status),
step7_overlap_check AS (
    SELECT *,
    /* This identifies overlapping stints at the same school
    In this example, only one student had issues*/
        CASE WHEN Start_Date__c <= Previous_End_Date__C
                AND College_Text__c = Previous_End_Name__c
            THEN 'OVERLAPPING'
            WHEN End_Date__c >= Next_Start_Date__c
                AND College_Text__c = Next_Start_Name__c
            THEN 'OVERLAPPING' ELSE 'DISCRETE' END                                             AS OverlapTest,
        MIN(Start_Date__c) OVER (PARTITION BY id) AS Start_Date_fin,
        MAX(End_Date__c) OVER (PARTITION BY id) AS End_Date_fin
    FROM step6_date_islands)
SELECT
    id                                                          AS Id,
    College_Text__c,
    Previous_End_Date__c,
    Start_Date_fin                                              AS Start_Date__c,
    End_Date_fin                                                AS End_Date__c,
    Next_Start_Date__c,
    Date_Last_Verified__c,
    Status__c,
    Degree_Type__c2                                             AS Degree_Type__c,
    Data_Source__c,
    Degree_Text__c,
    major_Text__c,
    row_number() OVER (ORDER BY Id, Start_Date__c, End_Date__c) AS Index_For_Debugging
FROM step7_overlap_check
ORDER BY Id, Start_Date__c, End_Date__c