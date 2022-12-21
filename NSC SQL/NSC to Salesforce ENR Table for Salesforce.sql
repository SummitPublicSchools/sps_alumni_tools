/* enr_update_12_21_2022 */
WITH GradStatus AS (SELECT CONCAT(YOUR_UNIQUE_IDENTIFIER, COLLEGE_CODE_BRANCH) as id2, /* salesforce wants unique student x school row  */
                           MAX(CASE
                                   WHEN GRADUATED = 'Y' THEN 'Y'
                                   WHEN GRADUATED = 'N' THEN 'N'
                                   ELSE '' END) as GRAD_correct
                    FROM MAIN.NATIONAL_STUDENT_CLEARINGHOUSE.RAW_DATA_2021_04_16
                    GROUP BY id2),
step1 as (
    SELECT
    CONCAT(YOUR_UNIQUE_IDENTIFIER, COLLEGE_CODE_BRANCH)  as id, /* salesforce wants unique student x school row  */
       /* 1.  sets up the date variable and computes the total days enrolled in any school */
    TO_DATE( HIGH_SCHOOL_GRAD_DATE, 'YYYYMMDD') AS HS_END,
    TO_DATE( ENROLLMENT_BEGIN, 'YYYYMMDD') AS begin,
    TO_DATE( ENROLLMENT_END, 'YYYYMMDD') AS end,
    /* 2. grabs the earliest start and latest end date per student x school combo */
    MIN(begin) OVER (PARTITION BY id) AS Start_Date__c,
    MAX(end) OVER (PARTITION BY id) AS End_Date__c,

    /* 3. grabs the current date of the machine */
    CURRENT_DATE as Date_Last_Verified__c,

    /* 4 determines degree type using basic regexp/contains function
       For now, these are the types represented,
       may need to add EdD, MD, PhD etc later*/
    GRADUATED,
    MAX(CASE
        WHEN GRADUATED = 'Y'
                 AND _2_year_4_year ='4-year'
                 AND LIKE(DEGREE_TITLE, 'JD%') <>1
                 AND LIKE(DEGREE_TITLE, 'MAST%') <>1
                 AND LIKE(DEGREE_TITLE, 'CERT%') <>1
             THEN 'Bachelors'
        WHEN GRADUATED ='Y' AND _2_year_4_year ='2-year'
            AND LIKE(DEGREe_TITLE, 'CERT%') <> 1 THEN   'Associates Degree'
        WHEN LIKE(DEGREE_TITLE, 'B%') <> 0 THEN 'Bachelors'
        WHEN  LIKE(DEGREE_TITLE, 'CERT%') <> 0 THEN 'Certificate'
        WHEN LIKE(DEGREE_TITLE, 'ASSO%') <> 0 THEN 'Associates Degree'
        WHEN LIKE(DEGREE_TITLE, 'MAST%') <> 0 THEN 'Masters'
        WHEN LIKE(DEGREE_TITLE, 'JD%') <> 0 THEN 'Juris Doctor (Law)'
        ELSE '' END)
        OVER (PARTITION BY id) AS Degree_Type__c,

        /* 5. change data source if that's ever necessary */
    'National Student Clearinghouse' AS Data_Source__c,

    /* 6. grabs full text of degree and major */
    MAX(DEGREE_TITLE) OVER (PARTITION BY id) AS Degree_Text__c,
    MAX(MAJOR) OVER (PARTITION BY id) AS Major_Text__c,
    MAX(COLLEGE_NAME) OVER (PARTITION BY id) AS College_Text__c,

    ENROLLMENT_STATUS,
    YOUR_UNIQUE_IDENTIFIER,
    COLLEGE_CODE_BRANCH,
    DEGREE_TITLE

    FROM MAIN.NATIONAL_STUDENT_CLEARINGHOUSE.RAW_DATA_2021_04_16
    ORDER BY id, begin, end

),
step2_unique_records AS (
    SELECT*
    FROM (
        SELECT id,
        ENROLLMENT_STATUS,
        YOUR_UNIQUE_IDENTIFIER,
        COLLEGE_CODE_BRANCH,
        DEGREE_TITLE,
        College_Text__c,
        Start_Date__c,
        End_Date__c,
        Date_Last_Verified__c,
        Data_Source__c,
        Degree_Text__c,
        Degree_Type__c,
        Major_Text__c,
        row_number() OVER (PARTITION BY id
             ORDER BY End_Date__c DESC) AS rn
        FROM step1)
    WHERE rn =1 /* this is how I deal with duplicated records,
                   It works by selecting whatever the most recent row is
                   for student*school combo */
),
step3_joining_grad_status AS (SELECT *,
        /* this is annoying, but some student/school pairs don't have a degree listed,
           though have data for GRADUATED-- So we have to manually add in degree-type*/
        CASE WHEN Degree_Type__c = '' AND
                GRAD_CORRECT = 'Y' AND
             COLLEGE_TEXT__C IN ('POST UNIVERSITY - ACCELERATED DEGREE',
             'GRAND CANYON UNIVERSITY-TRADITIONAL',
             'MARIST COLLEGE',
             'WESLEYAN UNIVERSITY',
             'UNIVERSITY OF CALIFORNIA-LOS ANGELES',
             'SOKA UNIVERSITY OF AMERICA') THEN 'Bachelors'
            WHEN Degree_Type__c = '' AND
                  GRAD_correct = 'Y' AND
             COLLEGE_TEXT__C IN ('CARRINGTON COLLEGE OF CALIFORNIA-SAN JOS',
                                 'FOOTHILL COLLEGE') THEN 'Associates'
            ELSE Degree_Type__c END AS Degree_Type__c2
             FROM step2_unique_records
             LEFT JOIN GradStatus
                ON step2_unique_records.id = GradStatus.id2
        ),
step4_date_islands AS (
     /* just handling the lead and lag dates and associated information to
        determine enrollment status
      */
    SELECT *,
    MAX(End_Date__c) OVER (PARTITION BY YOUR_UNIQUE_IDENTIFIER
            ORDER BY Start_Date__c, End_Date__c
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS Previous_End_Date__c,
    LEAD(Start_Date__c, 1) OVER (PARTITION BY YOUR_UNIQUE_IDENTIFIER
            ORDER BY Start_Date__c, End_Date__c) AS Next_Start_Date__c,
    MAX(College_Text__c) OVER (PARTITION BY YOUR_UNIQUE_IDENTIFIER
            ORDER BY Start_Date__c, End_Date__c
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS Previous_End_Name__c,
    LEAD(College_Text__c, 1) OVER (PARTITION BY YOUR_UNIQUE_IDENTIFIER
            ORDER BY Start_Date__c, End_Date__c) AS Next_Start_Name__c,
    CASE
        WHEN GRAD_correct='Y' THEN 'Graduated'
        WHEN ENROLLMENT_STATUS='W' THEN 'Withdrew'
        WHEN GRAD_correct='N' AND DATEDIFF(days, Next_Start_Date__c, End_Date__c) <131 THEN 'Transferred Within 131 days' /* option to change days that count as extended gap */
        WHEN GRAD_correct='N' AND DATEDIFF(days, Next_Start_Date__c, End_Date__c) >131 THEN 'Transferred After 131 days'
        WHEN GRAD_correct='N' AND Next_Start_Date__c IS NULL THEN 'Withdrew'
        WHEN GRAD_correct='N' AND ENROLLMENT_STATUS='F' THEN 'Enrolled Full-time'
        WHEN GRAD_correct='N' AND ENROLLMENT_STATUS='H' THEN 'Enrolled Half-time'
        WHEN GRAD_correct='N' AND ENROLLMENT_STATUS='Q' THEN 'Enrolled Three Quarter-time'
        WHEN GRAD_correct='N' AND ENROLLMENT_STATUS='L' THEN 'Enrolled Less Than Half-time'
        WHEN GRAD_correct='N' AND ENROLLMENT_STATUS='A' THEN 'On Leave'
        ELSE ''
    END AS Status__c
    FROM step3_joining_grad_status
    ),
step5_overlap_check AS (
    SELECT *,
        /* This identifies overlapping stints at the same school
           In this example, only one student had issues*/
        CASE WHEN Start_Date__c <= Previous_End_Date__C AND College_Text__c=Previous_End_Name__c THEN 'OVERLAPPING'
             WHEN End_Date__c >= Next_Start_Date__c AND College_Text__c=Next_Start_Name__c THEN 'OVERLAPPING'
        ELSE 'DISCRETE' END AS OverlapTest,
        MIN(Start_Date__c) OVER (PARTITION BY YOUR_UNIQUE_IDENTIFIER, COLLEGE_CODE_BRANCH) AS Start_Date_fin,
        MAX(End_Date__c) OVER (PARTITION BY YOUR_UNIQUE_IDENTIFIER, COLLEGE_CODE_BRANCH) AS End_Date_fin
    FROM step4_date_islands
)
SELECT id as Id,
        College_Text__c,
        Previous_End_Date__c,
        Start_Date_fin AS Start_Date__c,
        End_Date_fin AS End_Date__c,
        Next_Start_Date__c,
        Date_Last_Verified__c,
        Status__c,
        Degree_Type__c2 AS Degree_Type__c,
        Data_Source__c,
        Degree_Text__c,
        Major_Text__c,
       row_number() OVER (ORDER BY Id, Start_Date__c, End_Date__c) AS Index_For_Debugging
FROM step5_overlap_check
ORDER BY Id, Start_Date__c, End_Date__c