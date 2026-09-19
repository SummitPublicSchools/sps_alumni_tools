/*
 * Author: Howard Shen
 * Last Edit: September 18, 2026
 * Purpose: This query grabs the list of graduates to upload to the NSC FTP server.
 * See this gitbook for complete instructions: https://app.gitbook.com/o/-LxjWyeokAsuZLyAr6q3/s/-MNUJzWQIebv9CNouxFt/
 * 9/18/26 Edit: Add the code for dealing with NPS students
 */


SET max_year = 2026;

SELECT
    'PD3'
    , 'NO SSN'
    , IFNULL(students.legal_first_name, students.first_name)
    , IFNULL(students.legal_middle_name, students.middle_name)
    , IFNULL(students.legal_last_name, students.last_name)
    , NULL
    , students.last_name
    , students.first_name
    , TO_VARCHAR(students.birth_date, 'yyyymmdd')
    , students.student_id
    , 'Regular Diploma'
    , TO_VARCHAR(graduates.graduation_date, 'yyyymmdd')
    , 'N'
    , sites.site_name
    , sites.ceeb_code
    , IFF(students.gender = 'X', NULL, students.gender)
    , CASE students.federal_race
      WHEN 'American Indian or Alaska Native' THEN 'AM'
      WHEN 'Asian' THEN 'AS'
      WHEN 'Black or African American' THEN 'BL'
      WHEN 'Hispanic' THEN 'HI'
      WHEN 'Native Hawaiian or Other Pacific Islander' THEN 'PI'
      WHEN 'Two or More Races' THEN 'MU'
      WHEN 'White' THEN 'WH'
      ELSE NULL
      END
    , IFF(students.is_sed, 'Y', 'N')
    , NULL
    , NULL
    , NULL
    , NULL
    , IFF(students.is_ell, 'Y', 'N')
    , NULL
    , NULL
    , IFF(students.has_iep, 'Y', 'N')
    , NULL
    , 'ED'
FROM public.graduates
LEFT JOIN public.students_historical AS students
    ON graduates.student_id = students.student_id
    AND graduates.graduation_date = students.school_date
LEFT JOIN public.sites_historical AS sites
    ON (
        (graduates.site_id = sites.site_id AND sites.include_site) -- Do not map NPS sites onto themselves
        OR (graduates.site_id = 9999995 AND sites.site_id = 5) --Denali NPS match to Denali
        OR (graduates.site_id = 9999996 AND sites.site_id = 8) -- Tam NPS match to Tam
        OR (graduates.site_id = 9999997 AND sites.site_id = 7) -- K2 NPS match to K2
    )
    AND graduates.graduation_year = sites.academic_year
WHERE graduation_year >= $max_year - 7