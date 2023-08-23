/*
 * See this gitbook for how to use this query: https://app.gitbook.com/o/-LxjWyeokAsuZLyAr6q3/s/-MNUJzWQIebv9CNouxFt/
 */


SET max_year = 2022;

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
    ON graduates.site_id = sites.site_id
    AND graduates.graduation_year = sites.academic_year
WHERE graduation_year >= $max_year - 7