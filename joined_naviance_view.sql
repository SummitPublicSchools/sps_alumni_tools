CREATE OR REPLACE VIEW main.national_student_clearinghouse.naviance_data_all
AS
SELECT *
, '2015/12/01' AS date_updated /*note that the actual date typically doesn't include DAY.
Because we want to join these tables later, I just say the 1st of each month listed*/
  FROM main.national_student_clearinghouse.naviance_EVEREST_2015_12_01
  UNION ALL
  SELECT *
  ,'2016/09/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_EVEREST_2016_09
  UNION ALL
  SELECT *
  ,'2016/10/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_EVEREST_2016_10
  UNION ALL
  SELECT *
  ,'2017/04/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_EVEREST_2017_04
  UNION ALL
  SELECT *
   ,'2017/12/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_EVEREST_2017_12
  UNION ALL
  SELECT *
   ,'2018/05/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_NO_PREP_2018_05
  UNION ALL
  SELECT *
   ,'2015/12/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_PREP_2015_12_01
  UNION ALL
  SELECT *
   ,'2016/10/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_PREP_2016_10
  UNION ALL
  SELECT *
   ,'2017/04/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_PREP_2017_04
  UNION ALL
  SELECT *
    ,'2017/12/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_PREP_2017_12
  UNION ALL
  SELECT *
      ,'2015/12/01' AS date_updated

  FROM main.national_student_clearinghouse.naviance_RAINIER_2015_12_01
  UNION ALL
  SELECT *
      ,'2016/09/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_RAINIER_2016_09
  UNION ALL
  SELECT *
      ,'2017/04/01' AS date_updated

  FROM main.national_student_clearinghouse.naviance_RAINIER_2017_04
  UNION ALL
  SELECT *
      ,'2017/12/01' AS date_updated

  FROM main.national_student_clearinghouse.naviance_RAINIER_2017_12
  UNION ALL
  SELECT *
      ,'2017/12/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_SHASTA_2017_12
  UNION ALL
  SELECT *
      ,'2015/12/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_TAHOMA_2015_12_01
  UNION ALL
  SELECT *
      ,'2017/12/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_TAHOMA_2016_09
  UNION ALL
  SELECT *
        ,'2017/04/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_TAHOMA_2017_04
  UNION ALL
  SELECT *
        ,'2017/12/01' AS date_updated
  FROM main.national_student_clearinghouse.naviance_TAHOMA_2017_12
