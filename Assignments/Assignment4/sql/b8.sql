-- Part B8 
DECLARE @var AS VARCHAR(100)
SELECT SUM(CAST(total_earnings_female as int))
FROM [dbo].[emp]
WHERE total_earnings_female!='NA' AND minor_category='Architecture and Engineering' AND year=2016 ;


--SELECT SUM(CAST(total_earnings_female as int))
--FROM @var
--WHERE major_category='Computer, Engineering, and Science' AND year=2016 


