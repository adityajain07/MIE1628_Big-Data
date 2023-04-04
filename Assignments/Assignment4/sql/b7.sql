-- Part B7
SELECT year, 
SUM(CAST(total_earnings_male as int)) as total_earnings_male, 
SUM(CAST(total_earnings_female as int)) as total_earnings_female
FROM [dbo].[emp]
WHERE total_earnings_male!='NA' AND total_earnings_female!='NA'
GROUP BY year;
