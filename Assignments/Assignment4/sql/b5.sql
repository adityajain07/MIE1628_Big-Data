-- Part B5
SELECT SUM(CAST(total_earnings_male as int))
FROM [dbo].[emp]
WHERE major_category='Service' AND year=2015;