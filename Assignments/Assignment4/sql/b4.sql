-- Part B4
SELECT year, SUM(CAST(workers_female as int))
FROM [dbo].[emp]
WHERE major_category='Management, Business, and Financial'
GROUP BY year;