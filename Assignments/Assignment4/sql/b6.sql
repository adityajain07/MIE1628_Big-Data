-- Part B6
SELECT SUM(CAST(workers_female as int))
FROM [dbo].[emp]
WHERE minor_category='Management' AND year=2015;