-- Part B9
SELECT year, 
SUM(ROUND(CAST(workers_female as int)*CAST(full_time_female as float)/100,0)) as total_full_time_female, 
SUM(ROUND(CAST(workers_male as int)*CAST(full_time_male as float)/100,0)) as total_full_time_male,
SUM(ROUND(CAST(workers_female as int)*CAST(part_time_female as float)/100,0)) as total_part_time_female, 
SUM(ROUND(CAST(workers_male as int)*CAST(part_time_male as float)/100,0)) as total_part_time_male
FROM [dbo].[emp]
WHERE workers_female!='NA' AND full_time_female!='NA' 
AND workers_male!='NA' AND full_time_male!='NA' 
AND part_time_female!='NA' AND part_time_male!='NA'
GROUP BY year;
