(:JIQS: ShouldRun; Output="(0123456789ABCDEF, AABB)" :)
for $j as hexBinary in parallelize((hexBinary("0123456789abcdef"), hexBinary("AaBb"), hexBinary("aAbB"), hexBinary(())))
group by $j
order by serialize($j)
return $j
