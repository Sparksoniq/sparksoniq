(:JIQS: ShouldRun; Output="({ "$i" : 2, "$j" : 4 }, { "$i" : 3, "$j" : 4 }, { "$i" : 2, "$j" : 5 }, { "$i" : 3, "$j" : 5 })" :)
for $i in parallelize((1,2,3)) where $i ge 2 for $j in (4,5,6) order by $j, $i where $j lt 6 return {"$i": $i, "$j": $j}
