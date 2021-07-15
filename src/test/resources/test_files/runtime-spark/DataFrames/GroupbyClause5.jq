(:JIQS: ShouldRun; Output="([ 1, { "guess" : "Arabic", "choices" : [ "Arabic", "Cantonese", "Maltese", "Samoan" ] }, { "guess" : "Arabic", "choices" : [ "Albanian", "Arabic", "Czech", "Dutch" ] } ], [ 2, { "guess" : "Bulgarian", "choices" : [ "Albanian", "Bulgarian", "Russian", "Ukrainian" ] }, { "guess" : "Bulgarian", "choices" : [ "Arabic", "Bulgarian", "Hebrew", "Spanish" ] } ])" :)
for $i in parallelize((
 {"guess": "Arabic", "choices": ["Arabic", "Cantonese", "Maltese", "Samoan"]},
 {"guess": "Arabic", "choices": ["Albanian", "Arabic", "Czech", "Dutch"]},
 {"guess": "Bulgarian", "choices": ["Albanian", "Bulgarian", "Russian", "Ukrainian"]},
 {"guess": "Bulgarian", "choices": ["Arabic", "Bulgarian", "Hebrew", "Spanish"]}))
group by $guess := $i.guess
order by $guess, serialize($i.choices)
count $group_index
return [$group_index, $i]

(: test with no null or empty entires - grouping variable defined in place:)
