(:JIQS: ShouldRun; Output="([ 1, { "guess" : "Arabic", "choices" : [ "Arabic", "Cantonese", "Maltese", "Samoan" ] }, { "guess" : "Arabic", "choices" : [ "Albanian", "Arabic", "Czech", "Dutch" ] } ], [ 2, { "guess" : "Arabic", "country" : null, "choices" : [ "Arabic", "Greek", "Hebrew", "Spanish" ] } ], [ 3, { "guess" : "Arabic", "country" : "IN", "choices" : [ "Arabic", "Greek", "Hebrew", "Spanish" ] } ], [ 4, { "guess" : "Bulgarian", "target" : null, "country" : "AU", "choices" : [ "Albanian", "Bulgarian", "Russian", "Ukrainian" ] }, { "guess" : "Bulgarian", "target" : null, "country" : "AU", "choices" : [ "Arabic", "Bulgarian", "Hebrew", "Spanish" ] } ], [ 5, { "guess" : "Bulgarian", "target" : "Albanian", "country" : "AU", "choices" : [ "Albanian", "Bulgarian", "Russian", "Ukrainian" ] }, { "guess" : "Bulgarian", "target" : "Albanian", "country" : "AU", "choices" : [ "Arabic", "Bulgarian", "Hebrew", "Spanish" ] } ], [ 6, { "guess" : "Slovenian", "target" : "Norwegian", "country" : "AU", "choices" : [ "Dinka", "Nepali", "Norwegian", "Slovenian" ] } ])" :)
for $i in parallelize((
 {"guess": "Slovenian", "target": "Norwegian", "country": "AU", "choices": ["Dinka", "Nepali", "Norwegian", "Slovenian"]},
 {"guess": "Arabic", "choices": ["Arabic", "Cantonese", "Maltese", "Samoan"]},
 {"guess": "Arabic", "choices": ["Albanian", "Arabic", "Czech", "Dutch"]},
 {"guess": "Arabic", "country": "IN", "choices": ["Arabic", "Greek", "Hebrew", "Spanish"]},
 {"guess": "Arabic", "country": null, "choices": ["Arabic", "Greek", "Hebrew", "Spanish"]},
 {"guess": "Bulgarian", "target": "Albanian", "country": "AU", "choices": ["Albanian", "Bulgarian", "Russian", "Ukrainian"]},
 {"guess": "Bulgarian", "target": "Albanian", "country": "AU", "choices": ["Arabic", "Bulgarian", "Hebrew", "Spanish"]},
 {"guess": "Bulgarian", "target": null, "country": "AU", "choices": ["Albanian", "Bulgarian", "Russian", "Ukrainian"]},
 {"guess": "Bulgarian", "target": null, "country": "AU", "choices": ["Arabic", "Bulgarian", "Hebrew", "Spanish"]}))
group by $guess := $i.guess, $target := $i.target, $country := $i.country
order by $guess, $target, $country, serialize($i.choice)
count $group_index
return [$group_index, $i]

(: complex test with nulls and empty sequences - grouping variable defined in place:)
