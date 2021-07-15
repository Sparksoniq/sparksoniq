(:JIQS: ShouldRun; Output="({ "$guess" : "Arabic", "$target" : null, "$country" : null, "$i" : [ { "guess" : "Arabic", "choices" : [ "Arabic", "Cantonese", "Maltese", "Samoan" ] }, { "guess" : "Arabic", "choices" : [ "Albanian", "Arabic", "Czech", "Dutch" ] } ] }, { "$guess" : "Arabic", "$target" : null, "$country" : null, "$i" : { "guess" : "Arabic", "country" : null, "choices" : [ "Arabic", "Greek", "Hebrew", "Spanish" ] } }, { "$guess" : "Arabic", "$target" : null, "$country" : "IN", "$i" : { "guess" : "Arabic", "country" : "IN", "choices" : [ "Arabic", "Greek", "Hebrew", "Spanish" ] } }, { "$guess" : "Bulgarian", "$target" : null, "$country" : "AU", "$i" : [ { "guess" : "Bulgarian", "target" : null, "country" : "AU", "choices" : [ "Albanian", "Bulgarian", "Russian", "Ukrainian" ] }, { "guess" : "Bulgarian", "target" : null, "country" : "AU", "choices" : [ "Arabic", "Bulgarian", "Hebrew", "Spanish" ] } ] }, { "$guess" : "Bulgarian", "$target" : "Albanian", "$country" : "AU", "$i" : [ { "guess" : "Bulgarian", "target" : "Albanian", "country" : "AU", "choices" : [ "Albanian", "Bulgarian", "Russian", "Ukrainian" ] }, { "guess" : "Bulgarian", "target" : "Albanian", "country" : "AU", "choices" : [ "Arabic", "Bulgarian", "Hebrew", "Spanish" ] } ] }, { "$guess" : "Slovenian", "$target" : "Norwegian", "$country" : "AU", "$i" : { "guess" : "Slovenian", "target" : "Norwegian", "country" : "AU", "choices" : [ "Dinka", "Nepali", "Norwegian", "Slovenian" ] } })" :)
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
let $guess := $i.guess, $target := $i.target, $country := $i.country
group by $guess, $target, $country
order by $guess, $target, $country, serialize($i.choices)
return {"$guess": $guess, "$target": $target, "$country": $country, "$i": $i}

(: complex test with nulls and empty sequences - grouping variable pre-defined with let:)
