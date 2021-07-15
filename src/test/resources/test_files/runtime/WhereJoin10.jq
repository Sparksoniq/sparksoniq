(:JIQS: ShouldRun; Output="({ "code" : "CA", "stores" : 2 }, { "code" : "MA", "stores" : 2 }, { "code" : "MI", "stores" : 2 }, { "code" : "NY", "stores" : 1 })" :)
for $store in json-file("../../queries/stores.jsonl")
for $state in json-file("../../queries/states.jsonl")
where $state.code eq $store.state
group by $code := $state.code
order by $code
return { "code" : $code, "stores" : count($store) }


