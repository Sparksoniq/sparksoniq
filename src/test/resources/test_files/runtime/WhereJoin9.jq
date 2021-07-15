(:JIQS: ShouldRun; Output="({ "code" : "CA", "stores" : 2 }, { "code" : "MA", "stores" : 2 }, { "code" : "MI", "stores" : 2 }, { "code" : "NY", "stores" : 1 })" :)
let $stores := json-file("../../queries/stores.jsonl")
let $states := json-file("../../queries/states.jsonl")
return
for $store in $stores
for $state in $states
where $state.code eq $store.state
group by $code := $state.code
order by $code
return { "code" : $code, "stores" : count($store) }


