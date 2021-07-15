(:JIQS: ShouldRun; Output="({ "code" : "CA", "stores" : 3 }, { "code" : "MA", "stores" : 2 }, { "code" : "MI", "stores" : 3 }, { "code" : "NY", "stores" : 1 })" :)
let $stores := parallelize((
  { "storeid" : 1, "state" : "CA" },
  { "storeid" : 2, "state" : [ "MA", "CA" ] },
  { "storeid" : 3, "state" : "MA" },
  { "storeid" : 4, "state" : "CA" },
  { "storeid" : 5, "state" : [ "NY", "MI" ] },
  { "storeid" : 6, "state" : "MI" },
  { "storeid" : 7, "state" : "MI" }
))
let $states := parallelize((
  { "code" : "CA", "name" : "California" },
  { "code" : "MA", "name" : "Massachussetts" },
  { "code" : "NY", "name" : "New York" },
  { "code" : "MI", "name" : "Michigan" },
  { "code" : "WA", "name" : "Washington" }
))
return
for $store in $stores
for $state in $states[$$.code = flatten($store.state)]
group by $code := $state.code
order by $code
return { "code" : $code, "stores" : count($store) }


