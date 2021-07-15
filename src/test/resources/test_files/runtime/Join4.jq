(:JIQS: ShouldRun; Output="({ "name" : "C", "stores" : [ 1 ] }, { "name" : "C", "stores" : [ 2 ] }, { "name" : "C", "stores" : [ 3 ] }, { "name" : "A", "stores" : [ ] }, { "name" : "B", "stores" : [ ] })" :)
let $stores := parallelize([  { "store number" : 1, "state" : "MA" }, { "store number" : 2, "state" : "MA" }, { "store number" : 3, "state" : "MA" } ])
let $nations := parallelize([  { "state" : "A", "p" : 1 }, { "state" : "B", "p" : 2 }, { "state" : "C", "p" : 3 } ])
return
  for $nation in $nations[], $store allowing empty in $stores[][ $nation.p eq  last() ] 
  return { "name": $nation.state, "stores": [ $store."store number" ] }
