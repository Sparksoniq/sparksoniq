(:JIQS: ShouldRun; Output="([ 1 ], foo, true, false, null, 1.2, { "foo" : "bar" }, { "foo" : 1, "bar" : null, "foobar" : [ "test1", "test2" ] }, { "foo" : [ { "bar" : 1, "foobar" : 2 }, { "bar" : 3, "foobar" : 4 } ] })" :)
parse-json("[1]"),
parse-json("\"foo\""),
parse-json("true"),
parse-json("false"),
parse-json("null"),
parse-json("1.2"),
parse-json(()),
parse-json("{\"foo\":\"bar\"}"),
parse-json(unparsed-text("../../../queries/SpreadOverMultipleLinesDocument.json")),
(unparsed-text-lines("../../../queries/denormalized.json")!parse-json($$))[1],
try { parse-json("{\"foo\":\"bar}") } catch XPST0003 { () }
