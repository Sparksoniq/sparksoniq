(:JIQS: ShouldCrash; ErrorCode="RBML0005"; :)
annotate(
    (
        {"id": 1, "age":  20, "weight": 68.8},
        {"id": 2, "age":  35, "weight": 72.4},
        {"id": 3, "age":  50, "weight": 76.3}
    ),
    {"id": "integer", "age": "string", "weight": "double"}
)

(: schema has incorrect type - IllegalArgumentException :)
