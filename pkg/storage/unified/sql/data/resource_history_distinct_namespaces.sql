SELECT DISTINCT {{.Ident "namespace"}}
FROM resource_history
WHERE {{.Ident "group" }} = {{.Arg .Group }}
  AND {{.Ident "resource" }} = {{.Arg .Resource }}
  AND {{.Ident "resource_version" }} > {{.Arg .SinceRv }}
