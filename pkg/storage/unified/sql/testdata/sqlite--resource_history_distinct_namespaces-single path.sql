SELECT DISTINCT "namespace"
FROM resource_history
WHERE "group" = 'group'
  AND "resource" = 'res'
  AND "resource_version" > 10000
