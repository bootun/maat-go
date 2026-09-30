# 事件流对账 fixture（副本）

从后端仓库的 `test/fixtures/reconcile/` 复制，不要在这里单独修改（ADR 0052）。格式见后端仓库中同目录的 README。
`reconcile_fixture_test.go` 用它们测试 `Reconciler`：只比较 `text` 与 `rewound` 两类输出。
