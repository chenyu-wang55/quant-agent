# 新优化工具与运行约定

本次实现状态和验证证据见 [实施记录](optimization_progress_2026-10-10.md)。从项目目录运行下述命令，统一使用 `.venv`。模型抽取仅供独立解释和旁路研究，没有接入交易决策，也没有券商工具权限。

## 执行安全与维护

API 初始化和就绪查询仅检查数据库版本；缺库、缺表或 revision 不匹配时返回 503 和 `maintenance_required`，不会迁移、创建或重建数据库。维护者先停止写入并备份，再显式运行 `.venv/bin/python -m infra.db.init_db`。

最终发送许可使用 SQLite `BEGIN IMMEDIATE`，与控制写入按提交顺序排序。锁定提交后才取得许可的请求必须拒绝；已经取得许可的请求属于在途请求，需要对账。订单意图中持久保存自动策略来源和版本，重试或恢复发送仍需当前策略允许；策略版本变更会阻断旧意图的新发送。

原生保护必须以新鲜的券商对账确认净持仓。每个 OCO 组需要一条止损腿和一条止盈腿，分别检查方向、标的、父订单、价格、剩余数量及观察时间。两条腿不相加，多个组的每种退出路径分别合计，既不允许不足，也不允许超额卖出。保护或持仓调整后先同步券商订单和对账，再评估就绪。

## AI 抽取与离线评估

输入 JSON 包含 `decision_at` 和 `documents`。每个文档包含 `source_id`、`source_url`、`published_at`、`available_at` 和 `text`，时间须带时区，发布时间 ≤ 可用时间 ≤ 决策时间。先执行：

```bash
.venv/bin/python scripts/extract_evidence.py input.json --run-id research-001 --validate-only
```

默认 `QUANT_LLM_ENABLED=0`，不会请求 API。启用前必须配置 `OPENAI_API_KEY`、`QUANT_LLM_MODEL`、正数的 `QUANT_LLM_DAILY_BUDGET_USD`、`QUANT_LLM_RUN_BUDGET_USD`、与账户/模型匹配的 `QUANT_LLM_INPUT_USD_PER_MILLION` 和 `QUANT_LLM_OUTPUT_USD_PER_MILLION`。另有 `QUANT_LLM_TIMEOUT_SECONDS`（默认20）、`QUANT_LLM_MAX_OUTPUT_TOKENS`（800）、`QUANT_LLM_RETRIES`（0，上限2）、`QUANT_LLM_MAX_CONCURRENCY`（2，上限16）。这些数值不是已批准的费用预算。

实现依据 [OpenAI Docs：Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=responses)。请求固定使用 Responses 接口、严格 JSON Schema 和 `store=false`，不提供工具。每次请求前按输入字节、额外开销及最大输出 token 预留费用；未知结果保留全部预留，已发出的请求仍可能计费。预算或并发不足时不请求；计量超出预留时阻断结果并记账，要求重新核对价格配置。并发租约在超时加60秒后过期，崩溃不会退回已预留费用。独立进程必须共享同一个 `artifacts/llm/budget.db` 才能共享上限。

拒绝、截断、无效字段、无原文引用或未来证据均不发布。失败时输出 `blocked`、空模型事实和模板说明，模型信号保持缺失。有效输出以内容指纹缓存；原文、原始响应、实际模型、提示词/Schema、usage、已知或未知费用与延迟保存在 `artifacts/llm`，目录0700、文件0600，默认不纳入 Git。引用校验只能证明字面来源，不能证明抽取语义正确，必须另做金标评估。

金标文件是 JSON 数组，每项含 `case_id`、`group_id`、`split`（train/validation/test）、`decision_at`、`documents` 和 `gold: {"facts": [...]}`；事实字段含 `source_id`、`field`（event/metric/date/ticker/direction）、`value` 和原文 `quote`。近似发行人/事件/时间样本使用同一 group，禁止跨划分。预测文件是 `case_id` 到抽取审计 JSON 的映射。一次评估只比较同一实际模型/提示词/Schema 版本：

```bash
.venv/bin/python scripts/evaluate_evidence.py --gold gold.json --predictions candidate.json --output artifacts/llm/evaluation.json
```

命令不调用 API，输出字段分母、精确率/召回率、Wilson区间、事件召回、有效覆盖率、延迟/费用和全部失败案例。样本不足200条/测试不足60条或门槛不达标时退出码为2；拒绝全部样本不能通过。确定性模板基线没有抽取事实，可用空预测映射记录其召回及覆盖率。`manifests/ai_evaluation_template.json` 是待填模板；当前没有真实金标或模型质量验收结果。

## 概率校准与滚动窗口

API 新增 `signal_strength`、`confidence_kind="rule_signal_strength"` 与默认空的 `calibrated_probability`，旧 `confidence` 保持兼容。当前系统没有发布胜率。历史回测原有 confidence 分组统计属于规则分数诊断；独立校准器不自动替换策略。

`services/research/calibration.py` 只拟合训练边界前已经结束的标签，以训练期事件发生频率作为常数概率基线，将稀疏分箱向该频率收缩。后续样本单独报告 Brier/ECE、可靠性分组和样本数；保存目标、策略、数据指纹、训练边界及训练标签哈希。

准备 `manifests/research_template.json` 的真实版本，填写授权来源、共同区间、策略/数据指纹、成本、标签文件哈希，并在查看结果前改为 `status=frozen`、`data_kind=authorized_point_in_time`。标签 JSON 数组每项含 `observation_id`、`decision_at`、`label_end`、`signal_strength` 和布尔 `profitable_after_cost`。标签来自冻结策略及退出/成本口径，不得把当前实时数据回填成历史输入。

```bash
.venv/bin/python scripts/evaluate_calibration.py --manifest frozen-research.json --labels labels.json --output artifacts/calibration.json
```

默认窗口训练36个月、验证6个月、OOS6个月、前移6个月，末尾12个月留出；至少3轮。跨分段边界结束的标签被剔除，后续分段先隔离14个日历日；实际最长标签持有时间若超过该隔离期，冻结前须提高隔离期。最终留出集不由此命令读取评估，输出始终 `adopted=false`。此工具仅交付概率诊断；完整参数搜索、交易成本/延迟/流动性压力对比、置信区间及最终留出验收仍需真实数据和另行完成的 Q2/Q3 研究。

## 受监督 paper 窗口

证据 Schema 升为2，签名绑定策略配置 ID、影响交易行为的风控/审批参数及 `EXECUTION_BEHAVIOR_VERSION`。普通启停、paper/live模式切换、审计时间和日常数据刷新不重置计日；数据指纹记录在每次证据内。改变策略或执行行为必须生成新版本，旧 Schema 或签名不匹配的证据不计。

只有真实、通过验证报告的 PIT 输入、完整行情/基本面、必要运行门槛、实际自动执行评估、常规时段和无执行错误的当次运行能计日。历史回填 `as_of` 超过运行时点5分钟不计；Mock 永不计入合格天数。

每日顺序：Worker停止 → paper配置与冻结限额 → 锁定预检 → 监督者启用paper策略/自动执行并释放锁 → 再次预检 → 通过后运行Worker → 窗口结束先锁定、关闭自动执行并停Worker → 归档在途请求、对账与运行记录。使用计划中的 `.venv/bin/python scripts/preflight.py --target paper --require-kill-switch enabled/disabled` 两次预检。当前缺真实数据，不启动观察窗口；20个真实交易日和paper成交/故障演练尚未完成。
