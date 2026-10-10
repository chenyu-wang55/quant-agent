# 量化系统优化检查（2026-10-09）

检查日期：2026-10-09，America/Toronto。代码基线：`1fdafd400fa0b7e2025a4bc65d0cea9445efadab`。

依据：代码审查、5 个离线或临时数据库探针、生产 SQLite 只读查询、PyPI 官方版本元数据，以及当前 OpenAI 官方文档。上一次完整测试结果为 185 项通过；此次新增检查场景没有被该测试集充分覆盖。本次没有升级依赖、修改业务代码、迁移生产数据库或开启交易。

## 判断

系统具备规则选股、确定性价格计划、审计、回测和执行控制的基础。实际 AI 分析仍是模板生成：`services/llm/explainer.py:6` 没有调用大模型；仓库根目录的 `agents/us_equity_quant_agent.yaml` 写有 `gpt-5.4`，但未发现后端加载该 YAML 的执行链路。更换配置中的模型名称不会改变当前后端行为。

目前生产库仍为 `0035`，代码为 `0037`；594 个快照全部来自 Mock，最新日期为 7 月 8 日，订单和持仓为零。真实数据、历史回测验收和 20 天纸交易证据仍是运行前置条件。

## 已确认问题及优先级

P0：开启交易或扩展为 API + Worker 持续运行前必须处理。P1：下一轮优化处理。P2：在真实数据和基线结果具备后推进。

| 优先级 | 问题与证据 | 建议改动与验收条件 |
|---|---|---|
| P0 | 安全锁跨进程状态可能过期。`apps/api/dependencies.py:419` 在实例初始化时读取安全锁；执行路径读取 `self.kill_switch.enabled`，未发现每次提交前重新读取持久化状态。用两个独立 AppState 模拟服务：第一实例开启安全锁后，第二实例缓存仍为 false，而数据库为 true。 | 在每次买卖、恢复提交及自动执行前读取权威控制状态，数据库读取失败时阻断。增加两个进程共享数据库的测试：任一进程开启锁后，另一个进程的下一次提交不得调用 Broker。 |
| P0 | 券商保护检查没有核对数量覆盖。`infra/observability/health.py:435` 检查腿类型、价格是否存在和状态。离线探针中持仓 100 股、止损腿和止盈腿各 1 股，仍返回 ready=true。 | 核验卖出方向、标的、剩余保护数量、与当前止损控制一致的价格，以及对应订单关系；处理多个买入订单和部分成交。数量不足、方向错误、价格不符、关联失效必须阻断。该探针只证明健康检查缺口，不代表已发生真实无保护成交。 |
| P0 | 业务状态初始化会自动迁移数据库。`apps/api/dependencies.py:419` 调用 `init_db()`；`infra/db/init_db.py:84` 执行 `upgrade head`。首次访问依赖 AppState 的业务 GET 或 `/health/ready` 可能触发迁移。 | 将迁移放到独立维护命令，先备份并验证升级/恢复；API 初始化和 readiness 只检查版本，版本不匹配时给出明确阻断。增加“健康查询不改变数据库版本”的集成测试。 |
| P1 | 新闻解析仍只读取旧平铺字段。`services/ingestion/vendors/yfinance_provider.py:744` 依赖 `providerPublishTime`、`title` 等字段。离线输入 `content.pubDate/title` 包装格式时返回 0 个事件，相同内容的旧格式返回 1 个。当前安装包取新闻使用 tickerStream 数据流，旧平铺契约不足。 | 同时支持包装格式和旧格式，规范时间、标的、来源链接和去重；未知格式产生质量告警，不静默当作“没有新闻”。用脱敏供应商响应样本做契约测试。探针证明包装格式被丢弃，未调用真实新闻接口测量当天实际丢弃率。 |
| P1 | PIT 文件缓存更新失效。`services/ingestion/vendors/yfinance_provider.py:165` 仅按路径缓存。将同一 CSV 从 OLD/version1 覆盖为 NEW/version2 后，仍读到 OLD/version1。 | 使用内容指纹或不可变数据集版本作缓存键；一次研究运行固定一个输入版本。让验证指纹、实际读取内容和快照指纹一致，测试原路径替换与运行期间变化。 |
| P1 | 中文分析可能与信号相反。`services/ranking/recommendation_builder.py:118` 固定生成趋势向上、事件支持等描述。输入所有分数 0.1 且 risk_off，仍得到“趋势仍向上”“有条件可做”的报告。 | 先改为依据每项信号和缺失字段生成事实、反证及不确定性；对低分和 risk_off 明确表达。增加弱趋势、负面事件、缺少新闻、信号冲突的内容回归检查。 |

新闻供应商实现可核对 [yfinance 官方源码](https://github.com/ranaroussi/yfinance/blob/main/yfinance/base.py)。应先验证输入契约，再升级供应商库。

## AI 升级的实际用途

建议增加独立的模型适配层，统一配置模型、超时、预算、重试、输出契约及日志。第一阶段限定三个任务：

1. 从财报、公告和新闻中抽取结构化事件，保留来源、原文证据、发布日期和可用日期。
2. 检查价格证据、基本面和事件证据之间的冲突，输出支持因素和反对因素。
3. 根据已经计算好的价格计划生成中文分析，引用明确证据并列出数据不足之处。

可使用 [Responses API 的 Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=responses) 约束字段。Schema 合法不等于事实正确，还要验证引用、时间边界和数值一致性。入场价、止损、仓位及是否可执行继续由确定性程序校验。

当前 [OpenAI API 模型目录](https://developers.openai.com/api/docs/models) 列出 GPT-6 Luna、GPT-6.1 Sol 和 GPT-6 Astra。可分别作为低成本抽取、综合分析和复杂冲突复核的候选；这只是待评估分工，需核对账户可用性并用相同数据对比正确率、遗漏率、延迟和成本。当前系统没有接入这些模型。

每次 AI 结果应保存实际模型版本、提示词版本、输入证据指纹、原始响应或响应指纹、成本、延迟和校验结果。归档历史评估不得混入当前网页资料或模型自身掌握的未来事件；优先使用当时归档的抽取结果，并要求每个事实可追溯到当时已可用的证据。

模型评估样本和规则应保存在仓库或独立评估工具中。当前官方公告显示 OpenAI Evals 平台在 **2026-10-31** 转为只读，计划 **2026-11-30** 关闭；新建方案应避免依赖该即将停用的平台。[官方停用公告](https://developers.openai.com/api/docs/deprecations)

## 策略和回测优化

- `RecommendationBuilder.build()` 直接把 composite_score 作为 confidence。这是规则分数，不能当作经过校准的胜率。现有回测会报告 Brier/ECE，但未看到把校准模型应用回推荐的链路。建议区分信号强度与预测概率，使用独立验证样本拟合校准器并保留样本外检验。
- `BacktestEngine._segment_metrics()` 是单次回测结果的时间切分，没有实现多轮滚动训练/验证。若开始学习权重或引入统计模型，应增加滚动样本外评估、重叠持仓标签的隔离、参数搜索记录和交易成本压力测试。
- 先保留当前规则模型作为基线，逐项比较新事件抽取、校准和学习型排序的增量价值。中文报告更流畅、模型推理更强，均不能单独证明策略收益改善。
- 在完整 PIT 数据、真实样本外结果和纸交易证据具备前，继续保持交易锁定。

## 依赖版本检查

以下为检查时从 PyPI 官方 JSON 元数据获取的最新发布版本，尚未安装或做兼容性验证。

| 依赖 | 当前锁定/安装 | PyPI 最新 | 官方来源 |
|---|---|---|---|
| FastAPI | 0.139.0 | 0.143.0 | [PyPI](https://pypi.org/project/fastapi/) |
| Pydantic | 2.13.4 | 2.14.0 | [PyPI](https://pypi.org/project/pydantic/) |
| SQLAlchemy | 2.0.51 | 2.1.4 | [PyPI](https://pypi.org/project/sqlalchemy/) |
| yfinance | 1.5.1 | 1.7.0 | [PyPI](https://pypi.org/project/yfinance/) |
| pandas | 3.0.3 | 3.0.6 | [PyPI](https://pypi.org/project/pandas/) |
| Starlette | 1.3.1 | 1.7.0 | [PyPI](https://pypi.org/project/starlette/) |
| Alembic | 1.18.5 | 1.20.0 | [PyPI](https://pypi.org/project/alembic/) |
| Uvicorn | 0.51.0 | 0.54.0 | [PyPI](https://pypi.org/project/uvicorn/) |

建议按“数据适配器与 pandas”“FastAPI 与其兼容依赖”“SQLAlchemy 与 Alembic”分别升级和回归。SQLAlchemy 跨次版本须重点验证事务、幂等、迁移及回滚；Starlette 通过 FastAPI 的兼容依赖解析选择。当前测试已出现 httpx TestClient 的弃用警告，升级 Web 栈时一并检查测试客户端迁移。

CI 增加定期依赖升级 PR、供应商数据契约测试和依赖漏洞扫描。当前版本检查没有执行完整漏洞扫描，不能据此宣称某个版本安全或不安全。[FastAPI 官方升级建议](https://fastapi.tiangolo.com/deployment/versions/)

## 建议实施顺序

1. 修复安全锁跨实例刷新、保护数量核验和请求触发迁移的问题，并补充相应场景测试。
2. 修复新闻格式、缓存更新和分析文本与证据不一致；按组升级依赖并验证。
3. 通过明确维护流程升级生产库，接入完整 PIT 数据，建立真实回测基线。
4. 增加有来源和版本记录的 AI 抽取/解释层及本地评估，随后开展滚动样本外对比和受监督纸交易。

当前本地 SQLite 架构可继续使用；在并发规模、锁等待或历史数据量成为实测瓶颈时，再评估数据库和任务队列扩展。先解决跨进程状态一致性与数据可信度。
