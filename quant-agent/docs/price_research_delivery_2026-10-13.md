# 日频研究版交付记录

日期：2026-10-13（America/Toronto）。按所有者选择，首轮采用免费数据、明确股票池的日频技术研究版本；完整历史指数多因子与paper验收仍按原门槛执行。

## 工程完成情况

新增独立入口 `scripts/run_price_research.py`：从个人Twelve Data免费Basic账户收集20个股票与SPY的完整日线，冻结配置、保存逐文件SHA-256归档，随后生成候选、历史模拟账本、净值、分段指标及1.5×/2×成本压力。CLI可以使用同一完整归档离线重放。认证只读接口 `GET /research/price-only/latest`提供研究结果；采集或刷新失败时清空当前候选。

规则与成本在取得个人来源行情前冻结，配置见[研究清单](manifests/price_research_v1.json)。股票池于本日选择，因此历史研究存在选择及幸存者偏差。当前复权版本、静态板块标签和固定开盘成交成本也有适用限制。方法、运行命令与完整限制见[使用说明](price_research.md)。输出不进入审批、订单或paper计日，始终 `execution_allowed=false`、生产验收 `passed=false`。

不新增依赖、不修改锁文件或数据库schema。`.env.local`和全部原始数据、缓存及运行报告均被Git忽略；GitHub仅包含代码、方法、冻结规则和脱敏的工程验证证据。

## 已验证

正式Python3.13.1 `.venv`全套严格回归 **310项通过**，本轮新增 **37项**。ResourceWarning和PytestUnraisableExceptionWarning仍作为错误处理。日频研究模块覆盖率 **89.87%**，CI要求至少80%。Ruff、40个源文件mypy、15个核心模块850行上限和pip check全部通过。

测试覆盖完整日线与可用时点、未来数据不能改变早期排名、开盘跳空、仓位限制、现金与费用/已实现损益核对、成本压力、确定性重放、归档篡改/符号链接、共享免费额度、认证和失败刷新。测试中的合成行情与模拟HTTP成功响应只证明实现契约，不能作为真实选股或收益证据。

脱敏结果见[验证JSON](evidence/price_research_20261013/validation.json)。远端质量、Python3.11/3.13与锁定依赖漏洞检查，以当前PR和main的GitHub Actions结果为准。

## 真实任务尚未完成

截至这次工程验证，个人 `TWELVE_DATA_API_KEY`尚未配置。实际运行CLI返回 `blocked`，没有取得完整真实研究数据、生成真实候选或真实回测收益；运行状态不会把旧Mock快照、合成测试或官方demo当作真实完成。

SEC返回403、Yahoo限流、Stooq返回浏览器验证HTML而非行情CSV，Nasdaq出现传输错误且一次有界重试超时。Twelve官方demo证明公开契约可连通，但只覆盖一个样本且缺少要求的最新日期，程序拒绝其作为完整真实研究输入。

所有者从[免费Basic账户](https://twelvedata.com/pricing)取得个人密钥，填入本地 `quant-agent/.env.local`的 `TWELVE_DATA_API_KEY`空字段后，可执行上述入口。源站返回完整股票池/日期后再核验真实候选、账本和收益；任一标的或日期缺失仍停止，不删除标的、填零或调参凑收益。
