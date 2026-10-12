# 免费数据采集与验收

2026-10-12 起按所有者要求只使用免费数据源，不购买或调用 Sharadar、Zacks、Benzinga 等付费服务。
当前交付为独立研究采集工具，不修改生产 provider、数据库、Autopilot 策略或 paper 门槛。

## 来源与边界

| 来源 | 获取内容 | 当前不能据此证明的内容 |
|---|---|---|
| [SEC 官方 API](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) | 公司申报历史、历史扩展文件、Company Facts 财务观测 | 完整事件流、提前公布的财报日历、分析师预期、数据当时的采集版本 |
| [yfinance](https://ranaroussi.github.io/yfinance/) | Yahoo 日线 OHLCV、可取得的调整收盘与企业行动字段 | 退市覆盖、终止现金流、历史买卖价差、供应商当时的数据版本 |
| [Stooq](https://stooq.com/) | 显式选择的日线 CSV 研究来源 | 复权方式、拆股分红、退市覆盖；缺失字段保持空值 |

SEC 无需 API key。采集客户端识别自身，单进程顺序请求每次间隔至少0.25秒；429/5xx最多额外重试两次，遵守可解析且不超过30秒的 Retry-After。超过该等待值或HTTP日期形式的 Retry-After 会停止本次请求。403直接终止该请求，不更换身份或绕过限制。不同进程的总访问量仍须遵守 [SEC Fair Access](https://www.sec.gov/about/developer-resources) 每用户10次/秒限制。

yfinance官方说明其用途为研究与教育，Yahoo API面向个人使用；免费获取不等于有权再分发数据。原始文件保存在忽略提交的研究目录，GitHub只包含代码、合成测试和脱敏运行状态。

## 采集

从 `quant-agent` 项目目录使用正式 `.venv`，显式提供研究标的与CIK。CIK可在SEC公司申报页面确认；这个对应关系不代表历史成分股名单或经过验证的历史代码映射。

```bash
.venv/bin/python scripts/collect_free_data.py \
  --security AAPL:320193 --security MSFT:789019 --benchmark SPY \
  --start 2024-01-01 --end 2025-12-31 \
  --decision-at 2026-01-01T05:00:00Z \
  --price-source yfinance
```

默认输出为 `artifacts/free_data/<SHA-256>/`，单批允许1–20个不同CIK的研究标的。可用 `--output-dir`指定目录。通过 `SEC_USER_AGENT`或 `--sec-user-agent`设置能识别研究客户端的名称和真实联系信息；不在代码或报告中编造邮箱、保存身份头或加入 API key。源站拒绝时保留失败，等待恢复后再执行新批次。

`--price-source stooq`显式选择Stooq；不会在Yahoo失败后悄悄切换来源。Stooq的调整收盘、分红、拆股字段保持未知，不将0当作缺失值。两个来源都受限时同样输出partial，不制造行情。成功取得的部分证据仍可保留；采集错误时退出码2。

## 历史时点约束

- SEC观测保存CIK、概念、单位、财务起止日期、accession、申报类型、来源URL和可用时间；不能将季度/年度或不同币种观测混合。
- 可用时间保守取申报日纽约时区结束后的零点，与可取得的接受时间加5分钟两者的较晚值。没有接受时间时标记日期边界推定，不使用财务期末、当天零点UTC或当前下载时间代替历史披露时间。它是研究假设，不承诺历史实际采集延迟。
- `facts_as_of.jsonl`只使用决策时点已经可用的申报。后来的重述不能覆盖早期选取；同一时点有相互矛盾的观测时拒绝该组件。历史扩展文件缺失时不猜申报身份。
- 原始Company Facts和申报响应可能包含决策时点之后的资料。历史研究只能使用经过过滤的标准化文件，不能直接把完整原始响应作为当时输入。
- 日线完整OHLCV在XNYS实际收盘后15分钟才标为可用，包括13:00提前收盘日；午夜日期索引不代表日线当时可用。非交易日、重复、非有限数值和错误OHLC范围会被拒绝。

## 快照与离线核验

每批归档包含SEC原始JSON、规范化申报/财务观测、决策时点选取、行情JSONL和manifest。内容指纹覆盖参数、来源文件及错误状态；新批次写入新目录，不替换旧批次。manifest和文件以本机私有权限保存，逐文件记录SHA-256与大小。

```bash
.venv/bin/python scripts/collect_free_data.py --verify artifacts/free_data/<SHA-256>
```

离线核验不创建网络客户端；文件改动、指纹变化和符号链接会失败。`archive_integrity=verified`仅表示归档字节一致。

即使全部下载成功，manifest也只标记 `research_only`、`production_ready=false`、`pit_validation_status=not_verified`。完整研究样本与自动运行输入仍不同：历史成分与代码变化、退市和终止现金流、历史来源版本、EPS预期修正、提前已知的财报日历、事件覆盖、价差和程序所需财务比率仍待验证。工具不会填零、自动推导未定义口径、生成四类生产PIT CSV或通过20日paper计数。

退出码0表示采集组件完成或归档完整性通过，不能理解为策略收益或交易就绪通过。下一步在免费来源可访问后取得真实样本、核验以上缺口；如采用少特征研究策略，须另冻结策略版本和成本，仍不降低生产验收标准。
