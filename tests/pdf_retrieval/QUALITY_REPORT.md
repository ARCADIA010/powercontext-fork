# PDF 质量诊断与验证

基线：`a3cf84cac203217ccde6e37716071d95498ac7d5`。独立 worktree：`/root/projects/pc-pdf-quality`；功能分支：`feat/pdf-quality`。只允许推送 team 同名分支。

## 诊断结论

平台上一轮只给出 0/50，没有失败日志，具体触发项仍未知。公开验收基线实际为 `6 passed in 46.16s`；其检查覆盖 503 页入库和引用回查，却没有断言证据页命中、官方词项召回或全文 F1。因此公开全绿不能解释或排除平台质量失败。

在真实、持久化并重启的 scoped Memory 上复现了两个检索缺陷：

- 自建 D&A/capex 问句已有现金流物理页 64 进入候选，却未进入前 20；这些缩写没有扩展为原文的 depreciation and amortization / capital spending。新版本返回该页第 1 名。
- EBITDA 多页问句已召回物理页 62 和 64，但基线重排只保留现金流页 64（第 5），利润表页 62 被排出前 20。加入通用财务概念词及页首标题相关性后，两页分别位于第 1、19 名。
- 另有地理分部跨页延续页 5 未进入候选。这个缺口保留，未按页码补齐结果或篡改问题标签。

新增的真实 Runtime 回归测试对基线实际得到 `2 failed, 2 passed in 8.14s`：D&A/capex 预期页 1，却返回页 25、29；仅有一个现金流页的数据库中该问句返回空元组。这些是可复现产品缺陷，但不能据此断定平台唯一失败原因。

## 解析与接口核验

完整读取公开原 PDF，物理页 503 页，词项总数 191,777；逐页词项数 20–755；最长页 4,934 字符；无空文本页、替换字符、NUL 或异常控制字符。标准分词使用题面 `[a-z]+|\d+`。

原解析逻辑保持不变。基线与改进版再次完整解析后，503 页的全部 PDFPageInput 字段逐页相同：正文、物理页码、文档 ID 和完整文件 SHA 均一致。该次耗时为 5.764 / 6.513 秒；这是运行样本，不是性能保证。

使用本机已有 PDFium 独立提取对照：503 页词项数比例均处于 90%–110%；其中物理页 434 的词项多重集合 F1 为 0.946524，其余页至少 0.95。这是工具间对照，不是官方冻结文本或标注全文；不能宣称正式解析质量门槛已通过。第 434 页为目录页，未据此清洗或重写原文。

人工查看原 PDF 物理页 62、74 的渲染，核对了利润表、折旧/摊销表及国别收入表的年份、数字、括号负数与行列关系。两页保留了原页内容，未以文本替代视觉核对。

官方边界及已有自测覆盖空白页、空密码权限、坏文件和后页损坏原子失败、长文档身份、修订身份、metadata 精确字段、幂等入库、重启回查、distinct/limit 和 scope 隔离。

## 改动

产品仅修改 `src/powercontext/sources/pdf_pages.py`：

- 查询侧财务通用别名，例如 capex、D&A、operating income/profit、income statement/P&L；英美拼写与 FY 年份规范化。没有文档 ID、页码、特定问句或 workload 特判。
- BM25 保留原有数值降权，新增概念短语和页首标题相关性。没有在未测量的前提下更改共享 FTS 准入阈值、候选上限或 SQL。
- 利用公开 Memory search hit 的完整文本做候选重排，对最终入选条目通过公开 get 检查唯一 PDF Source，再按 Source ID 去重。没有绕过 Memory、伪造 citation、读取检索外原 PDF 或裁剪 Source 正文。

新增产品自有回归、冻结的自建问题集、可重跑 benchmark 和带逐路候选/最终页码/分数的测量 JSON。未修改 competition、依赖、锁文件、共享 venv、主工作区或其他题逻辑。

## 同一问题集的前后测量

全部标签由阅读公开财报正文后自行制定，使用物理页码；没有读取答案文件或隐藏证据。开发集 12 问用于改进，8 问留出集在算法冻结后验证，另有 2 个负样本。全部真实持久化 503 页，重启后通过 Memory API 检索。

| 指标 | 基线 | 改进 |
|---|---:|---:|
| 开发集候选证据召回 | 14/15（93.33%） | 14/15（93.33%） |
| 开发集 Recall@20 | 12/15（80%） | 14/15（93.33%） |
| 开发集 Recall@5 | 10/15（66.67%） | 13/15（86.67%） |
| 开发集多页问题完整覆盖 | 0/2 | 1/2 |
| 留出集候选召回 / Recall@20 | 8/8 / 8/8 | 8/8 / 8/8 |
| 留出集 Recall@5 | 7/8（87.5%） | 7/8（87.5%） |
| 开发集平均查询秒数（带诊断） | 5.1292 | 2.7669 |
| 留出集平均查询秒数（带诊断） | 4.2491 | 2.4219 |

耗时是同一测量程序运行样本，包含评分追踪且受缓存和并行读取影响，不是正式延迟保证。无评分追踪的初次样本约为开发集 2.812→1.215 秒、留出集 2.458→1.143 秒。

两个负样本前后均只有 1/2 返回空。完全不匹配词返回空；包含公司名和年份的铀同位素问题仍返回页面，暴露词法召回的局限。该接口返回候选证据，不保证问题可回答。中文问句对英文财报的跨语言能力未验证；没有外部模型或在线服务。

## 实际验收输出

命令：

```bash
make -C /root/projects/powercontext/competition PRODUCT=/root/projects/pc-pdf-quality test-pdf test-csv test-pi test-rerank test-evidence test-embedding test-incremental
```

按命令顺序的实际输出：

```text
6 passed in 32.72s
17 passed in 2.72s
15 passed in 6.03s
10 passed in 1.72s
3 passed in 2.16s
8 passed in 2.73s
14 passed in 1.90s
```

产品自有测试命令（独立 basetemp，无环境同步）：

```bash
cd /root/projects/pc-pdf-quality
PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
PYTHONPATH=/root/projects/pc-pdf-quality/src:/root/projects/powercontext/competition \
/root/projects/powercontext/competition/.venv/bin/python -m pytest \
-c /root/projects/powercontext/competition/pytest.ini \
--confcutdir=tests/pdf_retrieval --basetemp=.artifacts/pdf-quality/pytest-regression \
tests/pdf_retrieval -q
```

结果：`13 passed in 9.57s`。产品源码、benchmark、自有回归通过 Ruff/format 检查；产品模块通过显式指定只读共享解释器的 ty 检查。未改调度，无须扩大到 count-trigger。

## 重现质量测量

```bash
cd /root/projects/pc-pdf-quality
mkdir -p .artifacts/pdf-quality
git show a3cf84cac203217ccde6e37716071d95498ac7d5:src/powercontext/sources/pdf_pages.py > .artifacts/pdf-quality/baseline-pdf-pages.py
# 以下两条均使用共享解释器，只读依赖，不调用 uv。
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$PWD/src:/root/projects/powercontext/competition /root/projects/powercontext/competition/.venv/bin/python tests/pdf_retrieval/benchmark_pdf_quality.py --label baseline --split all --module-path .artifacts/pdf-quality/baseline-pdf-pages.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$PWD/src:/root/projects/powercontext/competition /root/projects/powercontext/competition/.venv/bin/python tests/pdf_retrieval/benchmark_pdf_quality.py --label final --split all
```

默认读取仓库公开原 PDF，以同一独立数据库对照。问题集与测量结果分别为同目录的 `pdf_quality_questions.json`、`pdf_quality_measurements.json`。benchmark 只在测试目录使用问题标签，不进入产品判题运行路径。

## 限制

公开一份财报和自建问题集不等同于正式 5 份、1151 页、24 问。官方每页冻结词数、证据召回、整页 F1 及各组 Recall@20 尚未验证；没有平台失败日志，不能宣称已定位唯一 0 分原因或获得 50 分。主助手应在检查该功能分支后统一决定集成与提测。
