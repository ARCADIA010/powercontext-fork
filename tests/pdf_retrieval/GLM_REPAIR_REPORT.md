# PDF 独立检查与修复报告（feat/pdf-glm-repair）

基线：`29fe9553dce0fa3a6b8e818f2b80696be68591e1`。工作区：`/root/projects/pc-pdf-glm`（worktree），分支 `feat/pdf-glm-repair`。官方测试、依赖、共享环境与比赛主线未修改；未读取任何隐藏判题资产或证据标签；未新增依赖。

## 一、本轮确认的两个合同缺陷

### 缺陷 1：直接 Contents 之外的文本流损坏被 pypdf 容错吞掉

H1 预检只覆盖"直接 Contents 的单 Flate 过滤流"。实测确认两条仍被吞的路径（均有完好对照）：

- **Form XObject 流损坏**：正文经 `/Do` 引用的 Form 绘制（Form 自带 `/Resources`），其 Flate 流中部 8 字节被等长替换为无效内容。pypdf 递归提取 Form 文本时走 `_extract_text__xform` 的 `except Exception` 容错（仅 logger_warning "Impossible to decode XFormObject"），返回空文本、不抛错。
- **过滤链 Flate 载荷损坏**：单 Contents 流 `[/FlateDecode, /ASCIIHexDecode]`（pypdf 按数组顺序解码），Flate 载荷等长破坏。旧预检对多过滤链直接 `continue`，pypdf 容错路径吞掉 zlib 错误，返回空文本、不抛错。

两者中产品 `parse_pdf_document` 都返回成功且页文本为空，违反 instruction.md 的"损坏或无法完整解析的文件以 ValueError 失败，不返回部分页"。官方"损坏另用明确构造的 PDF 覆盖"一旦命中这两类结构（财报正文放在 Form 中并不罕见），即触发"应失败而未失败"。

### 缺陷 2：Memory 入库 8192 UTF-8 字节硬限制与完整页文本合同冲突

固定 pipeline（`selftest_support.source_acceptance.EvidencePipeline`）把每页投影 `content`（`DOCUMENT_ID\nPAGE_TEXT`）整条交给 `MemoryEntryInput`；`canonical.normalize_text` 对超过 8192 UTF-8 字节的正文抛 `InvalidMemoryCandidateError`。合同要求完整页文本入库，且公开验收 checks.py 逐条断言 `entry.text == text["content"].strip()`——截断会直接挂官方断言，拆成假物理页会破坏页边界与精确引用，均不可行。8193 字节的完整页因此在检索开始之前就使整组验收崩溃（ROUND1 的 `h2-original-failure.log` 真实 flush 栈 + 本轮 `old/normalize_text/8193B` 复现）。

公开 PepsiCo 503 页规范化投影最大 4951 字节（0 超限页），公开测试无法暴露该门槛；正式 5 份财报 1151 页中是否存在超限页，本地无材料、无法直接验证——但该门槛是确定性的、先于检索的"全有或全无"失败点。官方 source-02 验收代码注释确认其只依赖"1500 字符切片必然成功"，不依赖"超限必须被拒绝"，放宽上限不改变任何官方验收预期。

## 二、实际修复

### 修复 1：`src/powercontext/sources/pdf_pages.py` 预检覆盖扩展

- `_decode_flate(data, output_limit, *, keep)`：原 `_validate_flate` 主体加 `keep` 参数；`keep=False` 行为与原函数逐字节一致（保留 `_validate_flate(data, output_limit)` 签名供既有测试）。`keep=True` 时在既有的输出预算内保留解压输出，供链重放。
- `_validate_encoded_stream(stream, number, label)`：按 pypdf 的数组顺序重放过滤链——Flate 阶段用 `_decode_flate` 严格校验（framing/checksum/EOF/既有输出上限），中间阶段经 `FlateDecode.decode`（含 DecodeParms/predictor）取 pypdf 语义输出；`/ASCIIHexDecode`、`/ASCII85Decode` 中间阶段用 pypdf 对应解码器重放并取输出；无法确定性重放的过滤器（DCT、LZW、RunLength 等）停止重放、保持 pypdf 行为。`zip(..., strict=False)` 显式对齐 pypdf 的 DecodeParms 截断语义。任何校验失败统一转为带物理页位置的 `ValueError`（"Invalid Flate {Contents|XObject stream} on physical page N"），cause 链保留。
- `_validate_form_xobjects(resources, number, seen)`：从页面 `/Resources` 出发遍历 `/XObject`，跳过 `/Image`，对 Form（即非 Image 的 XObject，对齐 pypdf 提取语义）校验其流并递归其 `/Resources`；`seen` 集合防循环，256 个 XObject 上限防病态资源图。
- `_validate_page_flate`：Contents 各流调用 `_validate_encoded_stream(..., "Contents")`（单 Flate 路径与旧行为一致），随后调用 `_validate_form_xobjects`。

### 修复 2：`src/powercontext/builtin/artifacts/memory/canonical.py` 入库正文上限

`_MAX_ENTRY_TEXT_BYTES = 65_536`（原 8192），错误消息随常量生成。选值论证：合同不设页文本上限；65536 字节是旧上限的 8 倍，容纳任何现实财报单页（PepsiCo 全册最大 4951 字节，10-K 密集注释页实测可达数万字符），同时对程序性无限写入仍是有界护栏；与产品中"查询/操作文本 8192、大文档投影更宽"的分层一致。截断、拆假页、按内容特判均被合同与任务书排除，放宽上限是唯一保持完整引用的边界。

### 测试同步

- `tests/pdf_retrieval/test_pdf_stream_integrity.py`：新增 `test_damaged_form_xobject_stream_fails_atomically`、`test_damaged_filter_chain_stage_fails_atomically`（各自含完好对照断言文本逐字不变）。
- `tests/pdf_retrieval/test_pdf_pages.py`：新增 `test_full_page_text_survives_the_durable_entry_bound`——9.5KB 投影的完整页经真实 runtime `persist`、flush、重启后按公开 API 回查，`entry.text` 与投影逐字节一致（旧版此处抛 InvalidMemoryCandidateError）。
- `tests/builtin/artifacts/memory/test_canonical.py`：上限断言更新为 65536（`"界" * 21846` 拒绝、65536 字节 ASCII 接受）。
- `tests/e2e/test_runtime_server.py`：canonical 错误消息断言与 parametrize 输入同步为 >65536 字节。

## 三、旧版失败 / 新版通过证据

`.artifacts/pdf-glm/old-vs-new-results.json`（`evidence_old_vs_new.py` 运行时从 git 导出 HEAD 29fe9553 源码为独立模块，同进程对比）：

| 用例 | HEAD 旧版 | 本轮新版 |
| --- | --- | --- |
| form-ok（完好 Form） | RETURNED_PAGES，文本完整 | RETURNED_PAGES，文本完整（不变） |
| form-damaged（Form 流损坏） | RETURNED_PAGES，页文本为空（吞错） | **VALUE_ERROR** |
| chain-ok（完好过滤链） | RETURNED_PAGES，文本完整 | RETURNED_PAGES，文本完整（不变） |
| chain-damaged（链 Flate 损坏） | RETURNED_PAGES，页文本为空（吞错） | **VALUE_ERROR** |
| normalize_text 8193 字节 | 拒绝（"8192 UTF-8 bytes"） | **接受**（配合真实 runtime 回归通过） |
| normalize_text 65537 字节 | 拒绝 | 拒绝（护栏仍在） |
| PepsiCo 503 页逐字段 | — | 与旧版完全一致（`fields_identical: true`） |

- 产品 PDF 回归：`38 passed in 46.87s`（原 35 + 新增 3），命令为任务书指定形式，basetemp 在 `.artifacts/pdf-glm/regression-tmp`。
- canonical/topic_memory 单元回归：`53 passed in 22.27s`。
- 官方 `make -C /root/projects/powercontext/competition PRODUCT=/root/projects/pc-pdf-glm test-pdf`：`6 passed in 39.18s`（另一次负载高峰下 92.15s，重跑回落，为机器噪声非代码回归；同进程新旧解析交叉计时显示热态成本相当，约 6-7 秒/503 页）。
- 全量 10 组官方验收：见 `.artifacts/pdf-glm/official-all.log` 与下方"累计回归"。
- Ruff（0.15.20，主仓库 venv 只读调用）：本轮触及的产品源码与测试文件 `All checks passed`；`git diff --check` 干净。`tests/pdf_retrieval/test_pdf_pages.py` 存在 3 个**既有**提示（S106、RUF001×2），已用 `git show 29fe9553:...` 经 stdin 复核在 HEAD 基线同样存在，非本次引入，未越界修改。

## 四、资源成本

- 解析：预检扩展对每个被提取的 Flate 流增加一次受限分块解压；PepsiCo 503 页热态解析约 6-7 秒（与基线相当，Form 流数量少）。
- 入库：上限放宽不改变 ≤8192 字节页的任何行为；仅使原本失败的 8193-65536 字节页通过。
- 无网络请求、无新增依赖、无全局 monkey patch、无配置写入；所有外部调用走公开 Memory API。

## 五、仍未验证的限制

- 正式 5 份财报（1151 页）中是否存在超过 65536 UTF-8 字节的页：本地仅有 PepsiCo 原文，未验证；若存在，仍会在入库处失败（但已从"必然存在的 8192 门槛"上移）。
- 平台 0/50 的唯一原因未确认：本轮修复的是两个已证实的合同缺陷（其一为检索前硬门槛），不宣称已定位平台唯一原因或已得 50 分。
- 正式质量门槛（每页词项数 90%-110%、证据词召回 0.95、整页 F1 0.95、evidence-page Recall@20 0.70）仍无官方标注可本地验证；既有自建集 Recall@20 14/15 与地理延续页缺口维持原状。
- ToUnicode CMap、ObjStm、字体流、图像流的损坏检测仍保持 pypdf 行为（与 ROUND2 计划的边界一致）；图像流刻意跳过（不参与文本提取）。
- `tests/e2e/test_runtime_server.py` 的断言同步在共享 venv 无法执行（缺 fastapi，该测试不在官方 10 组），仅静态核对参数与消息的一致性。
- 多文档身份检索、跨页证据延续在正式分组下的表现未验证。

## 六、提交状态

- 分支：`feat/pdf-glm-repair`，基于 29fe9553；工作区仅含本轮修改（`src/powercontext/sources/pdf_pages.py`、`src/powercontext/builtin/artifacts/memory/canonical.py`、4 个测试文件、本报告）。
- 提交 SHA：见 git log（本轮单次提交）。
- 推送：仅 `team/feat/pdf-glm-repair`；未合并、未推送 2026-competition、未 force push。
- 诊断资产：`.artifacts/pdf-glm/`（探针与证据脚本、探针 PDF、新旧对比 JSON、官方验收日志；regression-tmp 为 pytest 临时目录不提交）。
