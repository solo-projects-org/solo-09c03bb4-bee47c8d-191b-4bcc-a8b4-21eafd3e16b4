# Delta Lake 版本可恢复性分析网页

对象存储误删文件后，湖仓值班员用本工具查清一张 Delta Lake 表还有哪些
版本可以完整回退。值班员在网页上填写版本区间，上传三类材料：

1. `_delta_log` 中留存的 **JSON 提交**（`NNN.json`，20 位补零版本号）；
2. 留存的**经典单文件 Parquet 检查点**（`NNN.checkpoint.parquet`，用 PyArrow 读取）；
3. **现存数据文件的表内相对路径清单**（每行一个路径，如 `data/part-00000-….parquet`）。

工具逐版从 **版本 0 的提交**或**不晚于该版的最新检查点**出发，连续重放
`add` / `remove` 动作到目标版本，再把重建出的快照引用文件与现存清单核对：

- 🟢 **可重建 · 文件齐全**：重放连续无断档，且快照引用的数据文件全部现存；
- 🔴 **缺失提交，重放中断**：明确给出缺失的提交号（版本号列表）；
- 🟠 **重放完整但缺数据文件**：明确给出每个缺失的数据文件表内相对路径。

**仅处理无删除向量（Deletion Vector）的表**：协议特性或任何 add/remove
动作中出现 `deletionVector` 时直接拒绝分析，并指出来源文件。

## 目录结构

```
app/
  analyzer.py            # 核心逻辑：日志解析、逐版重放、文件核对、DV 拦截
  web.py                 # Flask 路由：上传分析 + 本地样例 API
  templates/index.html   # 单页界面（服务端渲染与 JS 渲染共用）
scripts/generate_samples.py  # 用 PyArrow 构造两套样例 Delta 表
samples/                 # 本地样例（健康表 / 误删事故表）
tests/                   # pytest：27 个核心逻辑与路由测试
Dockerfile / docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

页面下半部有两个按钮可**直接加载本地样例**，无需手动选择文件：

- **健康表样例（v0–v4）**：含 v2 检查点，5 个版本全部可重建；
- **误删事故样例（v0–v5）**：v3 提交丢失、数据文件 `part-00004` /
  `part-00006` 被删，但 v2/v4 检查点留存。预期结论：

  | 版本 | 结论 | 说明 |
  |---|---|---|
  | v0–v2 | 🟢 可重建 | v2 起以 `checkpoint-2` 为基线 |
  | v3 | 🔴 缺提交 | 缺提交号 v3 |
  | v4 | 🟠 缺数据文件 | 缺 `data/part-00004-incident.parquet`（v4 检查点仍引用它） |
  | v5 | 🟠 缺数据文件 | 缺 `part-00004` 与 `part-00006` |

  目录中还放了一个分片检查点残片，工具会明确列为“已忽略：仅支持经典
  单文件检查点”。

## 手工上传分析

1. 填写“起始版本 / 结束版本”；
2. 多选上传 `_delta_log` 里留存的 `.json` 与 `.checkpoint.parquet`
   （文件名必须保持 20 位补零版本号，上传时带不带目录前缀都可以）；
3. 上传现存数据文件清单（支持 `#` 注释、空行、`./` 前缀、URI 编码路径，
   两侧路径会做同一归一化）；不上传清单则所有引用文件都判为缺失；
4. 提交后得到总览统计、输入材料盘点和逐版核对明细（可展开查看每个
   引用文件的 ✓ / ✗ 状态）。

也可以只上传检查点：对不晚于检查点版本的目标版本，直接以检查点中的
add 列为快照基线；目标版本更新时，则从检查点版本 +1 起连续重放提交，
中途缺任一提交即报缺失提交号。检查点里的 remove 是墓碑，不属于快照。

## 本地开发（无 Docker 时）

```bash
python -m venv --without-pip .venv   # 系统无 ensurepip 时
python .venv/get-pip.py              # 再引导 pip
pip install -r requirements.txt
python -I scripts/generate_samples.py
gunicorn --bind 0.0.0.0:8000 app.web:app
python -I -m pytest tests -q
```

## 设计取舍

- **基线选择**：优先使用不晚于目标版本的最新检查点（重放距离最短），
  无检查点时才要求版本 0 的提交；
- **逐版独立重放**：每个目标版本都从基线完整重放一次，缺提交不会污染
  其他版本的结论，报告中的“重放起点”对每版透明；
- **只认经典单文件检查点**：分片检查点（`N.checkpoint.K.M.parquet`）
  与 `_last_checkpoint` 指针不参与重放，只在页面登记忽略原因；
- **DV 硬拦截**：删除向量会让“文件存在 ≠ 行数据完整”，超出本工具
  基于文件清单的核对能力，因此在协议层和动作层双重检测并拒绝。
