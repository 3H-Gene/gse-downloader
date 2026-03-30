# GSE Downloader

企业级 GEO 数据批量下载工具，支持断点续传、数据完整性校验、组学类型规范化和多维度统计。

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![WorkBuddy Skill](https://img.shields.io/badge/WorkBuddy-Skill-brightgreen.svg)](https://github.com/3H-Gene/gse-downloader/releases/latest)

---

## 🤖 WorkBuddy / OpenClaw Skill 安装

> **不想手动敲命令？直接把这个工具作为 AI Skill 加载，让 AI 帮你下数据。**

### WorkBuddy

1. 进入 [Releases 页面](https://github.com/3H-Gene/gse-downloader/releases/latest)，下载 `gse-downloader-skill.zip`
2. WorkBuddy → 左侧「技能」→「从文件安装」→ 选择 zip 文件
3. 对话中直接说："帮我下载 GSE134520 的数据" 即可

### OpenClaw

在 OpenClaw 项目中，对话里说：

> "用 gse-downloader 帮我搜索 lung cancer RNA-seq 数据集"

OpenClaw 会自动调用 SKILL.md 中定义的工具指令。

---

## ✨ 特性

| 特性 | 说明 |
|------|------|
| **断点续传** | 网络中断后自动恢复，HTTP Range 请求续传 |
| **完整性校验** | MD5 / SHA256 校验，保证数据准确性 |
| **状态管理** | 自动检测 `not_started / incomplete / completed / invalid` |
| **数据档案** | 自动生成 `archive.json`，包含元数据、样品信息、组学类型 |
| **数据规范化** | `format` 命令将数据整理为标准化目录结构和 expression_matrix.csv |
| **批量下载** | 支持批量 ID 文件，含失败重试和汇总报告 |
| **速率限制** | Token-bucket 限速，默认 2 req/s（NCBI 友好） |
| **多维统计** | 按物种、组学类型统计所有已下载数据集 |

---

## 🚀 安装

### 使用 Conda / Mamba（推荐）

```bash
# 创建并激活环境
mamba env create -f environment.yml
conda activate gse_downloader

# 以开发模式安装
pip install -e .
```

### 使用 pip

```bash
pip install -e .
# 或安装后
pip install gse-downloader
```

### 验证安装

```bash
gse-downloader --version
```

---

## 📖 使用指南

### 下载单个 GSE 数据集

```bash
# 基本下载（自动续传）
gse-downloader download GSE123456

# 指定输出目录
gse-downloader download GSE123456 --output /data/geo

# 使用配置文件
gse-downloader download GSE123456 --config config.toml

# 关闭进度条（日志模式）
gse-downloader download GSE123456 --no-progress
```

### 查看下载状态

```bash
gse-downloader status GSE123456
```

输出示例：

```
┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
┃ Property  ┃ Value               ┃
┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
│ GSE ID    │ GSE123456           │
│ Status    │ completed           │
│ Started   │ 2026-03-27 10:00:00 │
│ Completed │ 2026-03-27 10:05:00 │
│ Progress  │ 100.0%              │
│ Files     │ 2/2                 │
└───────────┴─────────────────────┘
```

### 查看数据档案

```bash
# 表格格式
gse-downloader archive GSE123456

# JSON 格式
gse-downloader archive GSE123456 --format json
```

### 数据规范化（format）

将下载的原始数据整理为标准化目录结构：

```bash
gse-downloader format GSE123456
```

**自动完成：**
- 创建 `raw/`、`processed/`、`metadata/` 子目录
- 将 FASTQ/BAM/CEL 等文件移入 `raw/`
- 将 count 矩阵、series matrix 移入 `processed/`
- 解析表达矩阵，生成 `processed/expression_matrix.csv`
- 提取样本元数据，生成 `metadata/metadata.csv`

支持的组学类型：RNA-seq、Microarray、ATAC-seq、ChIP-seq、Methylation、scRNA-seq、WGS/WES

### 完整性校验

```bash
# 校验单个数据集
gse-downloader verify GSE123456

# 校验所有已下载数据集
gse-downloader verify --all

# 指定数据目录
gse-downloader verify --all --output-dir /data/geo
```

### 批量下载

创建 GSE ID 列表文件（`gse_list.txt`）：

```
# 注释行（以 # 开头）会被跳过
GSE123456
GSE789012
GSE345678
```

```bash
# 批量下载（默认重试 1 次）
gse-downloader batch gse_list.txt

# 重试 3 次，并保存报告
gse-downloader batch gse_list.txt --retry 3 --report batch_report.json

# 指定输出目录
gse-downloader batch gse_list.txt --output /data/geo
```

### 统计报告

```bash
# 汇总统计（按物种 + 按组学类型）
gse-downloader stats

# 仅按物种统计
gse-downloader stats --by organism

# 仅按组学类型统计
gse-downloader stats --by omics_type

# 指定数据目录
gse-downloader stats --output-dir /data/geo
```

---

## ⚙️ 配置文件

创建 `config.toml`：

```toml
[download]
output_dir = "./gse_data"   # 数据存储目录
max_workers = 4             # 最大并发数
retry_times = 3             # 失败重试次数
timeout = 300               # 请求超时（秒）
verify_ssl = true           # SSL 证书验证
auto_resume = true          # 自动断点续传
rate_limit = 2.0            # 每秒请求数（NCBI 建议 ≤ 3）

[checksum]
enabled = true
algorithm = "md5"           # "md5" 或 "sha256"

[archive]
generate_json = true        # 下载后自动生成 archive.json
generate_readme = true
```

使用配置文件：

```bash
gse-downloader download GSE123456 --config config.toml
gse-downloader batch gse_list.txt --config config.toml
```

---

## 📁 目录结构

下载并 `format` 后的标准目录结构：

```
gse_data/
└── GSE123456/
    ├── archive.json                  # 数据档案（元数据 + 样本信息）
    ├── .gse_state.json               # 下载状态（内部使用）
    ├── raw/                          # 原始数据文件
    │   ├── *.fastq.gz                # RNA-seq 原始读段
    │   ├── *.CEL.gz                  # Microarray CEL 文件
    │   └── *.bam                     # 比对结果
    ├── processed/                    # 处理后数据
    │   ├── GSE123456_series_matrix.txt.gz
    │   └── expression_matrix.csv     # 统一表达矩阵（基因 × 样本）
    └── metadata/
        └── metadata.csv              # 样本元数据（样品名、组织、条件等）
```

### archive.json 结构

```json
{
  "gse_id": "GSE123456",
  "status": "completed",
  "omics_type": "RNA-seq",
  "sample_count": 12,
  "metadata": {
    "title": "研究标题",
    "summary": "研究摘要...",
    "overall_design": "实验设计描述",
    "series_type": "Expression profiling by high throughput sequencing"
  },
  "organisms": [{"name": "Homo sapiens", "taxid": 9606}],
  "tissues": ["liver", "kidney"],
  "diseases": ["cancer"],
  "samples": [
    {
      "gsm_id": "GSM1234567",
      "title": "Sample 1",
      "source_name": "liver tumor",
      "organism": "Homo sapiens",
      "characteristics": {"tissue": "liver", "disease": "HCC"}
    }
  ],
  "files": [...]
}
```

---

## 🔬 支持的组学类型

| 组学类型 | 识别关键词 / 平台 | 原始文件格式 |
|---------|----------------|------------|
| RNA-seq | HiSeq, NovaSeq, `rna-seq` | `.fastq.gz`, `.bam` |
| Microarray | Affymetrix, Agilent, Illumina array | `.CEL.gz`, `.idat` |
| scRNA-seq | 10x Genomics, Drop-seq, Smart-seq | `.fastq.gz`, `.bam` |
| ATAC-seq | `ATAC`, `chromatin accessibility` | `.fastq.gz`, `.bam` |
| ChIP-seq | `ChIP-seq`, `genome binding` | `.fastq.gz`, `.bam` |
| Methylation | WGBS, RRBS, 450K/850K array | `.fastq.gz`, `.idat` |
| WGS / WES | `whole genome`, `whole exome` | `.fastq.gz`, `.bam` |

---

## 🛠️ 开发

```bash
# 安装开发依赖
pip install -e ".[dev]"

# 运行全部测试
pytest tests/ -v

# 代码格式化 & 检查
black src/
ruff check src/

# 运行单个测试文件
pytest tests/test_formatter.py -v
pytest tests/test_rate_limiter.py -v
```

### 测试覆盖

| 模块 | 测试文件 |
|------|---------|
| Checksum | `tests/test_checksum.py` |
| Formatter | `tests/test_formatter.py` |
| OmicsDetector | `tests/test_omics_detector.py` |
| RateLimiter | `tests/test_rate_limiter.py` |
| StateManager | `tests/test_state_manager.py` |

---

## 📋 已知限制

- 大文件（MINiML tgz > 50MB）可能因国内网络超时失败，属正常现象，可重试
- 部分 GSE 数据集托管于 SRA，需要额外工具（如 `sra-tools`）下载 FASTQ
- GEO FTP 对频繁访问有速率限制，默认限速 2 req/s 可保持稳定

---

## 📄 License

MIT License — 详见 [LICENSE](LICENSE) 文件
