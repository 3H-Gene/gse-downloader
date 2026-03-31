---
name: gse-downloader
version: 1.1.0
description: |
  企业级 GEO 数据批量下载与数据画像工具，专为生物信息学研究人员设计。
  支持标准化 JSON 输入（兼容 geo-search-skill 输出）、多路径智能下载策略、
  断点续传、MD5/SHA256 完整性校验、数据结构化 Profiling（基础统计）、
  全流程 Pipeline（download→verify→profile）、metadata 本地缓存、
  组学类型自动识别（RNA-seq/scRNA-seq/ATAC-seq/ChIP-seq/Microarray 等）和多维度统计。
  当用户提到 GEO 数据下载、GSE 数据集、NCBI GEO、生物信息学数据获取时，使用此 skill。
description_zh: "GEO 数据获取与数据画像（搜索、断点续传、校验、档案、Profiling、Pipeline）"
description_en: "GEO data acquisition & profiling (search, resume, checksum, archive, profiling, pipeline)"
homepage: https://github.com/3H-Gene/gse-downloader
metadata:
  openclaw:
    emoji: '🧬'
    requires:
      tools: ['python', 'pip']
    install: |
      pip install "git+https://github.com/3H-Gene/gse-downloader.git" --quiet
    entry: gse-downloader
  security:
    credentials_usage: |
      This skill uses NCBI E-utilities API (https://eutils.ncbi.nlm.nih.gov).
      An optional NCBI API key can be configured in config.toml to increase rate limits.
      No credentials are stored or transmitted to any third party.
    allowed_domains:
      - eutils.ncbi.nlm.nih.gov
      - ftp.ncbi.nlm.nih.gov
      - www.ncbi.nlm.nih.gov
---

# GSE Downloader Skill

## 简介

GSE Downloader 是一个企业级 GEO 数据获取（Data Acquisition）与数据画像（Data Profiling）工具。

**v1.1.0 新特性：**
- 🔗 **标准化 JSON 输入**：兼容 `geo-search-skill` 输出，直接管道传入
- 🛣️ **多路径下载策略**：SOFT → Series Matrix → Supplementary，SRA 仅按需显式启用
- 📊 **Profiling 模块**：结构标准化 + 基础统计（sample_count/gene_count/missing_rate/sparsity）
- ⚡ **Pipeline 一键执行**：`download → verify → profile` 链式运行
- 💾 **Metadata 缓存**：本地 JSON 缓存，TTL 72h，避免重复 API 调用

## 功能

- 🔍 **GEO 搜索**: 关键词搜索 NCBI GEO 数据库，返回结构化结果
- 📋 **元数据查询**: 快速查看任意 GSE 的详细信息（本地或在线）
- 🔄 **断点续传**: 网络中断后自动恢复下载，支持 `--force` 强制重下
- ✅ **完整性校验**: MD5/SHA256 校验确保数据准确
- 📊 **Profiling**: 2-D 矩阵结构化 + 基础统计（不修改表达量值）
- ⚡ **Pipeline**: download→verify→profile 全流程一键
- 📋 **数据档案**: 完整的 GSE 数据档案，含元数据、样本信息
- 📈 **多维度统计**: 按物种、组学类型等维度统计分析
- 🧬 **组学识别**: 自动识别 RNA-seq、Microarray、ATAC-seq 等
- ⚙️ **配置向导**: 一键生成 config.toml

## 使用方法

### 命令行

```bash
# ── 初始化配置（首次使用推荐）──
gse-downloader init
gse-downloader init --output /data/geo --config my_config.toml

# ── 搜索 GEO 数据集 ──
gse-downloader search "breast cancer RNA-seq"
gse-downloader search "Alzheimer scRNA-seq" --limit 20
gse-downloader search "ATAC-seq mouse" --format json

# ── 查看元数据（不下载）──
gse-downloader info GSE134520
gse-downloader info GSE134520 --local        # 仅查看本地档案
gse-downloader info GSE134520 --format json  # JSON 输出

# ── 下载数据集 ──
gse-downloader download GSE123456
gse-downloader download GSE123456 --force    # 强制重新下载
gse-downloader download GSE123456 --no-progress  # 无进度条

# ── 查看下载状态 ──
gse-downloader status GSE123456

# ── 查看数据档案 ──
gse-downloader archive GSE123456
gse-downloader archive GSE123456 --format json

# ── 数据规范化 ──
gse-downloader format GSE123456

# ── 统计报告 ──
gse-downloader stats
gse-downloader stats --by organism
gse-downloader stats --by omics_type

# ── 批量下载 ──
gse-downloader batch gse_list.txt
gse-downloader batch gse_list.txt --retry 3 --report report.json

# ── 校验数据完整性 ──
gse-downloader verify GSE123456
gse-downloader verify --all

# ── 数据 Profiling（v1.1.0+）──
gse-downloader profile GSE123456          # 结构统计
gse-downloader profile GSE123456 --json   # JSON 输出

# ── 全流程 Pipeline（v1.1.0+）──
gse-downloader pipeline GSE123456                         # 单个数据集
gse-downloader pipeline '{"gse_id":"GSE123456","omics_type":"RNA-seq"}'  # JSON 输入
gse-downloader pipeline gse_list.json                     # 批量 JSON 文件
gse-downloader pipeline GSE123456 --force                 # 强制重下
gse-downloader pipeline GSE123456 --sra                   # 显示 SRA 运行号
gse-downloader pipeline GSE123456 --no-profile            # 仅下载+校验
gse-downloader pipeline GSE123456 --json                  # JSON 格式输出

# ── 版本 ──
gse-downloader --version
```

### Python API

```python
from gse_downloader import (
    GSEDownloader,
    GEOQuery,
    ArchiveProfile,
    FormatterFactory,
    OmicsType,
    # v1.1.0 新增
    Pipeline,
    DataProfiler,
    MetadataCache,
    parse_input,
    GseInput,
)

# ── 搜索 GEO ──
geo = GEOQuery()
hits = geo.search_series_detailed("lung cancer RNA-seq", retmax=10)
for hit in hits:
    print(f"{hit['gse_id']}: {hit['title']} ({hit['sample_count']} samples)")

# ── 查询 GSE 详情（在线）──
series = geo.get_series_info("GSE134520")
print(series.title, series.series_type, series.organism)

# ── 验证 GSE ID 格式和存在性 ──
ok, err = geo.validate_gse_id("GSE134520")

# ── 多路径下载文件列表（v1.1.0）──
files = geo.get_series_files_by_strategy("GSE134520", omics_hint="RNA-seq")
sra_files = geo.get_series_files_by_strategy("GSE134520", include_sra=True)

# ── 标准化输入解析（v1.1.0）──
# 支持字符串、JSON、列表、文件路径
inputs = parse_input("GSE134520")
inputs = parse_input({"gse_id": "GSE134520", "omics_type": "RNA-seq"})
inputs = parse_input(["GSE1", "GSE2", "GSE3"])
inputs = parse_input("gse_list.json")  # Path 对象或字符串路径

# ── 全流程 Pipeline（v1.1.0）──
result = Pipeline().run("GSE134520")
print(result.summary)
# 支持 JSON 输入（兼容 geo-search-skill）
result = Pipeline().run({"gse_id": "GSE134520", "omics_type": "RNA-seq", "sample_count": 10})
# 批量
results = Pipeline().run_batch(["GSE1", "GSE2", "GSE3"])

# ── 数据 Profiling（v1.1.0）──
from pathlib import Path
pr = DataProfiler().profile(Path("./gse_data/GSE134520"))
print(f"Genes: {pr.stats.gene_count}, Samples: {pr.stats.sample_count}")
print(f"Missing: {pr.stats.missing_rate:.4f}, Sparsity: {pr.stats.sparsity:.4f}")

# ── Metadata 缓存（v1.1.0）──
cache = MetadataCache(ttl_hours=72)
cached = cache.get("GSE134520")   # None if not cached
cache.set("GSE134520", series.__dict__)
cache.stats()  # {"total": 1, "stale": 0, "fresh": 1}

# ── 下载 ──
with GSEDownloader(output_dir="./data", rate_limit=2.0) as dl:
    files = dl.get_gse_files("GSE134520")
    results = dl.download_gse("GSE134520", files)
    for name, r in results.items():
        print(f"{name}: {'OK' if r.success else r.error}, {r.avg_speed/1e6:.1f} MB/s")

# ── 读取本地档案 ──
profile = ArchiveProfile.from_json("./data/GSE134520/archive.json")
schema = profile.schema
print(schema.sample_count, schema.omics_type, schema.tissues)

# ── 数据规范化 ──
formatter = FormatterFactory.get(OmicsType.RNA_SEQ)
result = formatter.format("./data/GSE134520")
print(result.expression_matrix, result.metadata_file)
```

## 数据档案字段

每个 GSE 数据集下载后会生成 `archive.json`，包含：

| 字段 | 说明 |
|-----|------|
| `gse_id` | GSE 编号 |
| `title` | 研究标题 |
| `summary` | 研究摘要 |
| `omics_type` | 组学类型 |
| `organisms` | 物种列表 |
| `tissues` | 组织类型 |
| `diseases` | 疾病类型 |
| `sample_count` | 样本数量 |
| `samples` | 详细样本信息（GSM ID、title、organism、source_name 等）|
| `series_type` | 系列类型 |
| `references.pubmed_ids` | 关联文献 |
| `platform` | 测序/芯片平台 |
| `submission_date` | 提交日期 |
| `status` | 下载状态 |

## 组学类型

自动识别以下组学类型：

| 类型标识 | 说明 |
|---------|------|
| `rna_seq` | RNA 测序 |
| `mirna_seq` | miRNA 测序 |
| `atac_seq` | ATAC-seq 染色质可及性 |
| `chip_seq` | ChIP-seq 蛋白结合 |
| `methylation_array` | 甲基化芯片 |
| `methylation_seq` | 甲基化测序 |
| `scrna_seq` | 单细胞 RNA-seq |
| `microarray` | 基因表达芯片 |
| `wgs` | 全基因组测序 |
| `wes` | 全外显子组测序 |
| `proteomics` | 蛋白质组学 |
| `other` | 其他 |

## 安装

### 方式一：从 GitHub 直接安装（推荐，普通用户）

```bash
pip install "git+https://github.com/3H-Gene/gse-downloader.git"
```

### 方式二：使用 Conda / Mamba（HPC / 生产环境）

```bash
git clone https://github.com/3H-Gene/gse-downloader.git
cd gse-downloader
mamba env create -f environment.yml
conda activate gse_downloader
pip install .
```

### 方式三：开发者模式

```bash
git clone https://github.com/3H-Gene/gse-downloader.git
cd gse-downloader
pip install -e ".[dev]"
```

## 依赖

- Python >= 3.10
- requests >= 2.28.0
- pandas >= 2.0.0
- loguru >= 0.7.0
- typer >= 0.9.0
- rich >= 13.0.0
- tqdm >= 4.65.0
- httpx >= 0.24.0
- jsonschema >= 4.0.0

## 目录结构

```
output/
└── GSE123456/
    ├── archive.json              # 数据档案（元数据）
    ├── download_state.json       # 下载状态
    ├── GSE123456_family.soft.gz  # SOFT 格式元数据
    ├── GSE123456_series_matrix.txt.gz  # 表达矩阵
    ├── raw/                      # 原始文件（format 后）
    │   └── GSE123456_RAW.tar
    └── processed/                # 规范化数据（format 后）
        └── expression_matrix.csv
```

## 配置文件 (config.toml)

```toml
[download]
output_dir = "./gse_data"
max_workers = 4
timeout = 300
verify_ssl = true
retry_times = 3
auto_resume = true
rate_limit = 2.0          # NCBI 推荐 ≤ 3/s

[checksum]
enabled = true
algorithm = "md5"         # md5 或 sha256

[ncbi]
email = "your@email.com"  # NCBI 推荐提供邮箱
# api_key = "your_key"   # 有 API key 可提高速率限制

[logging]
level = "INFO"
# log_dir = "./logs"
```

## 快速上手

```bash
# 1. 初始化配置
gse-downloader init

# 2. 搜索感兴趣的数据集
gse-downloader search "human liver cancer RNA-seq" --limit 5

# 3. 查看目标数据集详情
gse-downloader info GSE134520

# 4. 下载
gse-downloader download GSE134520

# 5. 查看档案
gse-downloader archive GSE134520

# 6. 数据规范化
gse-downloader format GSE134520
```

## License

MIT
