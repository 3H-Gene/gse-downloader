---
name: gse-downloader
description: "Enterprise-grade GEO data batch downloader for bioinformatics. Supports keyword search, resume download, checksum verification, omics-type detection, and statistics."
description_zh: "企业级 GEO 数据批量下载工具，支持关键词搜索、断点续传、完整性校验、组学类型识别和多维度统计"
description_en: "Enterprise-grade GEO data batch downloader with resume, checksum, omics detection and statistics"
---

# GSE Downloader Skill

## 简介

GSE Downloader 是一个企业级 GEO 数据批量下载工具，专为生物信息学研究人员设计。支持关键词搜索、断点续传、数据完整性校验、数据档案管理和多维度统计。

## 功能

- 🔍 **GEO 搜索**: 关键词搜索 NCBI GEO 数据库，返回结构化结果
- 📋 **元数据查询**: 快速查看任意 GSE 的详细信息（本地或在线）
- 🔄 **断点续传**: 网络中断后自动恢复下载，支持 `--force` 强制重下
- ✅ **完整性校验**: MD5/SHA256 校验确保数据准确
- 📊 **数据档案**: 完整的 GSE 数据档案，含元数据、样本信息
- 📈 **多维度统计**: 按物种、组学类型等维度统计分析
- 🧬 **组学识别**: 自动识别 RNA-seq、Microarray、ATAC-seq 等
- 📦 **Conda 环境**: 开箱即用的 Conda 环境配置
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

### 方式一：使用 Conda（推荐）

```bash
mamba env create -f environment.yml
conda activate gse_downloader
pip install -e .
```

### 方式二：直接安装

```bash
pip install -e .
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
