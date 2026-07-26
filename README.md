# dtf-dag-generator-aws

這是一個基於 AWS Lambda 的 DAG 產生器專案，目的為依據 S3 中的 table metadata 與 Jinja2 範本，自動產生 Airflow / MWAA 可執行的 DAG Python 檔，並上傳至指定的 S3 路徑供排程系統使用。

## 1. 專案目標

此專案主要用來完成以下流程：

- 由 Lambda 觸發，讀取 S3 上的 table 定義檔
- 依據資料類型（如 tfd、omni、ods）選擇對應模板
- 產生 Airflow DAG 檔案
- 將生成結果上傳至 S3，供 Airflow / MWAA 執行
- 支援 Glue Job 的排程與 partition 處理邏輯

## 2. 主要功能

- 讀取 S3 上的 YAML 設定檔，取得 table 與排程資訊
- 驗證 table name、data type、bucket 名稱是否合法
- 根據模板動態生成 DAG 程式碼
- 支援不同資料型態的 DAG 內容：
  - tfd
  - omni
  - ods
- 於 DAG 中整合 Glue Job 與 S3 清理流程

## 3. 專案結構

```text
dtf-dag-generator-aws/
├── lambda_function.py         # Lambda 入口，負責觸發 DAG 生成
├── gen_dag.py                 # DAG 產生核心邏輯
├── requirements.txt           # Python 相依套件
├── conf/
│   └── dags_args.yaml         # DAG 基本參數與模板對應表
├── airflow_template/
│   ├── ods_template.py
│   ├── omni_template.py
│   └── tfd_template.py       # Jinja2 DAG 模板
├── utils/
│   ├── s3_etl.py              # S3 讀寫邏輯
│   └── validators.py          # 資料驗證與清理工具
├── dtf-dag-generator-aws.json # Lambda / 部署設定檔
└── add_permission.json        # 權限設定檔
```

## 4. 執行流程

1. Lambda 收到事件後，取得事件中的 bucket 名稱。
2. 根據 bucket 前綴判斷對應的 AWS 資源桶。
3. 從 S3 讀取設定檔：
   - configs/crmcplhp03/glue_etl_common_lib/table_info.yaml
4. 呼叫 GenDag 依序產生每張 table 對應的 DAG 檔。
5. 將產生結果寫入指定的 S3 DAG 路徑，供 Airflow / MWAA 使用。

## 5. 核心程式說明

### lambda_function.py

這是 Lambda 的進入點，負責：

- 解析事件中的 bucket 名稱
- 選擇正確的來源 bucket
- 讀取 table_info.yaml
- 呼叫 DAG 生成流程

### gen_dag.py

這是整個專案的核心，負責：

- 讀取 dags_args.yaml 的基本設定
- 更新 table name（例如依據資料型態調整 view 名稱）
- 使用 Jinja2 模板填值，生成 DAG 程式碼
- 將生成好的 DAG 上傳到 S3

### utils/s3_etl.py

提供 S3 的讀寫能力：

- from S3 read YAML config
- upload generated DAG Python files to the target S3 location

### utils/validators.py

負責檔案與輸入值的安全性與合法性驗證：

- 驗證 identifier 是否符合規則
- 驗證 data type 是否為支援類型
- 驗證 bucket 是否為允許清單內
- 清理 log 內容，避免格式問題

## 6. 環境變數

部署時需設定以下環境變數：

- CONF_PATH：設定檔目錄，例如 conf
- TEMPLATE_PATH：模板目錄，例如 airflow_template
- OUTPUT_PATH：DAG 輸出 S3 路徑，例如 s3://bucket/prefix/
- ENV：環境別，例如 dev / test / prod
- ACCOUNT：AWS account id

## 7. 依賴套件

專案使用的 Python 套件如下：

- jinja2==3.0.3
- python-dateutil==2.8.2
- pyyaml==6.0.1
- boto3==1.34.103

可透過以下方式安裝：

```bash
pip install -r requirements.txt
```

## 8. 部署方式

專案已提供部署定義檔：

- dtf-dag-generator-aws.json

此檔案描述 Lambda 的容器映像檔部署方式，以及所需環境變數與執行參數。實際部署時通常會搭配 ECR、CodeBuild / CI/CD 流程一起使用。

## 9. 目前使用注意事項

- 此版本目前未設定 EventBridge 自動觸發 DAG；大多數情況仍需手動執行。
- 只有特定情況下會有排程設定，例如 event_cc_txn_m。
- CFT 匯入完成後，系統會刪除指定 partition；若找不到指定 partition，則會清空整張表資料。

## 10. 使用範例

在完成環境變數設定後，流程會由 Lambda 自動啟動，無需手動呼叫 Python 檔；若要在本地做測試，需先確認 S3 來源 bucket、設定檔路徑與模板路徑都已正確配置。

---

如需進一步擴充，後續可考慮加入：

- 更完整的錯誤處理與日誌分類
- 自動化測試
- DAG 生成前的 dry-run 機制
- 更清楚的排程與手動觸發流程文件