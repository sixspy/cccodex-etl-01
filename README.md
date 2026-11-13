# cc-etl-lab

CC_Codex 二期（26.11）**数据工程**产品线：**抽取转换加载台**（FastAPI + Vue 3 SPA + SQLite）。

与 `cc-data-lab`（Flask + HTMX 采集控制台）错开：本线面向 **ETL 管道编排 / 抽取落库**，前端为 Vue 单页，暖色仓运风格。

## 领域

- **所属领域**：数据工程  
- **子方向**：抽取-转换-加载 / 数据采集与落库  

## 技术栈

| 层 | 选型 |
|----|------|
| 后端 | Python 3.12、FastAPI、Uvicorn、SQLite |
| 前端 | Vue 3（CDN Composition）、双栏管道台 |
| 风格 | 暖琥珀仓运底、圆角强调、等宽源地址 |
| 测试 | pytest + httpx |

## 基线能力

登录、管道列表、管道详情、执行抽取、执行落库。后续单轮题在此叠加增量水位、清洗口径、并发争用、跨日重算等。

## 增量水位（etl-01 叠加）

- **口径设置**：本日只许钉死一种水位推进法并写入说明——`源端最大变更时刻`（跟着源端最大变更时刻往前走）或 `上次成功落库条数`（跟着本管道上次成功落库条数往前走）。写死后当日不得改弦；抽取进行中禁止改推进说明。
- **水位裁定**：水位怎么推进由服务端裁定。时刻法在抽取时推到所收非空编码行的最大变更时刻；条数法在落库时按成功落库的非空编码条数往前走。
- **抽取**：点抽取须携带推进法，与说明对不上返回「水位推进写歪了」，看板同步点亮。增量抽取只收水位之后的变更行；全量抽取可越过水位但必须写明原因；编码空的行不推动水位。
- **争抢**：同一管道同时抽取只成一笔，另一侧记「抽取争用失败」（详情可展开）。
- **看板**：管道总览并排水位值、今日已抽、落库条数、推进法名称；抽取详情可展开水位轨迹与抽取记录。
- **测试钩子**（`APP_DEV_HOOKS=0` 关闭）：`POST /api/dev/clock` 钉死后台时刻、`DELETE /api/dev/clock` 恢复；`POST /api/dev/extract-delay` 拉长抽取窗口以复现争抢。`APP_SEED_SOURCE=0` 可关掉 mock 源行种子。

## 本地

```powershell
cd claude-workspace\cc-etl-lab
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest
python run.py
```

http://127.0.0.1:6100/login · **admin / 123456**（宿主机勿用 6000，浏览器会拦）

## Docker

```powershell
docker compose up -d --build
```

端口 **6000**。冒烟：`python ..\scripts\_smoke_lab_access.py --dir .`
