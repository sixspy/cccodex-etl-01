import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class Config:
    SECRET = os.environ.get("APP_SECRET", "cc-etl-lab-dev-secret")
    ADMIN_USER = "admin"
    ADMIN_PASSWORD = "123456"
    LAB_NAME = "抽取转换加载台"
    # 测试钩子（钉死后台时刻、拉长抽取窗口），生产可置 APP_DEV_HOOKS=0 关掉
    DEV_HOOKS = os.environ.get("APP_DEV_HOOKS", "1") == "1"

    @staticmethod
    def database() -> str:
        return os.environ.get("DATABASE_PATH", str(ROOT / "instance" / "etl_lab.db"))
