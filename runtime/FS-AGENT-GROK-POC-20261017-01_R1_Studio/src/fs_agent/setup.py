"""Local, interactive configuration. Secrets never appear in arguments or chat output."""
import getpass
import json
import os
from pathlib import Path

from .config import Config
from .provider import atomic_json


def configure(config: Config, path: Path):
    data = json.loads(path.read_text())
    print("在专用服务账户中配置。直接回车保留原值；密钥不回显，不会自动启动服务。")
    secrets = [("飞书测试 App Secret", config.app_secret_file)]
    if config.provider_kind == "local_qwen_api":
        print("模型使用本机 Qwen API，无需 Grok 登录或 API key。")
    elif config.auth_method == "dedicated_xai_api_key":
        secrets.insert(0, ("专用 xAI API key", config.credential_file))
    else:
        print("Grok 使用专用设备码登录：另运行 service.sh login。")
    for label, target in secrets:
        value = getpass.getpass(label + "（可留空）：").strip()
        if value:
            if not target.is_relative_to(config.root / "secrets") or target.is_symlink():
                raise ValueError("invalid dedicated secret path")
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "w") as stream:
                stream.write(value + "\n")
            target.chmod(0o600)
    feishu = data["feishu"]
    for key, label in (("app_id", "测试 App ID"), ("tenant_key", "tenant_key"), ("bot_open_id", "机器人 open_id"),
                       ("owner_notification_open_id", "示例负责人的 open_id"),
                       ("allowed_open_ids", "准确两名测试用户 open_id，逗号分隔"),
                       ("allowed_department_ids", "允许的 open_department_id，逗号分隔"),
                       ("allowed_test_chat_ids", "测试群 chat_id，逗号分隔，可后补"),
                       ("department_match", "部门规则 any_exact（任一精确匹配）或 all_exact（用户所有部门均允许）")):
        value = input(label + "（留空保留）：").strip()
        if value:
            feishu[key] = [v.strip() for v in value.split(",") if v.strip()] if key.startswith("allowed_") else value
    confirmed = input("确认这是专用测试应用、没有其他事件消费者时输入 YES（留空不变）：").strip()
    if confirmed == "YES":
        feishu["exclusive_test_consumer_confirmed"] = True
    # P1 may run independently. Starting Feishu still requires every live identity and policy.
    data["template_only"] = False
    atomic_json(path, data)
    try:
        ready = Config.load(path)
        ready.check_feishu()
        ready.read_secret(ready.app_secret_file)
        print("飞书配置的本地格式检查通过；仍需开发者后台权限和 P1 真实验证。")
    except Exception:
        print("配置已保存在专用目录；飞书配置尚未齐备，启动时会拒绝连接。")
