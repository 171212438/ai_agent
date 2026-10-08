"""UserDef 的最小公共接口。"""


def greet(name: str) -> str:
    """生成一句问候语，供 demo.py 或其他 Python 代码调用。"""
    return f"你好，{name}！"
