"""通过 userdef 的命令行入口执行的演示脚本。"""

from userdef import greet


def main() -> None:
    print(greet("UserDef"))
    print(f"__name__ = {__name__}")


if __name__ == "__main__":
    main()
