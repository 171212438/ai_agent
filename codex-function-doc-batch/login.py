from __future__ import annotations

import argparse
import webbrowser

from openai_codex import Codex

def main() -> int:
  parser = argparse.ArgumentParser(description="登录 Codex，供批处理程序复用认证。")
  parser.add_argument(
    "--device-code",
    action="store_true",
    help="使用设备码登录；默认使用浏览器登录。",
  )
  parser.add_argument(
    "--no-open-browser",
    action="store_true",
    help="不要自动打开浏览器。",
  )
  args = parser.parse_args()

  with Codex() as codex:
    account = codex.account()
    if account.account is not None:
      print("当前已经登录 Codex，无需重复登录。")
      print(account.account)
      return 0

    if args.device_code:
      login = codex.login_chatgpt_device_code()
      print("请打开：", login.verification_url)
      print("并输入设备码：", login.user_code)
      if not args.no_open_browser:
        webbrowser.open(login.verification_url)
    else:
      login = codex.login_chatgpt()
      print("请在浏览器中完成登录：", login.auth_url)
      if not args.no_open_browser:
        webbrowser.open(login.auth_url)

    completed = login.wait()
    if not completed.success:
      print("登录失败：", completed.error)
      return 1

  print("登录成功。")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
