
from openai import OpenAI

# 创建 OpenAI 兼容客户端
client = OpenAI()

# 模型可用能力的接口契约
# 向模型声明两个工具：时间和天气
tools = [
  {
    "type": "function",
    "function": {
      "name": "get_current_time",
      "description": "Get the current data and time in a specific timezone",
      "parameters": {
        "type": "object",
        "properties": {
          "timezone": {"type": "string", "description": "Timezone name, e.g. America/Vancouver"}
        },
      },
    },
  },
  {
    "type": "function",
    "function": {
      "name": "get_weather",
      "description": "Get the current weather for a specific city",
      "parameters": {
        "type": "object",
        "properties": {
          "city": {"type": "string", "description": "City name"},
          "units": {"type": "string", "enum": ["celsius", "fahrenheit"]},
        },
      },
    },
  },
]

# 宿主程序的实际执行层
def execute_tool(name, arguments):
  if name == "get_current_time":
    return '{"datatime": "2025-09-13T05:18:47", "day_of_week": "Saturday"}'
  elif name == "get_weather":
    return '{"temperature": 13.2, "unit": "celsius", "conditions": "clear", "humidity": 93}'

# 用户询问温哥华的时间和天气
# Agent 的会话状态和执行轨迹
messages = [
  {"role": "system", "content": "You are a helpful assistant. Use tooles to get real-time information when needed."},
  {"role": "user", "content": "What's the current time and weather in Vancouver?"},
]

while True:  # 形成“决策 → 调用工具 → 获取结果 → 再决策”的循环
  response = client.chat.completions.create(model="gpt-5.4-pro", messages=messages, tools=tools)
  assistant_message = response.choices[0].message
  messages.append(assistant_message)

  if not assistant_message.tool_calls:
    # 模型不再请求工具时，打印答案并结束
    print(assistant_message.content)
    break

  for tool_call in assistant_message.tool_calls:
    # 根据工具名称执行 execute_tool()
    result = execute_tool(tool_call.function.name, tool_call.function.arguments)
    # 使用 tool_call_id 把执行结果关联回原工具请求
    messages.append({"role": "tool", "tool_call_id": tool_call.id, "content": result,})
