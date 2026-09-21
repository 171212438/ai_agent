# -*- coding: utf-8 -*-
# Streamlit Basic concepts：单文件综合示例。
# 参考：https://docs.streamlit.io/get-started/fundamentals/main-concepts
# 安装：python -m pip install -r requirements.txt
# 启动：python -m streamlit run app.py
# 请勿把文件命名为 streamlit.py、pandas.py 或 numpy.py，以免遮蔽第三方库。

import time

import numpy as np
import pandas as pd
import streamlit as st


# 01. 页面配置：宽屏 + 左侧控制面板。
# 主题配色在可选文件 .streamlit/config.toml 中；不使用它也能运行。
st.set_page_config(
    page_title="Streamlit 基础概念实验室",
    layout="wide",
    initial_sidebar_state="expanded",
)


# 02. 示例数据：固定种子保证样本可复现；缓存避免重复生成。
# 缓存和固定随机种子是两件事。本例数据很小，使用缓存主要为了演示。
@st.cache_data(show_spinner=False, max_entries=16)
def create_demo_data(
    seed: int,
) -> tuple[np.ndarray, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """生成矩阵、带列名的表格、折线图数据和模拟地理坐标。"""
    print(f"[cache miss] 生成示例数据，seed={seed}")
    rng = np.random.default_rng(seed)

    matrix = rng.standard_normal((10, 20))
    styled_data = pd.DataFrame(
        matrix.copy(),
        columns=[f"col {i}" for i in range(20)],
    )
    chart_data = pd.DataFrame(
        rng.standard_normal((20, 3)),
        columns=["a", "b", "c"],
    )
    map_data = pd.DataFrame(
        rng.normal(loc=[37.76, -122.4], scale=[0.02, 0.02], size=(1000, 2)),
        columns=["lat", "lon"],
    )
    return matrix, styled_data, chart_data, map_data


def count_click() -> None:
    """按钮回调：先更新状态，再由 Streamlit 从头执行脚本。"""
    st.session_state["click_count"] += 1
    next_run = st.session_state["run_count"] + 1
    st.session_state["last_callback"] = (
        f"按钮回调已执行；随后进入第 {next_run} 轮脚本。"
    )
    print(f"[callback] 点击次数 +1；即将进入第 {next_run} 轮脚本")


# 03. 会话状态只在不存在时初始化，避免重跑时清零。
initial_state = {
    "run_count": 0,
    "click_count": 0,
    "last_callback": "尚未触发按钮回调。",
    "task_runs": 0,
    "task_summary": None,
}
for state_key, initial_value in initial_state.items():
    if state_key not in st.session_state:
        st.session_state[state_key] = initial_value

# 这是本次浏览器会话的脚本执行次数，包含首次运行。
st.session_state["run_count"] += 1
print(f"[script] 开始第 {st.session_state['run_count']} 轮执行")


# 04. Sidebar：把全局控制放在左侧。
st.sidebar.title("控制面板")
st.sidebar.caption("操作控件后，可观察下方的脚本执行次数。")
seed = st.sidebar.number_input(
    "随机种子",
    min_value=0,
    max_value=999999,
    value=42,
    step=1,
    key="seed",
    help="同一种子生成相同样本；修改种子后生成另一组样本。",
)
contact = st.sidebar.selectbox(
    "联系偏好（仅示例）",
    ("Email", "Home phone", "Mobile phone"),
    key="contact",
)
value_range = st.sidebar.slider(
    "选择数值范围",
    min_value=0.0,
    max_value=100.0,
    value=(25.0, 75.0),
    step=1.0,
    key="value_range",
)
st.sidebar.caption("联系偏好和范围仅在“页面布局”中回显，不发送消息或筛选数据。")
st.sidebar.divider()
st.sidebar.metric("本会话脚本执行次数", st.session_state["run_count"])
st.sidebar.caption("计数包含首次执行，不代表按钮点击次数。")
st.sidebar.caption("默认标签页切换和展开详情不会触发重跑；输入控件的值变更会。")


# 05. 准备各区域共用的数据。
basic_df = pd.DataFrame(
    {"first column": [1, 2, 3, 4], "second column": [10, 20, 30, 40]}
)
matrix, styled_data, chart_data, map_data = create_demo_data(int(seed))

st.title("Streamlit 基础概念实验室")
st.caption("同一份 Python 脚本 · 六个演示区 · 数据展示、交互、布局与执行过程")

# 默认 tabs 是显示分组，不是按需执行。
# 每次重跑会执行全部 with 代码块，因此耗时任务必须放在按钮条件内部。
(
    tab_display,
    tab_tables,
    tab_charts,
    tab_widgets,
    tab_layout,
    tab_progress,
) = st.tabs(
    ["01 数据展示", "02 表格样式", "03 图表地图", "04 交互控件", "05 页面布局", "06 进度与执行"]
)


# 06. Magic 与 st.write：同样的数据，两种展示写法。
with tab_display:
    st.subheader("Magic 与 st.write")
    st.caption("左侧使用自动展示，右侧显式调用；两边展示相同的数据。")
    left, right = st.columns(2, gap="medium")

    with left, st.container(border=True):
        st.markdown("#### Magic：单独写一行")
        # 下面的字符串和 basic_df 就是真正的 Magic 演示，请勿删掉。
        # 该字符串不在文件或函数的开头，不是被忽略的 docstring。
        """**示例数据表**：两列数据，四行记录。"""
        basic_df
        st.code('"""**示例数据表**：两列数据，四行记录。"""\nbasic_df', language="python")

    with right, st.container(border=True):
        st.markdown("#### st.write：显式调用")
        st.write("**示例数据表**：两列数据，四行记录。")
        st.write(basic_df)
        st.code('st.write("**示例数据表**：两列数据，四行记录。")\nst.write(basic_df)', language="python")

    st.caption("basic_df = ... 负责创建数据；单独写 basic_df 或 st.write(basic_df) 才负责展示。")
    with st.expander("阅读提示：Magic 的作用范围"):
        st.write("本文件由 streamlit run 作为入口运行，Magic 才会生效；导入模块中的裸表达式不适用。")
        st.write("Magic 必须启用。完整工程的 config.toml 已设置 runner.magicEnabled = true。")


# 07. 数组、DataFrame、Styler 和静态表格。
with tab_tables:
    st.subheader("给表格选择展示方式和样式")
    st.caption("完整数据为 10 行 × 20 列；高亮的是每一列的最大值。")
    with st.container(border=True):
        st.markdown("#### 交互表格 + Pandas Styler")
        styled_view = styled_data.style.format("{:.2f}").highlight_max(
            axis=0,
            props="background-color: #DBEAFE; color: #172B4D; font-weight: bold;",
        )
        st.dataframe(styled_view, width="stretch", height=385)

    left, right = st.columns(2, gap="medium")
    with left, st.expander("直接展示 NumPy 数组"):
        st.caption("不必先转换成 DataFrame；这里仍是完整的 10 × 20 数组。")
        st.dataframe(matrix, width="stretch", height=240)
    with right, st.expander("使用 st.table 展示静态表格"):
        st.caption("为了让静态表格便于阅读，仅展示前 5 行、前 5 列。")
        st.table(styled_data.iloc[:5, :5].style.format("{:.2f}"))
    st.caption("交互表格可排序和滚动，不等于可以编辑；这里没有使用 st.data_editor。")


# 08. 图表与地图：所有点都是本地生成的模拟数据。
with tab_charts:
    st.subheader("同一类 Python 数据，可以画成图表或地图")
    left, right = st.columns(2, gap="medium")
    with left, st.container(border=True):
        st.markdown("#### 折线图：st.line_chart")
        st.caption("20 个样本，a / b / c 三条曲线。")
        st.line_chart(chart_data, height=320, width="stretch")
    with right, st.container(border=True):
        st.markdown("#### 地图：st.map")
        st.caption("旧金山附近的 1,000 个模拟坐标，并非真实采集记录。")
        st.map(map_data, latitude="lat", longitude="lon", height=320)
    st.caption("地图底图由外部服务提供；浏览器无法访问底图服务时，底图可能空白。")

    left, right = st.columns(2, gap="medium")
    with left, st.expander("补充：同一份数据绘制柱状图"):
        st.bar_chart(chart_data, height=260, width="stretch")
    with right, st.expander("查看地图坐标的前 10 行"):
        st.dataframe(map_data.head(10), width="stretch", hide_index=True)


# 09. 输入控件：返回值仍然是普通 Python 值。
with tab_widgets:
    st.subheader("网页操作 → Python 变量 → 页面结果")
    left, right = st.columns(2, gap="medium")
    with left, st.container(border=True):
        st.markdown("#### 滑块：返回一个整数")
        x = st.slider("选择 x", 0, 20, 3, key="square_x")
        st.metric("x 的平方", x * x)
        st.write(f"本轮计算：{x} × {x} = {x * x}")
    with right, st.container(border=True):
        st.markdown("#### 文本框：用 key 读取状态")
        st.text_input("你的名字", key="name", placeholder="例如：Python 学习者")
        st.caption("输入后按 Enter 或移开焦点，提交给 Python。")
        st.json({"st.session_state['name']": st.session_state["name"]})

    left, right = st.columns(2, gap="medium")
    with left, st.container(border=True):
        st.markdown("#### 下拉框：从一列数据中选一个值")
        option = st.selectbox("选择一个数字", basic_df["first column"], key="number")
        # Magic 也能展示一行中的多个值。
        "当前选中：", option
    with right, st.container(border=True):
        st.markdown("#### 复选框：控制是否执行展示语句")
        if st.checkbox("显示折线图原始数据", key="show_dataframe"):
            st.dataframe(chart_data, width="stretch", height=230)
        else:
            st.caption("勾选后执行 st.dataframe；取消勾选后，本轮不再生成该表格。")


# 10. Columns + Sidebar + Expander，并补充可观察的按钮回调。
with tab_layout:
    st.subheader("把内容放进不同的容器")
    st.caption("左侧 Sidebar 放控制项；主区域通过 columns 并排排列。")
    left, right = st.columns(2, gap="medium")
    with left, st.container(border=True):
        st.markdown("#### 左列：按钮与回调")
        # 这里只传函数对象 count_click，不要写成 count_click()。
        st.button("点击计数 +1", on_click=count_click, key="count_button", type="primary")
        st.metric("累计点击次数", st.session_state["click_count"])
        st.caption(st.session_state["last_callback"])
    with right, st.container(border=True):
        st.markdown("#### 右列：单选按钮")
        house = st.radio(
            "选择一个学院",
            ("Gryffindor", "Ravenclaw", "Hufflepuff", "Slytherin"),
            key="house",
        )
        st.write("当前选择：", house)

    with st.expander("查看侧边栏控件的返回值", expanded=True):
        st.json({"联系偏好": contact, "范围下限": value_range[0], "范围上限": value_range[1]})
    st.caption("with left: 表示把输出放到左列，不表示并行执行；展开或折叠默认只改变可见性。")


# 11. Progress + Empty：在一次脚本执行中更新同一个位置。
with tab_progress:
    st.subheader("观察重跑、缓存与执行进度")
    left, right = st.columns([3, 2], gap="medium")
    with left, st.container(border=True):
        st.markdown("#### 模拟耗时任务")
        st.caption("100 步，每步模拟等待 0.02 秒；实际耗时还包含界面更新开销。")
        if st.button("开始模拟任务", key="start_task", type="primary"):
            # 清除旧结果；只有整个循环结束，才记录“完成”。
            st.session_state["task_summary"] = None
            started_at = time.perf_counter()
            latest_iteration = st.empty()
            bar = st.progress(0)
            for step in range(1, 101):
                time.sleep(0.02)
                latest_iteration.text(f"已完成 {step} / 100")
                bar.progress(step)
            st.session_state["task_runs"] += 1
            st.session_state["task_summary"] = {
                "run": st.session_state["task_runs"],
                "seconds": time.perf_counter() - started_at,
            }

        # 展示放在按钮条件之外，其他控件引发重跑后也能看见已完成结果。
        summary = st.session_state["task_summary"]
        if summary is not None:
            st.success(f"第 {summary['run']} 次任务完成，用时 {summary['seconds']:.2f} 秒。")
        else:
            st.caption("尚无已完成结果。仅点击按钮才执行任务；切换标签页不会启动任务。")

    with right, st.container(border=True):
        st.markdown("#### 对照终端输出")
        st.write("**[script]**：每次从头执行脚本都会打印。")
        st.write("**[callback]**：计数按钮的回调，先于新一轮脚本打印。")
        st.write("**[cache miss]**：未命中缓存，真正进入数据生成函数时才打印。")
        st.caption("拖动 x：脚本重跑，但相同种子的缓存通常命中。修改随机种子：生成对应的新样本。")

    with st.expander("阅读提示：容易混淆的几件事"):
        st.write("进度条的一百次更新发生在同一轮脚本内，不是一百次整页重跑。")
        st.write("本例用 time.sleep 模拟同步任务，不是后台任务；演示运行时请等待其完成。")
        st.write("缓存复用函数结果；Session State 保存本会话的点击次数和任务结果，两者用途不同。")
        st.write("会话状态不是持久化存储，刷新页面或重新建立会话时可能重置。")
        st.write("保存源码后可通过页面的重跑提示更新；开发时可启用 Always rerun。")

st.divider()
st.caption("阅读顺序：页面配置 → 数据函数 → 状态初始化 → 侧边栏 → 六个演示区。")
print(f"[script] 第 {st.session_state['run_count']} 轮执行结束")
