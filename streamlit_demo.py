import streamlit as st
import numpy as np
import pandas as pd

print("=== 开始执行本轮脚本 ===")

st.title("平方计算器")

x = st.slider(
    "选择 x",
    min_value=0,
    max_value=10,
    value=3,
)

result = x * x

st.write(f"计算结果：{x}² = {result}")

show_details = st.checkbox("显示计算过程")

if show_details:
    st.write(f"具体计算：{x} × {x} = {result}")

# Add a selectbox to the sidebar:
add_selectbox = st.sidebar.selectbox(
    'How would you like to be contacted?',
    ('Email', 'Home phone', 'Mobile phone')
)

# Add a slider to the sidebar:
add_slider = st.sidebar.slider(
    'Select a range of values',
    0.0, 100.0, (25.0, 75.0)
)

print("=== 本轮脚本执行结束 ===")