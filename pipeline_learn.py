
import torch
import torch.nn as nn
import math


class PositionalEncoding(nn.Module):
  """
  位置编码模块
  """

  def forward(self, x):
    pass

class MultiHeadAttention(nn.Module):
  """
  多头注意力机制模块
  结构参数：
  模型维度 d_model (512): 每个 token 的向量维度
  注意力头数 num_heads (8): 并行注意力头数量
  单头 Query/Key 维度 d_k (64): 每个头中 Q、K 的维度，d_model / num_heads
  单头 Value 维度 d_v (64): 每个头中 V 的维度，d_k
  Q/K/V 投影矩阵 W_Q/W_K/W_V: 输入映射到 Query/Key/Value，d_model * d_model
  输出投影矩阵 W_O: 多头拼接后再映射回模型维度，d_model * d_model
  """
  def __init__(self, d_model, num_heads):
    super(MultiHeadAttention, self).__init__()  # 调用 nn.module 的初始化逻辑
    assert d_model % num_heads == 0, "d_model must be divisible by num_heads"

    self.d_model = d_model
    self.num_heads = num_heads
    self.d_k = d_model // num_heads

    # 定义 Q, K, V 和输出的线性层变换
    self.W_q = nn.Linear(d_model, d_model)
    self.W_k = nn.Linear(d_model, d_model)
    self.W_v = nn.Linear(d_model, d_model)
    self.W_o = nn.Linear(d_model, d_model)

  def scaled_dot_product_attention(self, Q, K, V, mask=None):
    # 1. 计算注意力得分（QK^T）
    attn_scores = torch.matmul(Q, K.transpose(-2, -1)) / math.sqrt(self.d_k)

    # 2. 应用掩码（如果提供）
    if mask is not None:
      attn_scores = attn_scores.masked_fill(mask == 0, -1e9)

    # 3. 计算注意力权重（Softmax）
    attn_probs = torch.softmax(attn_scores, dim=-1)

    # 4. 加权求和（权重 * V）
    output = torch.matmul(attn_probs, V)
    return output

  def split_heads(self, x):
    # 将输入 x 的形状从（batch_size, seq_length, d_model) 变换为 (batch_size, num_heads, seq_length, d_k)
    batch_size, seq_length, d_model = x.size()
    return x.view(batch_size, seq_length, self.num_heads, self.d_k).transpose(1, 2)

  def combine_heads(self, x):
    # 将输入 x 的形状从 (batch_size, num_heads, seq_lenght, d_k) 变回 (batch_size, seq_length, d_model)
    batch_size, num_heads, seq_length, d_k = x.size()
    return x.transpose(1, 2).contiguous().view(batch_size, seq_length, self.d_model)

  def forward(self, Q, K, V, mask=None):
    # 1. 对 Q, K, V 进行线性变换
    Q = self.split_heads(self.W_q(Q))
    K = self.split_heads(self.W_k(K))
    V = self.split_heads(self.W_v(V))

    # 2. 计算缩放点积注意力
    attn_output = self.scaled_dot_product_attention(Q, K, V, mask)

    # 3. 合并多头输出并进行最终的线性变换
    output = self.W_o(self.combine_heads(attn_output))
    return output

class PositionWiseFeedForward(nn.Module):
  """
  位置前馈网络模块
  """

  def forward(self, x):
    pass

# --- 编码器核心层 ---

class EncoderLayer(nn.Module):
  def __init__(self, d_model, num_heads, d_ff, dropout):
    super(EncoderLayer, self).__init__()
    self.self_attn = MultiHeadAttention()  # 待实现
    self.feed_forward = PositionWiseFeedForward()  # 待实现
    self.norm1 = nn.LayerNorm(d_model)
    self.norm2 = nn.LayerNorm(d_model)
    self.dropout = nn.Dropout(dropout)

  def forward(self, x, mask):
    # 残差连接与层归一化将在 3.1.2.4 节中详细解释
    # 1. 多头自注意力
    attn_output = self.self_attn(x, x, x, mask)
    x = self.norm1(x + self.dropout(attn_output))

    # 2. 前馈网络
    ff_output = self.feed_forward(x)
    x = self.norm2(x + self.dropout(ff_output))

    return x

# --- 解码器核心层 ---

class DecoderLayer(nn.Module):
  def __init__(self, d_model, num_heads, d_ff, dropout):
    super(DecoderLayer, self).__init__()
    self.self_attn = MultiHeadAttention()  # 待实现
    self.cross_attn = MultiHeadAttention()  # 待实现
    self.feed_forward = PositionWiseFeedForward()  # 待实现
    self.norm1 = nn.LayerNorm(d_model)
    self.norm2 = nn.LayerNorm(d_model)
    self.norm3 = nn.LayerNorm(d_model)
    self.dropout = nn.Dropout(dropout)

  def forward(self, x, encoder_output, src_mask, tgt_mask):
    # 1. 掩码多头自注意力 (对自己)
    attn_output = self.self_attn(x, x, x, tgt_mask)
    x = self.norm1(x + self.dropout(attn_output))

    # 2. 交叉注意力 (对编码器输出)
    cross_attn_output = self.cross_attn(x, encoder_output, encoder_output, src_mask)
    x = self.norm2(x + self.dropout(cross_attn_output))

    # 3. 前馈网络
    ff_output = self.feed_forward(x)
    x = self.norm3(x + self.dropout(ff_output))

    return x
