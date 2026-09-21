import torch
import torchvision
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F

import numpy as np
import matplotlib.pyplot as plt

from torchvision import datasets, transforms
from torch.utils.data import Dataset, DataLoader

#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################

#%% ########################################################################################

# 创建一个输入张量
input = torch.tensor([[1, 2], [3, 4]], dtype=torch.float32)

# 创建与输入相同形状的标准正态分布随机张量
x = torch.randn_like(input)

print(x)

#%% ########################################################################################

transform = transforms.Compose(  # 定义数据预处理
  [
    transforms.ToTensor(),  # 转换为张量
    transforms.Normalize((0.5,), (0.5,)),  # 标准化
  ]
)

train_dataset = torchvision.datasets.MNIST(root="./data", train=True, transform=transform, download=True)  # 加载训练数据集
train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)  # 使用 DataLoader 加载数据

data_iter = iter(train_loader)  ## 查看一个批次的数据
images, labels = next(data_iter)
print(f"批次图像大小: {images.shape}")  # 输出形状为 [batch_size, 1, 28, 28]
print(f"批次标签: {labels}")

#%% ########################################################################################

char_set = list(dict.fromkeys("hello"))  ## 数据集：字符序列预测（Hello -> Elloh）
char_to_idx = {c: i for i, c in enumerate(char_set)}
idx_to_char = {i: c for i, c in enumerate(char_set)}

input_str = "hello"  ## 数据准备
target_str = "elloh"
input_data = [char_to_idx[c] for c in input_str]
target_data = [char_to_idx[c] for c in target_str]

input_one_hot = np.eye(len(char_set))[input_data]  ## 转换为独热编码

inputs = torch.tensor(input_one_hot, dtype=torch.float32)  # 转换为 PyTorch Tensor
targets = torch.tensor(target_data, dtype=torch.long)

input_size = len(char_set)  ## 模型超参数
hidden_size = 8
output_size = len(char_set)
num_epochs = 200
learning_rate = 0.1

class RNNModel(nn.Module):  ## 定义 RNN 模型
  def __init__(self, input_size, hidden_size, output_size):
    super(RNNModel, self).__init__()
    self.rnn = nn.RNN(input_size, hidden_size, batch_first=True)  # [batch_size, sequence_length, input_size]
    self.fc = nn.Linear(hidden_size, output_size)

  def forward(self, x, hidden):  # 还需要隐藏状态 hidden
    out, hidden = self.rnn(x, hidden)  # h → e → l → l → o，时间步 0：输入 h + 初始 hidden → 得到新的 hidden₀；时间步 1：输入 e + hidden₀ → 得到 hidden₁
    out = self.fc(out)  # 应用全连接层
    return out, hidden

model = RNNModel(input_size, hidden_size, output_size)

criterion = nn.CrossEntropyLoss()  ## 定义损失函数和优化器
optimizer = optim.Adam(model.parameters(), lr=learning_rate)

losses = []  ## 训练 RNN
hidden = None  # 初始隐藏状态为 None
for epoch in range(num_epochs):
  optimizer.zero_grad()
  outputs, hidden = model(inputs.unsqueeze(0), hidden)  ## 前向传播
  hidden = hidden.detach()  # 把当前 hidden 与之前的计算图切断，使下一轮反向传播不会继续追溯到上一轮 epoch
  loss = criterion(outputs.view(-1, output_size), targets)  ## 计算损失
  loss.backward()
  optimizer.step()
  losses.append(loss.item())
  if (epoch + 1) % 20 == 0:
    print(f"Epoch [{epoch + 1}/{num_epochs}], Loss: {loss.item():.4f}")

with torch.no_grad():  ## 测试 RNN
  test_hidden = None
  test_output, _ = model(inputs.unsqueeze(0), test_hidden)
  predicted = torch.argmax(test_output, dim=2).squeeze().numpy()
  print("Input sequence: ", "".join([idx_to_char[i] for i in input_data]))
  print("Predicted sequence: ", "".join([idx_to_char[i] for i in predicted]))

plt.plot(losses, label="Training Loss")  ## 可视化损失
plt.xlabel("Epoch")
plt.ylabel("Loss")
plt.title("RNN Training Loss Over Epochs")
plt.legend()
plt.show()

#########################################################################################

transform = transforms.Compose(  ## 1. 数据加载与预处理  # 把多个数据处理操作按照顺序组合起来，每次从数据集中取出一张图片，这些操作都会自动执行
  [
    transforms.ToTensor(),  # 转为张量
    transforms.Normalize((0.5,), (0.5,)),  # 归一化到 [-1, 1]
  ]
)

train_dataset = datasets.MNIST(root="./data", train=True, transform=transform, download=True)  ## 加载 MNIST 数据集  # 训练用
test_dataset = datasets.MNIST(root="./data", train=False, transform=transform, download=True)  # 测试用
train_loader = torch.utils.data.DataLoader(dataset=train_dataset, batch_size=64, shuffle=True)  # DataLoader 负责把数据按照批次交给训练代码，每次取 64 张图片 torch.Size([64, 1, 28, 28])
test_loader = torch.utils.data.DataLoader(dataset=test_dataset, batch_size=64, shuffle=False)  # 单张图片形状 [1, 28, 28]，shuffle 表示是否打乱样本顺序，训练时打乱，测试时不必

class SimpleCNN(nn.Module):  ## 2. 定义 CNN 模型
  def __init__(self):
    super(SimpleCNN, self).__init__()
    # 定义卷积层
    self.conv1 = nn.Conv2d(1, 32, kernel_size=3, stride=1, padding=1)  # 输入1通道，输出32通道
    self.conv2 = nn.Conv2d(32, 64, kernel_size=3, stride=1, padding=1)  # 输入32通道，输出64通道
    # 定义全连接层
    self.fc1 = nn.Linear(64 * 7 * 7, 128)  # 展平后输入到全连接层
    self.fc2 = nn.Linear(128, 10)  # 10 个类别

  def forward(self, x):
    x = F.relu(self.conv1(x))  # 第一层卷积 + ReLU
    x = F.max_pool2d(x, 2)  # 最大池化
    x = F.relu(self.conv2(x))  # 第二层卷积 + ReLU
    x = F.max_pool2d(x, 2)  # 最大池化
    x = x.view(-1, 64 * 7 * 7)  # 展平
    x = F.relu(self.fc1(x))  # 全连接层 + ReLU
    x = self.fc2(x)  # 最后一层输出
    return x

model = SimpleCNN()  # 创建模型实例

criterion = nn.CrossEntropyLoss()  # 3.1 定义损失函数--多分类交叉熵损失
optimizer = optim.SGD(model.parameters(), lr=0.01, momentum=0.9)  # 3.2 定义优化器

num_epochs = 5  ## 4. 模型训练
model.train()  # 设置模型为训练模式
for epoch in range(num_epochs):
  total_loss = 0
  for images, labels in train_loader:
    outputs = model(images)  # 前向传播
    loss = criterion(outputs, labels)  # 计算损失
    optimizer.zero_grad()  # 清空梯度
    loss.backward()  # 反向传播
    optimizer.step()  # 更新参数
    total_loss += loss.item()
  print(f"Epoch [{epoch + 1}/{num_epochs}], Loss: {total_loss / len(train_loader):.4f}")

model.eval()  ## 5. 模型测试  # 设置模型为评估模式
correct = 0
total = 0

with torch.no_grad():  # 关闭梯度计算
  for images, labels in test_loader:
    outputs = model(images)
    _, predicted = torch.max(outputs, 1)
    total += labels.size(0)
    correct += (predicted == labels).sum().item()
accuracy = 100 * correct / total
print(f"Test Accuracy: {accuracy:.2f}%")

dataiter = iter(test_loader)  ## 6. 可视化测试结果
images, labels = next(dataiter)
outputs = model(images)
_, predictions = torch.max(outputs, 1)
fig, axes = plt.subplots(1, 6, figsize=(12, 4))
for i in range(6):
  axes[i].imshow(images[i][0], cmap="gray")
  axes[i].set_title(f"Label: {labels[i]}\nPred: {predictions[i]}")
  axes[i].axis("off")
plt.show()

#########################################################################################

torch.manual_seed(42)  ## 1. 准备数据
X = torch.randn(100, 2)  # 100 个样本，2 个特征
true_w = torch.tensor([2.0, 3.0])
true_b = 4.0
Y = X @ true_w + true_b + torch.randn(100) * 0.1

class LinearRegressionModel(nn.Module):  ## 2. 定义模型
  def __init__(self):
    super().__init__()
    self.linear = nn.Linear(2, 1)

  def forward(self, x):
    return self.linear(x)

model = LinearRegressionModel()
print(model.linear.weight)
print(model.linear.bias)

criterion = nn.MSELoss()  ## 3. 定义损失函数和优化器
optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
# optimizer = torch.optim.Adam(model.parameters(), lr=0.01)  # 通常收敛更快

num_epochs = 1000  ## 4. 训练模型
for epoch in range(num_epochs):
  model.train()
  optimizer.zero_grad()
  predictions = model(X)
  loss = criterion(predictions.squeeze(), Y)
  loss.backward()
  # print(model.linear.weight.grad)
  # print(model.linear.bias.grad)
  optimizer.step()  # 更新参数，修改 model.linear.weight.grad 和 model.linear.bias.grad
  if (epoch + 1) % 100 == 0:
    print(f"Epoch [{epoch + 1}/{num_epochs}], Loss: {loss.item():.4f}")

print(f"\n训练后的权重: {model.linear.weight.detach().squeeze()}")  ##5. 评估模型
print(f"训练后的偏置: {model.linear.bias.detach().item()}")  # detach 创建一个与计算图分离的 Tensor，只用于读取结果，不再参与梯度计算
print(f"真实权重: {true_w.numpy()}")
print(f"真实偏置: {true_b}")

#########################################################################################

torch.manual_seed(42)  # 随机种子，确保每次运行结果一致，从“种子 42 所对应的初始状态”开始生成随机数

X = torch.randn(100, 2)  # 100 个样本，每个样本 2 个特征
true_w = torch.tensor([2.0, 3.0])  # 假设真实权重
true_b = 4.0  # 偏置项
Y = X @ true_w + true_b + torch.randn(100) * 0.1  # 加入一些噪声

print(X[:5])
print(Y[:5])

#########################################################################################

class MyDataset(Dataset):  # 自定义数据集类
  def __init__(self, x_data, y_data):
    """
    初始化数据集，X_data 和 Y_data 是两个列表或数组
    x_data: 输入特征
    y_data: 目标标签
    """
    self.X_data = x_data
    self.Y_data = y_data

  def __len__(self):
    """返回数据集的大小"""
    return len(self.X_data)

  def __getitem__(self, idx):
    """返回指定索引的数据"""
    x = torch.tensor(self.X_data[idx], dtype=torch.float32)  # 转换为 Tensor
    y = torch.tensor(self.Y_data[idx], dtype=torch.float32)
    return x, y

X_data = [[1, 2], [3, 4], [5, 6], [7, 8]]  # 示列--输入特征
Y_data = [1, 3, 8, 5]  # 示列--目标标签
dataset = MyDataset(X_data, Y_data)  # 创建数据集实例

dataloader = DataLoader(dataset, batch_size=2, shuffle=True, drop_last=True)  # 创建 DataLoader 实例，batch_size 设置每次加载的样本数量，shuffle 打乱样本顺序
for _ in range(1):  # 整个数据集会被遍历 1 次
  for batch_idx, (inputs, labels) in enumerate(dataloader):  # inputs--当前批次的所有输入特征，labels--当前批次所有输入对应的标签
    print(f"Batch {batch_idx + 1}:")
    print(f"Inputs: {inputs}")
    print(f"Labels: {labels}")

#########################################################################################

n_samples = 100
data = torch.randn(n_samples, 2)  # 生成 100 个二维数据点
labels = (data[:, 0] ** 2 + data[:, 1] ** 2 < 1).float().unsqueeze(1)  # 点在圆内为1，圆外为0
plt.scatter(data[:, 0], data[:, 1], c=labels.squeeze(), cmap="coolwarm")  # 可视化数据 ####
plt.title("Generated Data")
plt.xlabel("Feature 1")
plt.ylabel("Feature 2")
plt.show()

class SimpleNN(nn.Module):  # 定义前馈神经网络
  def __init__(self):
    super(SimpleNN, self).__init__()
    self.fc1 = nn.Linear(2, 4)  # 输入层有 2 个特征，隐藏层有 4 个神经元
    self.fc2 = nn.Linear(4, 1)  # 隐藏层输出到 1 个神经元（用于二分类）
    self.sigmoid = nn.Sigmoid()  # 二分类激活函数
  def forward(self, x):
    x = torch.relu(self.fc1(x))  # 使用 ReLU 激活函数
    x = self.sigmoid(self.fc2(x))  # 输出层使用 Sigmoid 激活函数
    return x

model = SimpleNN()  # 实例化模型
criterion = nn.BCELoss()  # 定义损失函数--二元交叉熵损失
optimizer = optim.SGD(model.parameters(), lr=0.1)  # 定义优化器--使用随机梯度下降优化器

epochs = 100
for epoch in range(epochs):  # 训练模型
  outputs = model(data)  # 前向传播 #
  loss = criterion(outputs, labels)
  optimizer.zero_grad()  # 反向传播 ##
  loss.backward()
  optimizer.step()
  if (epoch + 1) % 10 == 0:  # 每 10 轮打印一次损失
    print(f"Epoch [{epoch + 1}/{epochs}], Loss: {loss.item():.4f}")

def plot_decision_boundary(model, data):  # 可视化决策边界
  x_min, x_max = data[:, 0].min() - 1, data[:, 0].max() + 1
  y_min, y_max = data[:, 1].min() - 1, data[:, 1].max() + 1
  xx, yy = torch.meshgrid(torch.arange(x_min, x_max, 0.1), torch.arange(y_min, y_max, 0.1), indexing="ij")
  grid = torch.cat([xx.reshape(-1, 1), yy.reshape(-1, 1)], dim=1)
  predictions = model(grid).detach().numpy().reshape(xx.shape)
  plt.contourf(xx, yy, predictions, levels=[0, 0.5, 1], cmap="coolwarm", alpha=0.7)
  plt.scatter(data[:, 0], data[:, 1], c=labels.squeeze(), cmap="coolwarm", edgecolors="k")
  plt.title("Decision Boundary")
  plt.show()

plot_decision_boundary(model, data)

#########################################################################################

n_in, n_h, n_out, batch_size = 10, 5, 1, 10  # 定义输入层大小、隐藏层大小、输出层大小和批量大小

x = torch.randn(batch_size, n_in)  # 创建输入数据--随机生成
y = torch.tensor([[1.0], [0.0], [0.0], [1.0], [1.0], [1.0], [0.0], [0.0], [1.0], [1.0]])  # 创建目标输出数据

model = nn.Sequential(  # 创建顺序模型，包含线性层、ReLU激活函数和Sigmoid激活函数
  nn.Linear(n_in, n_h),  # 输入层到隐藏层的线性变换
  nn.ReLU(),  # 隐藏层的ReLU激活函数
  nn.Linear(n_h, n_out),  # 隐藏层到输出层的线性变换
  nn.Sigmoid(),  # 输出层的Sigmoid激活函数
)

criterion = nn.MSELoss()  # 定义均方误差损失函数
optimizer = optim.SGD(model.parameters(), lr=0.01)  # 随机梯度下降优化器，学习率为0.01

losses = []  # 用于存储每轮的损失值

for epoch in range(50):  # 执行梯度下降算法进行模型训练，迭代50次
  y_pred = model(x)  # 前向传播，计算预测值
  loss = criterion(y_pred, y)  # 计算损失
  losses.append(loss.item())  # 记录损失值

  print(f"Epoch [{epoch + 1}/50], Loss: {loss.item():.4f}")  # 打印损失值

  optimizer.zero_grad()  # 清零梯度
  loss.backward()  # 反向传播，计算梯度
  optimizer.step()  # 更新模型参数

plt.figure(figsize=(8, 5))  ## 可视化损失变化曲线
plt.plot(range(1, 51), losses, label="Loss")
plt.xlabel("Epoch")
plt.ylabel("Loss")
plt.title("Training Loss Over Epochs")
plt.legend()
plt.grid()
plt.show()

y_pred_final = model(x).detach().numpy()  # 最终预测值
y_actual = y.numpy()  # 实际值

plt.figure(figsize=(8, 5))
plt.plot(range(1, batch_size + 1), y_actual, "o-", label="Actual", color="blue")
plt.plot(range(1, batch_size + 1), y_pred_final, "x--", label="Predicted", color="red")
plt.xlabel("Sample Index")
plt.ylabel("Value")
plt.title("Actual vs Predicted Values")
plt.legend()
plt.grid()
plt.show()

#########################################################################################

# 定义输入层大小、隐藏层大小、输出层大小和批量大小
n_in, n_h, n_out, batch_size = 10, 5, 1, 10

# 创建虚拟输入数据和目标数据
x = torch.randn(batch_size, n_in)  # 随机生成输入数据
y = torch.tensor([[1.0], [0.0], [0.0], [1.0], [1.0], [1.0], [0.0], [0.0], [1.0], [1.0]])  # 目标输出数据

# 创建顺序模型，包含线性层、ReLU激活函数和Sigmoid激活函数
model = nn.Sequential(
  nn.Linear(n_in, n_h),  # 输入层到隐藏层的线性变换
  nn.ReLU(),  # 隐藏层的ReLU激活函数
  nn.Linear(n_h, n_out),  # 隐藏层到输出层的线性变换
  nn.Sigmoid()  # 输出层的Sigmoid激活函数
)

# 定义均方误差损失函数和随机梯度下降优化器
criterion = torch.nn.MSELoss()
optimizer = torch.optim.SGD(model.parameters(), lr=0.01)  # 学习率为0.01

# 执行梯度下降算法进行模型训练
for epoch in range(50):  # 迭代50次
  y_pred = model(x)  # 前向传播，计算预测值
  loss = criterion(y_pred, y)  # 计算损失
  print('epoch: ', epoch, 'loss: ', loss.item())  # 打印损失值

  optimizer.zero_grad()  # 清零梯度
  loss.backward()  # 反向传播，计算梯度
  optimizer.step()  # 更新模型参数

#########################################################################################

tensor = torch.tensor([[1, 2, 3], [4, 5, 6]], dtype=torch.float32)
print("Tensor:\n", tensor)
print("Shape:", tensor.shape)
print("Size:", tensor.size())
print("Data Type:", tensor.dtype)
print("Device:", tensor.device)
print("Dimensions:", tensor.dim())
print("Total Elements:", tensor.numel())
print("Requires Grad:", tensor.requires_grad)
print("Is CUDA:", tensor.is_cuda)
print("Is Contiguous:", tensor.is_contiguous())

tensor_T = tensor.T
print("Transposed Tensor:\n", tensor_T)

tensor = torch.tensor(42)
print("Single Element Value:", tensor.item())

#########################################################################################

class SimpleNN1(nn.Module):
  def __init__(self):
    super(SimpleNN1, self).__init__()
    self.fc1 = nn.Linear(2, 2)  # nn.Linear 会自动创建权重矩阵和偏置向量，不需要手动定义
    self.fc2 = nn.Linear(2, 1)

  def forward(self, x):
    x = torch.relu(self.fc1(x))  # ReLU 激活函数
    x = self.fc2(x)
    return x

model = SimpleNN()

criterion = nn.MSELoss()  # 定义损失函数, 均方误差
optimizer = optim.Adam(model.parameters(), lr=0.001)  # 将所有可训练参数交给 Adam

input_data = torch.randn(10, 2)  # 10 个样本, 2 个特征
target_data = torch.randn(10, 1)  # 10 个目标值

for epoch in range(100):  # 训练 100 轮
  optimizer.zero_grad()  # 清除模型参数上一轮保存的梯度, 否则会累加
  output_data = model(input_data)  # 前向传播
  loss = criterion(output_data, target_data)
  loss.backward()  # 从损失开始, 反向计算每个权重和偏置对损失的影响
  optimizer.step()  # 读取每个参数的 grad 并修改, 最初的随机值 → 向降低损失的方向小步调整
  if (epoch + 1) % 10 == 0:
    print(f'Epoch [{epoch + 1}/100], Loss: {loss.item():.4f}')
  # with torch.no_grad():  # 关闭梯度记录, 不记录计算过程, 也不准备反向传播
  #   new_output = model(input_data)
  #   new_loss = criterion(new_output, target_data)

# 前向计算路径: input_data → fc1 → ReLU → fc2 → output_data → MSELoss → loss
# 反向计算: loss → output_data → fc2 → ReLU → fc1

#########################################################################################

model = nn.Linear(1, 1)

criterion = nn.MSELoss()  # 回归问题使用均方误差
optimizer = optim.Adam(model.parameters(), lr=0.01)  # 使用 Adam 优化器更新模型参数

x = torch.tensor([[1.0], [2.0], [3.0]])
y = torch.tensor([[2.0], [4.0], [6.0]])

for epoch in range(100):
  prediction = model(x)  # 模型预测
  loss = criterion(prediction, y)  # 计算预测值与真实值之间的损失
  optimizer.zero_grad()  # 清空上一轮保存的梯度
  loss.backward()  # 计算每个参数的梯度
  optimizer.step()  # 更新模型参数
  if epoch % 10 == 0:
    print(f"Epoch: {epoch}, Loss: {loss.item():.4f}")

#########################################################################################

class MyModel(nn.Module):
  def __init__(self):
    super().__init__()
    self.encoder = nn.Sequential(nn.Linear(4, 8), nn.ReLU())  # 顺序执行的流水线
    self.layers = nn.ModuleList([nn.Linear(8, 8), nn.Linear(8, 8)])  # 保存多个网络层
    self.scale = nn.Parameter(torch.tensor(1.0))  # 自定义可训练参数
    self.output = nn.Linear(8, 1)

  def forward(self, x):
    x = self.encoder(x)  # Sequential 自动依次执行
    for layer in self.layers:  # ModuleList 需要自己编写执行过程
      x = torch.relu(layer(x))
    x = x * self.scale  # 使用自定义参数
    return self.output(x)

#########################################################################################

class SimpleCNN(nn.Module):
  def __init__(self):
    super().__init__()

    self.features = nn.Sequential(
      # 输入：3 × 32 × 32 彩色图片
      nn.Conv2d(3, 16, kernel_size=3, padding=1),  # 第一个卷积层 16 × 32 × 32
      nn.ReLU(),

      # 输出：16 × 16 × 16
      nn.MaxPool2d(kernel_size=2, stride=2),  # 第一个池化层

      # 输出：32 × 16 × 16
      nn.Conv2d(16, 32, kernel_size=3, padding=1),  # 第二个卷积层
      nn.ReLU(),

      # 输出：32 × 8 × 8
      nn.MaxPool2d(kernel_size=2, stride=2)  # 第二个池化层
    )

    self.classifier = nn.Sequential(
      # 32 × 8 × 8 = 2048
      nn.Flatten(),  # Flatten 展平
      nn.Linear(32 * 8 * 8, 128),  # 第一个线性层
      nn.ReLU(),
      nn.Linear(128, 10)  # 最后线性层
    )

  def forward(self, x):
    x = self.features(x)
    x = self.classifier(x)
    return x

#########################################################################################

