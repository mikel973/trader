# trader##  安装组件### 安装 conda* [anaconda 清华镜像](https://mirrors.tuna.tsinghua.edu.cn/anaconda/archive/)* [miniconda 清华镜像](https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/)```shell# 创建 env 环境conda create -n mtrader python=3.8# 激活 env 环境conda activate mtrader  ```### 安装组件```python# 安装数据获取组件pip install tusharepip install qstock    pip install akshare pip install baostock# 安装 回测组件pip install backtrader# 安装 绘图组件pip install matplotlibpip install pandas_datareader```### tushare 需要设置 token```pythonimport tushare as tsts.set_token('your token here')pro = ts.pro_api('your token')```