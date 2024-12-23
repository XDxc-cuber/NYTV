<img src="https://upload.wikimedia.org/wikipedia/commons/f/f3/Apache_Spark_logo.svg" alt="Apache Spark Icon" width="600">

# :dizzy:Spark分布式处理纽约市交通流量信息-热力地图可视化与流量预测

## 项目简介

本项目旨在处理纽约市交通流量数据，利用 Apache Spark 的分布式计算能力对大规模数据进行快速处理，使用深度学习模型进行交通流量预测，并通过 Web 界面进行可视化展示。项目分为三个主要模块：

- **后端模块**：使用 HDFS 进行数据存储，利用 PySpark 的分布式计算能力对原始交通数据处理和聚合，根据用户前端提交的时间范围动态筛选数据范围，生成热力图，并且提供切换 HDFS 数据和本地预测数据的功能。

- **预测模块**：基于 D2STGNN 模型进行未来交通流量预测。

- **前端模块**：通过 Web 界面展示交通流量数据和预测结果，用户可以交互查看不同年份的热力图、切换实际与预测数据、以及下载热点街道数据。

    


## 项目结构

```
NYTV-main/
├── App/
│   ├── app.py                        # 主程序
│   ├── static/
│   │   ├── hot_streets.csv           # 热点街道数据
│   │   └── map.html                  # 生成的热力图文件
│   ├── templates/
│   │   ├── index.html                # 前端主页面模板
│   │   └── map.html                  
│   └── predict_month.csv             # 预测数据
├── Predict/
│   ├── configs/
│   │   ├── NYTV.yaml
│   │   └── ...
│   ├── dataloader/
│   │   ├── ...
│   ├── datasets/
│   │   ├── raw_data/
│   │   │   └── NYTV/
│   │   │       ├── generate_training_data.py
│   │   │       ├── generate_adj_mx.py
│   │   │       └── ...
│   ├── figures/
│   │   └── ...
│   ├── main.py
│   ├── models/
│   │   └── ...
│   ├── utils/
│   │   └── ...
│   ├── requirements.txt
│   └── ...
├── README.md
└── ...
```



## 环境依赖

### 后端模块

- **PySpark**：数据处理和聚合
- **HDFS**：分布式存储数据
- **Pandas**：数据转换

### 前端模块

- **Folium**：热力图生成
- **Flask**：提供 Web 框架支持

### 预测模块

- **Python 3.10**
- **PyTorch**
- **依赖库**（详见 `requirements.txt`）



## 数据集

本项目使用了纽约市交通局（NYC DOT）的[自动交通流量计数数据集](https://data.cityofnewyork.us/Transportation/Automated-Traffic-Volume-Counts/7ym2-wayt/about_data)。该数据集包含纽约市桥梁和道路的交通流量信息样本，由自动交通记录仪（ATR）采集。

- **数据更新时间**：2024 年 9 月 3 日
- **数据范围**：各行政区桥梁交叉口和道路上的交通流量数据，包含车辆计数、日期、时间、位置、方向等信息。
- **数据量**：约 171 万行，14 列
- **数据字段**：
  - `RequestID`：每个计数请求的唯一 ID
  - `Boro`：所在行政区
  - `Yr`、`M`、`D`：计数日期（年、月、日）
  - `HH`、`MM`：计数时间（小时、分钟）
  - `Vol`：15 分钟内的车辆计数总和
  - `SegmentID`：街道段 ID
  - `WktGeom`：空间坐标信息（Well-Known Text 格式）
  - `street`、`fromSt`、`toSt`：所在街道、起点街道、终点街道
  - `Direction`：交通方向



---

## Spark集群部署

本项目中，Spark集群采用docker容器部署。使用docker容器的好处在于，可以使用现有的镜像快速搭建实例，并且通过可以通过docker-compose文件快速定义各个节点的配置信息和网络结构。相较于多虚拟机的集群更加稳定可控。

集群架构为一个master节点对应两个worker节点，同时HDFS文件系统使用namenode和datanode两个容器。docker-compose文件配置如下：

```yml
version: '3.8'
networks:
  spark-hadoop-network:
    driver: bridge
services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    ports:
      - "9870:9870"
      - "8020:8020"
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://192.168.1.9:8020
      - CLUSTER_NAME=spark-hadoop-cluster
    volumes:
      - namenode-data:/hadoop/dfs/name

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    ports:
      - "9864:9864"
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://namenode:8020
    volumes:
      - datanode-data:/hadoop/dfs/data

  spark-master:
    image: bde2020/spark-master
    container_name: spark-master
    ports:
      - "8080:8080"  # Spark Web UI
      - "7077:7077"  # Spark Master Port
    environment:
      - SPARK_MODE=master

  spark-worker-1:
    image: bde2020/spark-worker
    container_name: spark-worker-1
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://192.168.1.9:7077

  spark-worker-2:
    image: bde2020/spark-worker
    container_name: spark-worker-2
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://192.168.1.9:7077

volumes:
  namenode-data:
  datanode-data:

```

该配置定义了容器使用的镜像名称和网络架构情况，启动容器后，容器之间可以相互通信。主机与容器之间的通信使用局域网的ip+端口。



## 后端模块（PySpark）

后端的主要功能是处理数据的加载、筛选和分析，同时生成热力图并与前端交互。后端功能可进一步细分为以下几个模块：

#### 1. **主要配置与初始化**

这里定义了HDFS的相关信息和spark集群的工作配置参数。其中.master项定义了工作模式为cluster模式。

```Python
# 初始化 Flask 应用
app = Flask(__name__, static_folder="static")

# Google 瓦片 URL
GOOGLE_TILE_URL = "https://mt.google.com/vt/lyrs=m&x={x}&y={y}&z={z}"

# 默认 HDFS 数据文件路径的配置
HDFS_DATA_PATH = "hdfs://192.168.1.9:8020/data/data.csv"  # HDFS Namenode 地址
hdfs_temp_output_path = "hdfs://192.168.1.9:8020/output/temp_processed_data"  # HDFS 临时输出路径
hdfs_final_output_path = "hdfs://192.168.1.9:8020/output/processed_data.csv"  # HDFS 最终输出路径

# Spark 集群的配置
spark = SparkSession.builder \
    .appName("NYC Traffic Heatmap") \
    .master("spark://192.168.1.9:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "20") \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.cores", "2") \
    .config("spark.driver.bindAddress", "192.168.1.9") \
    .config("spark.driver.host", "192.168.1.9") \
    .getOrCreate()
```

#### **2. 数据加载和处理（分布式读写和聚合）**

通过 PySpark 从 HDFS 加载交通流量数据（`data.csv`），按经纬度、时间和街道信息聚合流量数据。

```Python
# 从 HDFS 加载 CSV 数据
raw_data = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(HDFS_DATA_PATH)

# 按经纬度、时间和街道信息聚合流量数据
aggregated_data = raw_data.groupBy(
    "lat", "lon", "year", "month", "day", "hour", "street"
).agg(
    spark_sum("Vol").alias("total_volume"),
    avg("Vol").alias("avg_volume")
)

# 合并数据分区
aggregated_data = aggregated_data.coalesce(1)

# 将结果保存到 HDFS 临时目录
aggregated_data.write.format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(hdfs_temp_output_path)

# 使用 WebHDFS API 获取生成的文件名
namenode_host = "192.168.1.9"
webhdfs_url = f"http://{namenode_host}:9870/webhdfs/v1/output/temp_processed_data/?op=LISTSTATUS"
response = requests.get(webhdfs_url)
response.raise_for_status()
file_status = response.json()
files = file_status['FileStatuses']['FileStatus']
csv_file_name = [file['pathSuffix'] for file in files if file['pathSuffix'].startswith('part')][0]
print("success")

# 使用 WebHDFS API 重命名文件
rename_url = f"http://{namenode_host}:9870/webhdfs/v1/output/temp_processed_data/{csv_file_name}?op=RENAME&destination=/output/processed_data.csv&user.name=root"
rename_response = requests.put(rename_url)
rename_response.raise_for_status()
```

#### **3. 动态筛选和数据过滤**

后端根据用户前端提交的时间范围（年、月、小时）筛选符合条件的数据，并返回结果。

将筛选后的数据转换为 Pandas DataFrame，以便前端展示和绘制热力图。

```Python
# 过滤数据（利用 Spark DataFrame）
filtered_data = aggregated_data.filter(
    (col("year") >= selected_start_year) & (col("year") <= selected_end_year) &
    (col("month") >= selected_start_month) & (col("month") <= selected_end_month) &
    (col("hour") >= selected_start_hour) & (col("hour") <= selected_end_hour)
)

# 将数据转换为 Pandas DataFrame
pandas_data = filtered_data.toPandas()
```

#### **4. 热力图生成**

通过 Folium 和 HeatMap 将筛选后的数据转换为热力图，在热力图中叠加 Google 地图瓦片。

```python
# Google 瓦片 URL
GOOGLE_TILE_URL = "https://mt.google.com/vt/lyrs=m&x={x}&y={y}&z={z}"

# 创建热力图
heat_data = pandas_data[["lat", "lon", "total_volume"]].values.tolist()
nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=12, tiles=None)
TileLayer(tiles=GOOGLE_TILE_URL, attr="© Google Maps", name="Google Maps", overlay=False).add_to(nyc_map)
HeatMap(heat_data, min_opacity=0.5, max_val=max(pandas_data["total_volume"])).add_to(nyc_map)
nyc_map.save("./static/map.html")
```

#### **5. 数据切换功能**

支持动态切换不同的数据源，便于用户在前端界面进行分析对比。   

可以从本地加载预测数据文件（`predict_month.csv`）进行对比分析。

```python
@app.route("/switch_data_file", methods=["POST"])
def switch_data_file():
    global use_hdfs
    new_data_file = request.json.get("data_file")
    
    if new_data_file == "processed_data_with_year.csv":
        use_hdfs = True  # 切换到 HDFS 数据
    elif new_data_file == "predict_month.csv":
        use_hdfs = False  # 切换到本地预测数据
```

#### **6. 热点街道数据下载**

支持用户下载高流量的热点街道分析结果到本地。

```python
@app.route("/download_hot_streets", methods=["GET"])
def download_hot_streets():
    try:
        # 从本地文件读取过滤后的数据
        hot_streets_path = "./static/hot_streets.csv"
        return send_file(hot_streets_path, as_attachment=True)
    except Exception as e:
        return jsonify({"status": "error", "message": "Failed to process hot streets."})
```



## 前端模块（HTML + Flask 模板 + AJAX）

前端通过 Flask 模板渲染动态生成页面，结合 AJAX 实现与后端的交互，功能包括表单筛选、动态热力图加载和数据源切换。

#### **2.1 表单筛选功能**

提供时间范围选择表单（年、月、小时），提交表单后，通过 POST 请求将筛选条件发送至后端，后端返回过滤后的数据和更新后的热力图。

```python
<div id="year-selector">
        <form method="POST">
            <label for="start_year">开始年份:</label>
            <select id="start_year" name="start_year">
                {% for year in range(min_year, max_year + 1) %}
                    <option value="{{ year }}" {% if year == selected_start_year %}selected{% endif %}>{{ year }}</option>
                {% endfor %}
            </select>

            <label for="end_year">结束年份:</label>
            <select id="end_year" name="end_year">
                {% for year in range(min_year, max_year + 1) %}
                    <option value="{{ year }}" {% if year == selected_end_year %}selected{% endif %}>{{ year }}</option>
                {% endfor %}
            </select>

            <label for="start_month">开始月份:</label>
            <select id="start_month" name="start_month">
                {% for month in range(min_month, max_month + 1) %}
                    <option value="{{ month }}" {% if month == selected_start_month %}selected{% endif %}>{{ month }}</option>
                {% endfor %}
            </select>

            <label for="end_month">结束月份:</label>
            <select id="end_month" name="end_month">
                {% for month in range(min_month, max_month + 1) %}
                    <option value="{{ month }}" {% if month == selected_end_month %}selected{% endif %}>{{ month }}</option>
                {% endfor %}
            </select>

            <label for="start_hour">开始时间 (小时):</label>
            <select id="start_hour" name="start_hour">
                {% for hour in range(min_hour, max_hour + 1) %}
                    <option value="{{ hour }}" {% if hour == selected_start_hour %}selected{% endif %}>{{ hour }}</option>
                {% endfor %}
            </select>

            <label for="end_hour">结束时间 (小时):</label>
            <select id="end_hour" name="end_hour">
                {% for hour in range(min_hour, max_hour + 1) %}
                    <option value="{{ hour }}" {% if hour == selected_end_hour %}selected{% endif %}>{{ hour }}</option>
                {% endfor %}
            </select>

            <button type="submit">生成热力图</button>
        </form>
        <button id="switch1" onclick="switchToNow()">切换至实际示例</button>
        <button id="switch2" onclick="switchToPredict()">切换至预测示例</button>
        <button id="download-button" onclick="downloadHotStreets()">下载热点街道数据</button>
    </div>
```

#### **2.2 动态热力图显示**

 将生成的热力图嵌入到前端页面中。

```python
<iframe src="/static/map.html"></iframe>
```

#### **2.3 数据源切换**

前端提供按钮切换不同的数据源，并通过 AJAX 请求通知后端切换。

```python
	<script>
        function downloadHotStreets() {
            window.location.href = "/download_hot_streets";
        }
        function switchToNow() {
            fetch("/switch_data_file", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({ data_file: "processed_data_with_year.csv" })
            })
            .then(response => response.json())
            .then(() => window.location.reload());
        }

        function switchToPredict() {
            fetch("/switch_data_file", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({ data_file: "predict_month.csv" })
            })
            .then(response => response.json())
            .then(() => window.location.reload());
        }
    </script>
```

#### 2.4 API 路由功能说明

| 路由                  | 方法 | 功能描述                                         |
| --------------------- | ---- | ------------------------------------------------ |
| /                     | GET  | 加载主页面，展示默认时间范围内的数据和热力图。   |
| /                     | POST | 接收用户的过滤条件，返回更新后的热力图及数据表。 |
| /switch_data_file     | POST | 切换数据文件（HDFS 和本地文件）。                |
| /download_hot_streets | GET  | 下载过滤后的热点街道数据。                       |



## 预测模块

### 预测模型D2STGNN

预测模块使用了 **D2STGNN（Decoupled Dynamic Spatial-Temporal Graph Neural Network）** 模型，这是一个用于时空序列预测的先进深度学习模型，主要在交通预测领域表现出色。该模型能够有效捕捉复杂的时空依赖关系，建模交通数据中的时空相关性。

![D2STGNN](https://github.com/GestaltCogTeam/D2STGNN/raw/github/figures/D2STGNN.png)

- **VLDB'22 paper: **["Decoupled Dynamic Spatial-Temporal Graph Neural Network for Traffic Forecasting"](https://arxiv.org/abs/2202.04179)

- **模型特点**：

  - **解耦**：将交通数据的扩散信号和固有信号进行解耦，分别处理，提高预测性能。
  - **动态图学习**：通过动态图学习模块，捕获交通网络随时间变化的动态特性，适应时空关系的变化。
  - **双阶段注意力**：通过时间和空间注意力机制，提升对时空依赖的建模能力。
  - **多尺度学习**：结合多尺度特征提取和图神经网络，通过不同的时间窗口长度捕捉不同粒度的时间依赖，将短期波动和长期趋势结合起来，提高了对长时间依赖和局部变化的建模能力和预测性能。
  - **双域学习**：在时域和频域上同时建模，充分利用数据的时空依赖。
  - **高性能**：在多个真实交通数据集上取得了先进的预测效果。

  

### 节点和边的构建

#### 1. 节点构建

- **节点构建**：`WktGeom` 包含了道路点的空间几何信息，作为每个节点的唯一标识。将 `WktGeom` 转化为字符串，然后使用哈希函数映射为节点索引 `[0, n-1]`。
- **节点信息获取**：`WktGeom` 包含了道路点的空间几何信息，利用这些信息可以确定图的节点。

#### 2. 边构建

- **街道信息收集**：每个数据点包含三个街道信息：`street`（流量统计的街道）、`fromSt`（起始街道）和 `toSt`（终点街道）。

- **维护街道字典**：建立一个字典，键为街道名称，值为与该街道相关的所有节点的集合。

  ```python
  street_dict = {}
  for node in nodes:
      streets = [node['street'], node['fromSt'], node['toSt']]
      for street in streets:
          if street not in street_dict:
              street_dict[street] = set()
          street_dict[street].add(node['id'])
  ```

- **边的连接方式**：

  - 对于每一条街道，获取其相关的所有节点集，假设有 `m` 个节点。

  - **排序节点**：假设街道是直线的，根据节点的经度（longitude）对节点进行排序。

    ```python
    nodes_on_street = list(street_dict[street])
    nodes_on_street.sort(key=lambda node_id: node_longitude[node_id])
    ```

  - **连接节点**：按照排序后的顺序，将相邻的节点两两连接，即第一个节点连接第二个，第二个连接第三个，直到第 `m` 个节点。这样每条街道得到 `m-1` 条边，避免了生成过多的边。

    ```python
    for i in range(len(nodes_on_street) - 1):
        node_i = nodes_on_street[i]
        node_j = nodes_on_street[i + 1]
        edges.add((node_i, node_j))
    ```

#### 3. 边权重计算

- **距离计算**：使用 `WktGeom` 中的坐标，直接计算节点之间的欧氏距离，作为边的权重。

  ```python
  # 示例代码
  from math import sqrt
  def euclidean_distance(node_a, node_b):
      x1, y1 = node_coordinates[node_a]
      x2, y2 = node_coordinates[node_b]
      return sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)
  ```

#### 4. 生成图结构

- **图表示**：通过上述步骤，构建了一个包含节点和边的交通路网图，节点数为 `N`，邻接矩阵大小为 `N x N`。

- **数据整合**：将节点的时间信息进行合并，得到形状为 `(T, N, 1)` 的流量数据，其中 `T` 为时间长度，`N` 为节点数量。



### 模型构建与训练

- **核心模型**：采用先进的时空图神经网络模型对交通数据进行预测，捕获复杂的时空关联。
- **模型特点**：
  - **信号分离**：将交通信号分为扩散信号和固有信号，分别进行建模，提高预测精度。

  - **动态图学习**：引入动态图结构，捕捉交通网络的动态变化特性。
  - **节点筛选策略**：

    - 由于完整图的节点数量较大，计算资源有限，选取流量最大的节点进行建模。

    - 具体步骤：

      - 选取流量最大的 10 个节点。

      - 从这 10 个节点出发，使用广度优先搜索（BFS），直到获得 500 个节点的子图。

      - 重新构建该子图的邻接矩阵，尺寸为 `507 x 507`。
- **预测任务**：
  - **短期预测**：预测未来 12 小时内的交通流量。

  - **长期预测**：预测未来 12 个月内的交通流量。

  - **差异**：主要在于时间维度的处理，短期关注细粒度变化，长期关注趋势变化。

* **模型配置**

  ```yaml
  ---
  # start up
  start_up:
    # =================== running mode (select one of the three) ================== #
    mode: test     # three mode: test, resume, scratch
    resume_epoch: 0   # default to zero, if use the 'resume' mode, users need to set the epoch to resume.
  
    model_name:   D2STGNN                                   # model name
    device:       cuda:0
    load_pkl:     False                                     # load serialized dataloader
  
  # Data Processing
  data_args:
    data_dir:       datasets/NYTV                        # data path
    adj_data_path:  datasets/sensor_graph/adj_mx_NY.pkl     # adj data path
    adj_type:       doubletransition                        # adj type to preprocessing
  
  # Model Args
  model_args:
    batch_size:   32
    num_feat:     1
    num_hidden:   32
    node_hidden:  12
    time_emb_dim: 12
    dropout:      0.1
    seq_length:   12
    k_t:          3
    k_s:          2
    gap:          3
    num_modalities: 2
  
  # Optimization Args
  optim_args:
    # adam optimizer
    lrate:          0.005                                   # learning rate
    print_model:    False
    wdecay:         1.0e-4                                  # weight decay of adam
    eps:            1.0e-7                                  # eps of adam
    # learning rate scheduler
    lr_schedule:    True                                    # if use learning rate scheduler
    lr_sche_steps:  [1, 3, 10, 20, 54, 200]                     # steps where decay the learning rate
    lr_decay_ratio: 0.5                                     # learning rate decay rate
    # curriculum learning
    if_cl:          True                                    # if use curriculum learning
    cl_epochs:      3                                       # epochs of curriculum learning when to forecasting next time step
    output_seq_len: 12
    # warm up
    warm_epochs:    3                                      # epochs of warmming up
    # procedure
    epochs:         100                                     # total epoch numbers
    patience:       100                                     # patience for earlystopping
    seq_length:     12                                      # input & output seq length
  
  ```

* **训练数据**

  * 12小时流量预测
    * 由于时间跨度过大（2000.01.01——2024.06.11），原始数据共计117085条，筛选最后20000条数据用于训练和预测。数据维度：$(20000,507,1)$
  * 12个月流量预测
    * 数据维度：$(197,507,1)$

## 团队成员分工

| 姓名   | 学号        | 分工                                                    | 贡献百分比 |
| ------ | ----------- | ------------------------------------------------------- | ---------- |
| 郑智玮 | 51275903122 | 实验设计、仓库搭建、web展示模块、编写README、代码整合   | 25%        |
| 金子龙 | 51275903071 | 实验设计、实现Spark数据处理、编写README、代码优化       | 25%        |
| 麻旭晨 | 51275903113 | 实验设计、实现预测模块、编写README、代码优化            | 25%        |
| 裘王辉 | 51275903106 | 实验设计、Spark集群部署、编写README和PPT、代码整合      | 25%        |





