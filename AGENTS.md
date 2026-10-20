# Interlace Repository Instructions

## 执行要求

本文件是仓库级 AI 编码代理指令。修改代码、公共 API、运行语义、架构或相关文档前，必须完整阅读并遵守下面的
架构宪法。

- 实现、测试和文档必须与当前架构宪法一致。
- 以最新架构为准。除非用户明确要求，不保留旧别名、隐式转换、弃用路径、兼容分支或历史架构描述。
- 有意进行的设计调整若与宪法冲突，必须同步修改本文件、实现、测试和相关手册章节，不得绕过冲突。
- 架构宪法保持规范、简洁；原理解释和使用教程写入 `docs/` 中的编号章节。

## 架构宪法

状态：Normative
适用范围：核心 Runtime、扩展适配器与领域框架

## 第一条：Less is more

1. 概念是用户必须知道才能使用的东西；能做成方法、参数或返回值的，不要求用户导入新的类名。能力靠组合少数原语获得，
   不为每个场景新增机制。
2. 内部责任划分不自动升级为用户概念。
3. 删除错误抽象优先于维护错误抽象的兼容层。
4. 新能力能由 Graph、Event、Runtime 和领域组件组合时，不进入核心。
5. 新用户概念必须证明不能由现有概念承担，且至少服务两个不同领域。
6. 概念按层暴露：写业务与编排只用顶层名字及其方法参数；汇聚策略、显式连接、执行计划、执行句柄与共享 Slot 池位于
   `interlace.engine`；插件位于 `interlace.plugins`；适配器协议位于 `interlace.spi`。

顶层 API 固定为：

```text
Graph、Node、Output、Runtime、Context、Event
```

`interlace.engine` 固定为上述名字加 `InputPolicy、Edge、ExecutionPlan、Execution、Slot、SlotPool`；执行状态枚举为
`Execution.Status`，执行限制 `ExecutionLimits` 与 `InputSelector` 属于 `interlace.spi`。

## 第二条：局部数据流与跨图事件分离

1. Output 只在当前 Graph 内沿 Edge 传播。
2. Event 只通过 `Context.emit()` 或 `Runtime.emit()` 进入跨图流程。
3. Node 返回值不得隐式转换为 Event，Event 不得隐式转换为 Edge 数据。
4. 目标 Graph 不在源 Node 的 emit 调用栈内执行。
5. 跨图事件使用 imperative `emit()`，不得依靠 `yield` 的多义语义。

## 第三条：Graph 是静态有向定义

1. Node 以普通映射 `inputs`、`outputs` 声明端口名到普通 Python 类的类型；Graph 冻结时复制为有序只读快照，
   冻结后修改声明不影响已冻结 Graph。`inputs` 为空的节点是源节点：作为入口时在启动时触发一次，不读取输入策略。
2. 输入策略是按名称引用 selector 的 `InputPolicy(name, config)`，内建 ALL（默认）、ANY 与 LONGEST；selector 只根据
   端口与可用 token 数量选择组合，不读取领域值。
3. Graph 进入 Runtime 前必须冻结并校验可达性和类型兼容；普通 Edge 可以组成环。
4. Node ID 属于 Graph binding，Node 不保存某次 execution 状态。
5. Graph 内循环由 Output 沿回边继续传值，并在不再产生可执行数据时自然结束。
6. 无下游 Edge 的 Output 由 `Runtime.run()` 返回。
7. ExecutionPlan 从冻结 Graph 选择节点、可选独立入口和原有 Edge；缺省入口沿用原图，缺省连接保留所选节点间的全部边。
   不自动补边或改变节点行为；按实际入口绑定输入并校验可达性及其他 ALL 节点的完整输入。
   无计划内下游的端口是终端输出；可声明完整预期 outputs，在执行前拒绝出口不符，不过滤结果或隐式截断流程。
   outputs、cut_outputs、boundary_edges 和 describe() 提供不可变边界或独立诊断快照；静态可达不保证运行时产出。
   ExecutionPlan 按身份比较，变更选择须构造新计划；已冻结 Graph 不接受新的 selector 快照。
8. Graph.compose() 以节点映射、显式入口和 Edge 集合快捷组装普通 Graph，不推断入口或端口；返回值保持构建状态，
   复用 add/connect 与原有冻结校验，不引入第二套图定义或执行语义。

## 第四条：Event 是最小事实

1. Event 核心只携带 `type` 和 `payload`。
2. 关联 ID、时间、来源、幂等键和 tracing 数据属于领域 payload 或外部观察者。
3. `emit()` 成功返回表示事件传输已接受 Event。
4. 已接受 Event 不因源 Graph 后续失败而撤回。
5. Runtime 接受每一个 Event，不比较 payload，不做领域去重。

## 第五条：Runtime 是门面，PluginHost 是装配根

1. Runtime 注册 Graph，用 `on(event, graph, queue, concurrency)` 组合路由与本地消费，并用 `observe()` 独立注册
   Event 观察者；高级组合可以分别调用 `route()` 与 `consume()`。这些组合接口与钩子登记统一返回可注销的
   Registration。Runtime 通过 `hooks` 公开唯一的钩子总线。
2. Runtime 只有一条装配路径：全部角色与基础设施都由插件经 PluginHost 提供。未被提供的角色由 LocalRuntimePlugin
   补齐，补齐角色所需或任一插件依赖、装饰却无人提供的基础设施由 LocalInfrastructurePlugin 补齐；不得另设只供
   内建实现使用的装配路径，也不接受绕过 PluginHost 的显式角色注入。
3. Router 组装事件发布和任务投递，Worker 组装任务消费和 Graph 执行。插件提供的 Router/Worker 由 Runtime 关闭；
   插件或其注入的底层组件默认仍由提供者或调用方管理。
4. Runtime 不得直接实现消息持久化、队列算法、Node 执行、插件发现或领域策略。
5. Trigger、Task、Queue、Scheduler、execution record 可以作为内部职责存在，但不要求用户逐项组装。
6. Runtime 与运行角色关闭时先进入 CLOSING，拒绝新的根提交，排空已接受工作后进入 CLOSED；并发 close 等待同一次
   关闭完成。`on()` 在 route 失败时撤销本次新建的消费绑定，不遗留半组合状态。

## 第六条：可替换性通过窄协议获得

1. EventBus 只负责 Event 发布、订阅和投递生命周期；subscribe 返回独立、幂等的注销函数。
2. TaskPublisher 负责工作投递，TaskConsumer 负责命名执行通道、本地并发和消费；TaskBackend 是两者的组合。
   bind 返回独立、幂等的注销函数；注册失败必须回收本次资源，注销和空闲等待必须包含在途交付的 lease 清理。
   WorkerRole.consume 返回仅撤销本次新建消费绑定的幂等释放函数。默认内存通道只向已有消费者的通道接受 Work，
   未消费或已退订的通道立即拒绝提交。
3. GraphExecutor 只负责执行冻结 Graph，通过 Execution 的公开输出接口交付结果；同步返回 None，异步返回
   Awaitable[None]。执行宿主管理 start/succeed/fail，不依靠整批返回值补发输出。
4. EventBus、任务传输和 GraphExecutor 必须可以独立替换和组合，不得合并为万能 Backend。
5. SPI 位于高级扩展层，不进入顶层 `interlace` API。
6. Runtime 和插件装配层只依赖公开角色或能力协议，不得使用默认内存实现的私有状态。
7. RouterRole、WorkerRole 和 SlotProvider 按结构化协议替换，不要求继承默认实现。ExecutionFactory 可注入
   OutputStore 与 ExecutionNotifier，默认实现与第三方实现均经同一装配路径。WorkerRole 即 ExecutionHost，
   RouterRole 公开 publish 与 publish_local；扩展行为经钩子总线完成，不进入角色协议。

## 第七条：可靠性不得夸大

1. 当前内存实现不得宣称持久 broker、消息 ack、跨进程租约、恢复或 exactly-once。
2. 核心不自动重试；emit 后的失败不会撤回已发布 Event。
3. Redis/MQ 适配器必须明确自身 delivery、ack、retry 和 failure 语义。
4. URL、tool_call_id、batch_id 和外部副作用幂等属于领域 Graph/Store。

## 第八条：Context 保持狭窄

1. Context 提供 `emit()`、当前逻辑执行链的 `slot`、本次配置 `options`、取消查询 `cancelled`、协作式
   `checkpoint()`、按 Node/插件命名空间隔离的 execution-local `state()`、静止阶段 `on_quiescence()`
   （仅在数据流自然静止且执行成功前调用），以及只能触发、不能登记的钩子视图 `hooks`，供领域代码调用插件定义的
   钩子点。
2. 数据库、HTTP client、LLM provider、领域 Store 和 tracing 通过 Node 构造器或观察者注入。
3. Node 不得获得 Runtime 内部工作请求、TaskBackend 或 executor。
4. Context.options 提供当前 Graph execution 的配置，顶层只读，每次 Node firing 深复制嵌套值。
   Runtime 的 start/run/arun/iter/aiter 接受 options 并在提交前深复制；Work.options 供任务适配器传递。
   配置键使用领域命名空间，值必须可深复制，不保存 Client 等资源；核心不解释下载或模型推理等领域选项。
   配置不沿 Output 改写、不随 emit 继承，跨图事件创建的新 execution 默认使用空配置。
5. Graph(context_factory=...) 按图固定同步 Context 工厂，默认 Context.make；工厂每次 Node firing 接收已绑定的
   基础 Context 与节点中间件改写前的只读输入，返回本次独立 Context。领域工厂自行定义构造与 make 逻辑，
   通过 Context(parent=base) 组合基础上下文；emit、取消、Slot、state 的节点命名空间隔离、options 快照、钩子视图和
   静止回调均委托给基础上下文，不复制内部执行状态。Context 公开构造只接受 parent；基础上下文由执行器通过内部
   绑定接口创建，不作为公共构造参数；核心不解释领域字段。
   Node 与该次调用的中间件使用同一返回实例，工厂不得缓存或跨调用复用 Context，不得改写基础执行能力契约。
6. 领域字段由工厂或领域节点从 typed inputs 绑定，跨节点数据通过 Output 传递；Context 实例和附加字段不自动沿 Edge、
   Event 或跨进程传播，也不因同名 state 命名空间而跨 Node 共享。目标图使用自身 context_factory。
   工厂执行计入本次 Node firing 的步数与超时，错误和取消按原执行契约传播；替代 GraphExecutor 必须遵守同一约定。

## 第九条：同步与异步共享语义

1. 所有节点行为统一继承 Node，execute 可以使用 def 或 async def 声明。
2. execute 返回 Output、Iterable[Output]、None 或得到这些结果的 Awaitable；执行器统一等待 Awaitable，
   对最终结果应用同一输出校验和执行控制。异步生成器不属于节点返回契约。
3. queue concurrency 限制完整 Graph execution，而不是单个 Node。
4. 所有 Node 共享同一 InputPolicy、Output、Edge 和 Event 语义。
5. Node 默认必须无状态且可重入；跨 Work execution 状态放入 Slot，不得隐式存放在共享 Node 实例中。

## 第十条：Slot 跟随逻辑执行链

1. Slot 是进程内执行资源，不绑定线程、Worker 或 Consumer；同一进程内的 Work 跨 Consumer 流转时必须携带同一个
   Slot。
2. 根 Work 从 Consumer 配置的 SlotPool 获取 Slot，整个逻辑链结束后自动归还。
3. 分支 Work 可以共享 Slot，但同一个 Slot 的 Graph execution 不得并发执行。
4. `concurrency` 限制 Consumer 的本地 Graph execution；Slot 池限制逻辑执行链，两者相互独立。`slots=` 接受整数
   （创建该容量的自有池）或 SlotProvider（可被多个 Consumer 共享），缺省时自有池容量等于 concurrency。
5. 等待 Slot 的根 Work 不得占用 Consumer 的执行线程，也不得阻塞已携带 Slot 的延续 Work。
6. Slot、SlotPool 和进程内 lease 不跨进程序列化；Work 穿过进程或消息边界后开始新的本地 Slot 链，远程适配器不得
   宣称保留原进程的 Slot 连续性。
7. 适配器通过 `interlace.spi.SlotProvider` 的公开申请和可用通知接口取得 `SlotLease`，通过 `Delivery.slot_lease` 传递；
   lease 提供引用管理和串行 execution 能力，不暴露内部锁。每个接管的引用必须释放，执行期间的引用由 lease 保护。
   已关闭的 SlotProvider 申请时抛出 SlotProviderClosedError；Consumer 据此拒绝再也无法分配的根 Work，
   其他申请异常视为暂时失败，Work 保留待推进。

## 第十一条：Execution 控制必须默认开放

1. 一次 Node firing 计为一步；`max_steps=0` 表示不限制步数。`max_queued=0` 表示不限制 Graph 内全部输入端口
   待消费 token 的总数；正数为硬上限，超过时以 QueueLimitExceededError 结束为 QUEUE_LIMITED，不阻塞生产者、
   不丢弃 token。替代 GraphExecutor 必须遵守同一计数与终止语义。
2. Graph execution 的 `timeout=None` 和 Node 的 `timeout=None` 表示各自不限制时长，正数统一使用秒。
3. 取消和同步 Node timeout 是协作式语义；核心不得宣称能够安全强杀任意 Python 函数。
4. 控制异常不得被节点中间件或任何 Wrap 中间件当作普通业务异常恢复。
5. 跨 Graph 的每个 Work 是独立 execution，独立计步和计时。
6. Execution 是同步等待、异步等待和 terminal Output 流的统一句柄；便利接口不得维护不同执行语义。
   ExecutionPlan 只约束当前执行，选中节点的 Event、外部写入及新 Work 不因计划而被屏蔽或自动继承计划。
7. 已交给流消费者的 terminal Output 不因后续 Graph 失败而撤回。
8. 输出存储与等待通知实现可替换；存储必须维持追加顺序和可重放性。完整结果按需读取，流式消费不得强制物化
   全量结果；同步与异步交付共享背压、取消和超时约束。Runtime 关闭时经 WorkerRole.begin_close() 解除未结束执行的
   订阅背压，剩余输出留在存储中供订阅方读完，未读订阅不能阻止排空已接受工作。

## 第十二条：扩展只有一种机制

1. 扩展物只有两类：钩子点与具名值。核心与插件以同一方式定义和使用它们；新增扩展能力优先定义钩子点或约定具名值
   名称，不得增加互不相干的全局注册表或专用贡献通道。
2. 钩子点只有 Notice（只读通知）与 Wrap（中间件）两种模式，模式、作用域键、可选的内层值类型以及 Wrap 是否必须
   有处理函数在定义时固定；必须有处理函数的 Wrap 在调用作用域内无人承接时拒绝操作，核心钩子点均为放行。
   只改值的扩展以 Wrap 改写传给 call_next 的值。登记保存在每个 Runtime 唯一的 HookBus。登记随时可以进行，
   调用读取登记快照，进行中的调用不受影响。Notice 在触发方路径上同步执行；移出路径的投递是普通 Notice 处理函数，
   使用有界队列、满时丢弃计数，不阻塞触发方。
3. Notice 处理函数失败记录后隔离，不得改变业务执行结果或 `emit()` 结果；Wrap 中间件的失败按所在操作的
   错误契约传播。Wrap 中间件吞掉或替换 call_next 抛出的执行控制异常、非 Exception 的 BaseException、节点层的
   Engine 契约错误或 StopGraph 时，宿主在交付替代结果之前重新抛出原异常。同步入口拒绝返回 awaitable 的处理函数。
4. 核心在固定边界提供钩子点：EVENT_EMIT、WORK_ROUTE、WORK_DELIVER、EXECUTION_RUN、NODE_CALL、NODE_INSPECT 与
   RUNTIME_EVENT；Runtime 的启动与关闭是 RUNTIME_EVENT 的 runtime.started、runtime.closing、runtime.closed 事实。
   这些点只能改写其值或包裹其操作，不得改写 Graph 冻结、端口、Event 与 Execution 契约；EXECUTION_RUN 只能替换
   inputs 与 options，WORK_DELIVER 不得替换 Slot lease。
5. 具名值以命名空间名称在 HookBus 上提供，同名唯一，读取方在使用时查找。输入 selector 是与 InputPolicy 同名的
   具名值，只能选择当前非空端口，每个 firing 对每个端口最多消费一个 token；Graph 冻结时绑定当时的快照，缺失名称
   必须在冻结阶段失败。
6. TaskConsumer 以 DeliveryResult 明确表达 ACK、RETRY 或 REJECT；broker lease、重投递和死信实现留给 backend。
7. keyed join、窗口和领域状态属于扩展 Node，不进入 Engine 的固定调度语义。
8. NODE_CALL 中间件是同步的流式 Wrap：Engine 逐项推进 Node 输出，中间件迭代 call_next 时才推进源，结束或失败时
   关闭其返回的迭代器；迭代阶段的控制边界、取消、超时、清理与直接调用一致。传给 call_next 的调用只能替换输入值，
   中间件输出与 StopGraph 携带的输出遵守节点输出端口契约。需要异步接口时经 NodeCall.wait 在执行约束下等待。
9. NODE_INSPECT 同步采集实际 Node 调用边界：输入位于全部中间件改写之后，原始输出位于中间件改写之前；不得修改
   参数或恢复异常，返回值不参与执行，采集失败隔离。触发身份通过观测作用域关联。

## 第十三条：公共行为必须可验证

1. Graph 冻结与执行约束必须有失败测试。
2. Event 提交、跨图连接、并发和错误传播必须有契约测试。
3. 三个替换协议必须有非默认实现组合测试。
4. 插件依赖、冲突、装饰顺序、API 兼容、失败回滚、defer 逆序清理、运行期安装与卸载、设置校验、包依赖方向和默认
   装配路径必须有契约测试。
5. 示例只通过公开 API 组合，优先使用顶层名字，并覆盖核心编排方式。
6. 文档不得把计划能力写成已实现能力。

## 第十四条：核心实现可替换，公共契约保持稳定

1. Graph、Node、端口声明、Edge、Output、Event、Execution 的基本语义、冻结校验规则和错误契约属于微内核，插件不得
   改写这些契约；满足相同契约的核心实现可以通过公开协议替换。
2. 部署能力和非本质策略通过声明为单例 capability 的具名值扩展；默认实现必须与第三方实现经过同一 PluginHost 装配路径。
3. 插件只有声明与 `setup(api)`，函数与类形式能力相同。声明包含 namespaced ID、插件版本、SPI 主版本、依赖的
   capability、提供与装饰的 capability 以及可选的设置类型；插件之间只按 capability 名称依赖，不引用其他插件 ID。
   宿主按依赖顺序 setup。capability 的提供者先于全部装饰者装配，全部装饰者先于其余依赖方，后装配的装饰者位于外层。
4. 单例 capability 冲突、缺失依赖、循环依赖、SPI 不兼容、装饰无人提供的 capability、未兑现的 provides 或
   wraps 声明，以及不合法或指向未知插件的设置必须在 Runtime 可用前失败。设置是冻结 dataclass 声明的标量数据，
   由宿主校验后经只读 `api.settings` 交给插件，不静默回退默认值。
5. 扩展接口只有 on、provide、wrap、require、defer 与只读的 settings。声明的 capability 只能由声明者在 setup 期提供
   并冻结，其他具名值与钩子随时登记；资源清理以 defer 登记。宿主关闭和启动回滚时先逆序注销各插件的登记，再按
   登记逆序执行全部清理，setup 中途失败前登记的清理同样执行。宿主运行期间可安装不提供、不装饰 capability 的插件，
   返回 Registration；卸载、安装失败回滚与关闭对该插件使用同一清理顺序，运行期插件先于启动插件卸载。
6. 插件不得访问 Runtime 私有状态；替代组件需要钩子能力时以插件扩展接口取得的钩子总线构造。

## 第十五条：Pythonic 实现与中文注释

1. 代码实现应符合 Python 惯用方式，接口设计简洁、清晰、优美；优先使用标准语言特性和窄协议，避免不必要的
   包装、重复抽象和隐式行为。
2. 模块、类、函数、方法及行内说明使用中文；代码标识符、标准格式标题和工具指令保留其原有语法。
3. 公开模块、类、函数和方法必须提供 Google 风格文档字符串：以中文概述职责，按实际契约填写 Args、Returns、
   Yields、Raises；无对应内容时省略该节。私有函数、方法与嵌套函数至少提供一句中文概述，仅在参数、返回值或
   异常语义无法从名称和类型看出时补充对应节。
4. 文档字符串必须准确描述所在签名的实际类型、单位、取值与行为；不得跨函数复用与签名不符的模板化描述，
   宁可省略也不写泛化套话。
5. 成员字段（含类属性和私有实例字段）必须有中文说明，可使用字段旁注释或类文档字符串的 Attributes 节，明确
   含义及必要的单位、默认值、所有权或可变性边界。实现、测试和示例均遵守本条。

## 第十六条：官方服务按需启用

1. RPC 与图编辑服务属于官方 `interlace.service` 扩展层，通过 `interlace[service]` 安装；基础包保持零运行时依赖，
   导入 `interlace` 不加载服务依赖或监听端口。
2. 服务仅通过 Runtime、Execution、冻结 Graph 与公开 GraphCatalog 能力调用和检查图，不读取运行角色私有状态；
   服务的观察与诊断登记以运行期插件安装，随服务关闭或 Runtime 关闭卸载。
3. HTTP/JSON、JSON-RPC 与图编辑接口共享执行服务和 Execution 契约；协议适配器不实现第二套执行器。服务只提供
   数据接口，不托管前端页面。
4. 可编辑节点只能由应用显式登记的工厂与配置模型构建；草稿可不完整，发布必须经过核心冻结校验。
5. 发布版本不可变，执行记录保存当时的图结构；草稿更新比较修订号。任意 Python 构造代码不隐式转换为编辑文档。
6. 生命周期观测不携带节点业务输入对象；服务可通过 NODE_INSPECT 独立快照输入、配置和原始输出，并通过
   观测作用域关联 Python logging。诊断记录按 execution、node 与 firing 隔离，允许关闭采集，不改变原对象或业务结果。
   诊断快照按字段名片段与文本凭据形态脱敏；服务以 execution.finished 事件记录终态。
7. 服务历史、诊断与终端输出存储不代表任务恢复、broker 持久化或跨进程 Slot 延续；执行历史按 retention 上限清理。
8. 项目属于服务管理层，按“项目、逻辑定义、版本与运行实例”组织管理接口；不改变 Runtime 注册名与执行语义。
   已有定义通过数据迁移归入默认项目，移动定义时其草稿、版本与历史保持整体归属，定义标识仍全局唯一。

## 验证要求

行为变更应先运行相关测试，并在可行时运行完整检查：

```bash
uv run pytest -q
uv run --extra service mypy interlace
uv run ruff check interlace tests examples
uv run ruff format --check interlace tests examples
```

文档变更还应检查本地链接、Markdown 围栏和发生变化的 Mermaid 图。

服务变更运行 `uv run --extra service pytest tests/service -q`。

发布使用与 `pyproject.toml` 版本一致的 `v<version>` Release 标签，完整 CI 通过后构建并严格检查 wheel/sdist 元数据。
PyPI 上传通过 `python-publish.yml` 和 `pypi` environment 的 Trusted Publisher 授权，只在上传作业授予 OIDC 权限。
源码、构建产物与已上传版本必须区分；下游正式依赖仅在发行版已上传并验证可安装后切换，不借用本地路径或其他仓库凭据。
