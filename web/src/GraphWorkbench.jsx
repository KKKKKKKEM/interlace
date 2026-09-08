import React, { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  ReactFlow,
  Background,
  Handle,
  Position,
  applyNodeChanges,
  applyEdgeChanges,
  addEdge,
  useReactFlow,
  useViewport,
  MarkerType,
} from "@xyflow/react";
import {
  Activity,
  Check,
  CheckCheck,
  ChevronRight,
  Circle,
  Download,
  GitBranch,
  Play,
  PanelBottomClose,
  PanelBottomOpen,
  Maximize,
  Minus,
  Plus,
  Redo2,
  Save,
  Square,
  Trash2,
  Undo2,
  X,
  Workflow,
} from "lucide-react";
import { createParser } from "eventsource-parser";
import NodeRunDetails, { firingState } from "./NodeRunDetails.jsx";
import { Tool, ResizeHandle } from "./ui.jsx";

const names = {
  not_run: "未执行",
  pending: "排队中",
  running: "运行中",
  succeeded: "已完成",
  failed: "失败",
  cancelled: "已取消",
  timed_out: "已超时",
  node_timed_out: "节点超时",
  step_limited: "达到步数上限",
  interrupted: "已中断",
};
const empty = () => ({ name: "", entrypoint: "", nodes: [], edges: [] });
const copy = (value) => structuredClone(value);
const errorText = (error) =>
  typeof error === "string" ? error : JSON.stringify(error);

/** 以真实端口渲染可连接的节点。 */
function GraphNode({ id, data, selected }) {
  return (
    <div
      className={`graph-node ${selected ? "selected" : ""} ${data.state || ""}`}
    >
      <div className="node-title">
        <span className={`node-symbol ${data.entry ? "entry" : ""}`}>
          {data.entry ? <GitBranch size={17} /> : <Workflow size={17} />}
        </span>
        <div className="node-heading">
          <strong title={id}>{id}</strong>
          <div className="node-kind" title={data.title || data.type}>
            {data.title || data.type}
          </div>
        </div>
        {data.entry && <span className="entry-label">入口</span>}
      </div>
      <div className="ports">
        <div>
          {Object.entries(data.inputs || {}).map(([port, type]) => (
            <div className="port in" key={port}>
              <Handle id={port} type="target" position={Position.Left} />
              <span>{port}</span>
              <small>{type}</small>
            </div>
          ))}
        </div>
        <div>
          {Object.entries(data.outputs || {}).map(([port, type]) => (
            <div className="port out" key={port}>
              <span>{port}</span>
              <small>{type}</small>
              <Handle id={port} type="source" position={Position.Right} />
            </div>
          ))}
        </div>
      </div>
      <div
        className={`node-state ${data.state ? "" : "empty"}`}
        aria-hidden={!data.state}
      >
        <Circle size={9} />
        {names[data.state] || data.state}
        {data.count > 1 && ` · ${data.count} 次触发`}
      </div>
    </div>
  );
}
const nodeTypes = { graph: GraphNode };

/** 按配置 Schema 渲染表单，复杂结构使用严格 JSON 输入。 */
function ConfigFields({ schema = {}, value, onChange }) {
  const properties = schema.properties || {};
  return (
    <>
      {Object.entries(properties).map(([key, field]) => {
        const current = value[key] ?? field.default;
        const set = (next) => onChange({ ...value, [key]: next });
        return (
          <label className="field" key={key}>
            <span>
              {field.title || key}
              {schema.required?.includes(key) && <b className="required"> *</b>}
            </span>
            {field.enum ? (
              <select
                value={current ?? ""}
                onChange={(e) =>
                  set(field.enum.find((v) => String(v) === e.target.value))
                }
              >
                <option value="" disabled>
                  请选择
                </option>
                {field.enum.map((v) => (
                  <option key={String(v)} value={String(v)}>
                    {String(v)}
                  </option>
                ))}
              </select>
            ) : field.type === "boolean" ? (
              <input
                type="checkbox"
                checked={current ?? false}
                onChange={(e) => set(e.target.checked)}
              />
            ) : ["number", "integer"].includes(field.type) ? (
              <input
                type="number"
                value={current ?? ""}
                min={field.minimum}
                max={field.maximum}
                step={field.type === "integer" ? 1 : "any"}
                onChange={(e) =>
                  set(
                    e.target.value === "" ? undefined : Number(e.target.value),
                  )
                }
              />
            ) : field.type === "string" ? (
              <input
                value={current ?? ""}
                onChange={(e) => set(e.target.value)}
              />
            ) : (
              <JsonField value={current ?? null} onChange={set} />
            )}
          </label>
        );
      })}
      {!Object.keys(properties).length && <div className="quiet">无配置项</div>}
    </>
  );
}

/** 用同一套图标工具控制画布缩放并显示当前比例。 */
function CanvasTools() {
  const { zoomIn, zoomOut, fitView } = useReactFlow();
  const { zoom } = useViewport();
  return (
    <div
      className="canvas-tools material-glass"
      role="toolbar"
      aria-label="画布视图"
    >
      <Tool
        icon={Minus}
        label="缩小画布"
        disabled={zoom <= 0.2}
        onClick={() => zoomOut()}
      />
      <span className="zoom-value">{Math.round(zoom * 100)}%</span>
      <Tool
        icon={Plus}
        label="放大画布"
        disabled={zoom >= 2}
        onClick={() => zoomIn()}
      />
      <span className="separator" />
      <Tool
        icon={Maximize}
        label="适应画布"
        onClick={() => fitView({ padding: 0.2, maxZoom: 1 })}
      />
    </div>
  );
}

/** 保留未完成的 JSON 编辑，在解析成功后同步值。 */
function JsonField({ value, onChange }) {
  const [text, setText] = useState(JSON.stringify(value, null, 2));
  const [invalid, setInvalid] = useState(false);
  return (
    <>
      <textarea
        value={text}
        aria-invalid={invalid}
        onChange={(e) => {
          setText(e.target.value);
          try {
            onChange(JSON.parse(e.target.value));
            setInvalid(false);
          } catch {
            onChange(e.target.value);
            setInvalid(true);
          }
        }}
      />
      {invalid && <small className="error-text">JSON 格式不完整</small>}
    </>
  );
}

/** 工作台共享已有图、可编辑草稿和运行事件。 */
export default function GraphWorkbench({
  definitionName,
  projectId,
  graphName,
  runId,
  allowedGraphs,
  autoRun = false,
  showRuns = false,
  accessToken,
  onAccessToken,
  onDirtyChange,
  onPublished,
  onEditDraft,
  onRunStarted,
  actionsTarget,
}) {
  const [token, setToken] = useState(
    sessionStorage.getItem("interlace.token") || "",
  );
  const [tokenInput, setTokenInput] = useState(token);
  const [auth, setAuth] = useState(false);
  const [graphs, setGraphs] = useState([]);
  const [drafts, setDrafts] = useState([]);
  const [catalog, setCatalog] = useState([]);
  const [runs, setRuns] = useState([]);
  const [definition, setDefinition] = useState(empty);
  const [revision, setRevision] = useState(0);
  const [registered, setRegistered] = useState("");
  const [editable, setEditable] = useState(false);
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [selected, setSelected] = useState(null);
  const [selectedStep, setSelectedStep] = useState(null);
  const [selectedEdge, setSelectedEdge] = useState(null);
  const [dirty, setDirty] = useState(false);
  const [message, setMessage] = useState(null);
  const [busy, setBusy] = useState(false);
  const [dialog, setDialog] = useState(null);
  const [inputText, setInputText] = useState("{}");
  const [optionsText, setOptionsText] = useState("{}");
  const [timeout, setTimeoutValue] = useState("");
  const [maxSteps, setMaxSteps] = useState(0);
  const [panel, setPanel] = useState("runs");
  const [panelHeight, setPanelHeight] = useState(256);
  const [panelLimit, setPanelLimit] = useState(480);
  const [panelCollapsed, setPanelCollapsed] = useState(false);
  const [activeRun, setActiveRun] = useState(null);
  const [observations, setObservations] = useState([]);
  const [outputs, setOutputs] = useState([]);
  const [, renderHistory] = useState(0);
  const history = useRef({ past: [], future: [] });
  const current = useRef({});
  const booted = useRef(false);
  const autoOpened = useRef(false);
  // 面板上限跟随工作区高度，始终保留可操作的画布。
  const workspaceRef = useRef(null);
  const { fitView } = useReactFlow();
  current.current = { definition, nodes, edges };

  useEffect(() => {
    const element = workspaceRef.current;
    const observer = new ResizeObserver(() => {
      setPanelLimit(
        Math.max(180, Math.min(480, Math.floor(element.clientHeight * 0.6))),
      );
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    setToken(accessToken || "");
    setTokenInput(accessToken || "");
    setAuth(false);
  }, [accessToken]);
  useEffect(() => {
    onDirtyChange?.(dirty);
  }, [dirty, onDirtyChange]);
  useEffect(() => () => onDirtyChange?.(false), [onDirtyChange]);

  const api = useCallback(
    async (path, body) => {
      const response = await fetch(`./api/${path}`, {
        method: body === undefined ? "GET" : "POST",
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      if (response.status === 401) setAuth(true);
      const value = await response.json();
      if (!response.ok) throw new Error(errorText(value.detail || value));
      return value;
    },
    [token],
  );

  const refresh = useCallback(async () => {
    const [g, d, c, r] = await Promise.all([
      api("graphs"),
      api("drafts"),
      api("nodes"),
      api("executions"),
    ]);
    setGraphs(g);
    setDrafts(d);
    setCatalog(c);
    setRuns(r);
    return g;
  }, [api]);

  const attempt = async (action) => {
    setBusy(true);
    setMessage(null);
    try {
      await action();
    } catch (error) {
      setMessage({ error: true, text: error.message });
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    refresh().catch((error) =>
      setMessage({ error: true, text: error.message }),
    );
  }, [refresh]);
  useEffect(() => {
    const timer = setInterval(
      () =>
        api("executions")
          .then(setRuns)
          .catch(() => {}),
      1500,
    );
    return () => clearInterval(timer);
  }, [api]);
  useEffect(() => {
    const protect = (event) => {
      if (dirty) {
        event.preventDefault();
        event.returnValue = "";
      }
    };
    window.addEventListener("beforeunload", protect);
    return () => window.removeEventListener("beforeunload", protect);
  }, [dirty]);
  useEffect(() => {
    if (!dialog && !auth) return;
    const previous = document.activeElement;
    const modal = document.querySelector('[role="dialog"]');
    modal?.querySelector("input, textarea, button")?.focus();
    const keyboard = (event) => {
      if (event.key === "Escape") {
        setDialog(null);
        setAuth(false);
      }
      if (event.key !== "Tab") return;
      const fields = [
        ...modal.querySelectorAll(
          "button:not(:disabled), input, textarea, select",
        ),
      ];
      if (event.shiftKey && document.activeElement === fields[0]) {
        event.preventDefault();
        fields.at(-1)?.focus();
      } else if (!event.shiftKey && document.activeElement === fields.at(-1)) {
        event.preventDefault();
        fields[0]?.focus();
      }
    };
    document.addEventListener("keydown", keyboard);
    return () => {
      document.removeEventListener("keydown", keyboard);
      previous?.focus();
    };
  }, [dialog, auth]);

  const resetHistory = () => {
    history.current = { past: [], future: [] };
    renderHistory((v) => v + 1);
  };
  const remember = () => {
    history.current.past.push(copy(current.current));
    history.current.past = history.current.past.slice(-50);
    history.current.future = [];
    renderHistory((v) => v + 1);
    setDirty(true);
  };
  const restore = (from, to) => {
    const value = history.current[from].pop();
    if (!value) return;
    history.current[to].push(copy(current.current));
    setDefinition(value.definition);
    setNodes(value.nodes);
    setEdges(value.edges);
    setDirty(true);
    renderHistory((v) => v + 1);
  };

  const draw = (def, structure, canEdit, name, rev = 0) => {
    setDefinition(copy(def));
    setEditable(canEdit);
    setRegistered(name);
    setRevision(rev);
    setNodes(
      def.nodes.map((node, index) => {
        const actual = structure?.nodes.find((n) => n.id === node.id);
        const columns = innerWidth < 700 ? 1 : 3;
        return {
          id: node.id,
          type: "graph",
          position: node.position || {
            x: 70 + (index % columns) * 350,
            y: 65 + Math.floor(index / columns) * 215,
          },
          data: {
            ...actual,
            type: node.type,
            config: node.config || {},
            entry: def.entrypoint === node.id,
          },
        };
      }),
    );
    setEdges(
      def.edges.map((edge, index) => ({
        id: `e${index}`,
        type: "smoothstep",
        source: edge.source,
        target: edge.target,
        sourceHandle: edge.source_port,
        targetHandle: edge.target_port,
        markerEnd: { type: MarkerType.ArrowClosed },
        label: edge.source_port,
      })),
    );
    setSelected(null);
    setSelectedEdge(null);
    setDirty(false);
    resetHistory();
    setActiveRun(null);
    window.setTimeout(
      () =>
        fitView({
          padding: 0.2,
          minZoom: showRuns ? 0.2 : innerWidth < 700 ? 0.65 : 0.2,
          maxZoom: 1,
        }),
      60,
    );
  };

  useEffect(() => {
    const timer = window.setTimeout(
      () => fitView({ padding: 0.2, maxZoom: 1 }),
      60,
    );
    return () => window.clearTimeout(timer);
  }, [Boolean(selected), Boolean(selectedEdge), fitView]);

  const mayLeave = () =>
    !dirty || window.confirm("当前修改尚未保存，是否离开？");
  const openGraph = (graph) => {
    if (!mayLeave()) return;
    const saved = graph.publication?.definition;
    const def = saved || {
      name: graph.name,
      entrypoint: graph.entrypoint,
      nodes: graph.nodes.map((n) => ({ id: n.id, type: n.type })),
      edges: graph.edges,
    };
    // 已发布版本始终只读，编辑操作显式打开最新草稿。
    draw(def, graph, false, graph.name);
  };
  const openDraft = async (draft) => {
    if (!mayLeave()) return;
    const previews = await Promise.all(
      draft.definition.nodes.map(async (n) => {
        try {
          return { id: n.id, ...(await api("nodes/preview", n)) };
        } catch {
          return { id: n.id };
        }
      }),
    );
    draw(draft.definition, { nodes: previews }, true, "", draft.revision);
  };
  useEffect(() => {
    if (booted.current) return;
    const draft = drafts.find(
      (item) => item.definition.name === definitionName,
    );
    const candidates = graphs.filter(
      (item) =>
        item.name === definitionName ||
        item.publication?.name === definitionName,
    );
    candidates.sort(
      (a, b) => (b.publication?.version || 0) - (a.publication?.version || 0),
    );
    if (runId) {
      booted.current = true;
      attempt(async () => {
        const run = await api(`executions/${encodeURIComponent(runId)}`);
        if (!allowedGraphs.includes(run.graph))
          throw new Error("运行实例不属于当前定义");
        openGraph(run.graph_snapshot);
        setActiveRun(run);
        setPanel("timeline");
      });
    } else if ((graphName === "draft" || !candidates.length) && draft) {
      booted.current = true;
      attempt(() => openDraft(draft));
    } else {
      const graph =
        candidates.find((item) => item.name === graphName) || candidates[0];
      if (graph) {
        booted.current = true;
        openGraph(graph);
      }
    }
  }, [graphs, drafts, definitionName, graphName, runId]);

  const documentValue = () => ({
    ...definition,
    nodes: nodes.map((n) => ({
      id: n.id,
      type: n.data.type,
      config: n.data.config || {},
      position: n.position,
    })),
    edges: edges.map((e) => ({
      source: e.source,
      target: e.target,
      source_port: e.sourceHandle || "default",
      target_port: e.targetHandle || "default",
    })),
  });

  const save = async () => {
    const value = await api("drafts", {
      definition: documentValue(),
      revision,
      project_id: projectId,
    });
    setRevision(value.revision);
    setDirty(false);
    await refresh();
    setMessage({ text: "草稿已保存" });
  };
  const publish = async () => {
    const value = await api("publish", {
      definition: documentValue(),
      revision,
      project_id: projectId,
    });
    setRevision(value.revision);
    setDirty(false);
    const updated = await refresh();
    const graph = updated.find((g) => g.name === value.graph);
    draw(graph.publication.definition, graph, false, graph.name);
    setMessage({ text: `已发布 ${value.graph}` });
    onDirtyChange?.(false);
    onPublished?.(value);
  };
  const addNode = (type) => {
    remember();
    const base =
      type.type
        .split(/[./]/)
        .at(-1)
        .replace(/[^\w-]/g, "") || "node";
    let id = base;
    let index = 2;
    while (nodes.some((n) => n.id === id)) id = `${base}_${index++}`;
    const config = Object.fromEntries(
      Object.entries(type.schema.properties || {})
        .filter(([, p]) => p.default !== undefined)
        .map(([key, p]) => [key, p.default]),
    );
    const node = {
      id,
      type: "graph",
      position: {
        x: 80 + (nodes.length % 3) * 350,
        y: 80 + Math.floor(nodes.length / 3) * 215,
      },
      data: {
        type: type.type,
        title: type.title,
        config,
        entry: !nodes.length,
      },
    };
    setNodes((old) => [...old, node]);
    setSelected(id);
    setDialog(null);
    if (!nodes.length) setDefinition((old) => ({ ...old, entrypoint: id }));
    api("nodes/preview", { id, type: type.type, config })
      .then((ports) =>
        setNodes((old) =>
          old.map((n) =>
            n.id === id ? { ...n, data: { ...n.data, ...ports } } : n,
          ),
        ),
      )
      .catch(() => {});
  };
  const changeConfig = (config) => {
    remember();
    setNodes((old) =>
      old.map((n) =>
        n.id === selected ? { ...n, data: { ...n.data, config } } : n,
      ),
    );
    setDirty(true);
  };
  const selectedNode = nodes.find((n) => n.id === selected);
  const inspectedEdge = edges.find((edge) => edge.id === selectedEdge);
  const selectedType = catalog.find((n) => n.type === selectedNode?.data.type);

  const runDialog = () => {
    const graph = graphs.find((g) => g.name === registered);
    const ports =
      graph?.nodes.find((n) => n.id === graph.entrypoint)?.inputs || {};
    setInputText(
      JSON.stringify(
        Object.fromEntries(
          Object.entries(ports).map(([key, type]) => [
            key,
            type === "str"
              ? ""
              : ["int", "float"].includes(type)
                ? 0
                : type === "list"
                  ? []
                  : type === "bool"
                    ? false
                    : {},
          ]),
        ),
        null,
        2,
      ),
    );
    setDialog("run");
  };

  const startRun = async () => {
    const run = await api("executions", {
      graph: registered,
      inputs: JSON.parse(inputText),
      options: JSON.parse(optionsText),
      timeout: timeout === "" ? null : Number(timeout),
      max_steps: Number(maxSteps),
    });
    setActiveRun(run);
    setPanel("timeline");
    setDialog(null);
    await refresh();
    onRunStarted?.(run);
  };

  useEffect(() => {
    if (autoRun && registered && !autoOpened.current) {
      autoOpened.current = true;
      runDialog();
    }
  }, [autoRun, registered]);

  useEffect(() => {
    if (!activeRun?.id) {
      setObservations([]);
      setOutputs([]);
      return;
    }
    const controller = new AbortController();
    setObservations([]);
    setOutputs([]);
    const read = async (path, onEvent) => {
      const response = await fetch(`./api/executions/${activeRun.id}/${path}`, {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        signal: controller.signal,
      });
      if (!response.ok)
        throw new Error(
          path === "outputs"
            ? "该执行的输出已不在当前进程中"
            : "运行记录读取失败",
        );
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      const parser = createParser({
        onEvent: (event) => onEvent(event.event, JSON.parse(event.data)),
      });
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          parser.feed(decoder.decode(value, { stream: true }));
        }
      } finally {
        reader.releaseLock();
      }
    };
    read("events", (kind, value) => {
      if (kind === "observation") setObservations((old) => [...old, value]);
      if (kind === "finished") {
        setActiveRun((old) => (old?.id === value.id ? value : old));
        setRuns((old) =>
          old.map((item) => (item.id === value.id ? value : item)),
        );
      }
    }).catch((error) => {
      if (error.name !== "AbortError")
        setMessage({ error: true, text: error.message });
    });
    read("outputs", (kind, value) => {
      if (kind === "output") setOutputs((old) => [...old, value]);
      if (kind === "failure") setMessage({ error: true, text: value.detail });
    }).catch((error) => {
      if (error.name !== "AbortError")
        setOutputs([{ unavailable: error.message }]);
    });
    return () => controller.abort();
  }, [activeRun?.id, token]);

  const nodeStates = {};
  if (activeRun?.graph === registered)
    observations.forEach((event) => {
      if (
        !event.node ||
        !["node.started", "node.finished"].includes(event.kind)
      )
        return;
      const old = nodeStates[event.node] || { count: 0 };
      nodeStates[event.node] = {
        state: event.kind === "node.started" ? "running" : firingState(event),
        count: old.count + (event.kind === "node.started" ? 1 : 0),
      };
    });
  const displayNodes = nodes.map((n) => ({
    ...n,
    data: {
      ...n.data,
      state: activeRun?.graph === registered ? "not_run" : undefined,
      ...nodeStates[n.id],
      entry: n.id === definition.entrypoint,
    },
  }));
  const firings = observations
    .filter((e) => e.kind === "node.started")
    .map((start) => {
      const finish = observations.find(
        (e) =>
          e.kind === "node.finished" &&
          e.node === start.node &&
          e.attributes.step === start.attributes.step,
      );
      return {
        ...start,
        finish,
        duration: finish
          ? new Date(finish.occurred_at) - new Date(start.occurred_at)
          : null,
      };
    });

  const actions = (
    <div className="actions">
      {!editable && graphs.find((g) => g.name === registered)?.publication && (
        <button
          onClick={() =>
            attempt(async () => {
              const d = drafts.find(
                (item) => item.definition.name === definitionName,
              );
              if (d) onEditDraft();
            })
          }
        >
          编辑草稿
        </button>
      )}
      {editable && (
        <>
          <Tool
            icon={Undo2}
            label="撤销"
            disabled={!editable || !history.current.past.length}
            onClick={() => restore("past", "future")}
          />
          <Tool
            icon={Redo2}
            label="重做"
            disabled={!editable || !history.current.future.length}
            onClick={() => restore("future", "past")}
          />
          <span className="separator" />
          <Tool
            icon={Save}
            label="保存草稿"
            disabled={!editable || busy}
            onClick={() => attempt(save)}
          />
          <Tool
            icon={CheckCheck}
            label="校验图"
            disabled={!editable || busy}
            onClick={() =>
              attempt(async () => {
                await api("validate", documentValue());
                setMessage({ text: "图校验通过" });
              })
            }
          />
        </>
      )}
      {(editable || graphs.find((g) => g.name === registered)?.publication) && (
        <Tool
          icon={Download}
          label="导出图定义"
          disabled={
            !editable && !graphs.find((g) => g.name === registered)?.publication
          }
          onClick={() => {
            const url = URL.createObjectURL(
              new Blob([JSON.stringify(documentValue(), null, 2)], {
                type: "application/json",
              }),
            );
            const link = document.createElement("a");
            link.href = url;
            link.download = `${definition.name}.json`;
            link.click();
            URL.revokeObjectURL(url);
          }}
        />
      )}
      {editable ? (
        <button
          className="primary"
          disabled={busy || !nodes.length}
          onClick={() => attempt(publish)}
        >
          <Check size={15} />
          发布
        </button>
      ) : (
        <button
          className="primary"
          disabled={
            !registered ||
            !graphs.some((item) => item.name === registered) ||
            busy
          }
          onClick={runDialog}
        >
          <Play size={15} />
          运行
        </button>
      )}
    </div>
  );

  return (
    <div
      className={`studio definition-workbench ${showRuns ? "with-execution" : "graph-only"}`}
    >
      <main className="workspace" ref={workspaceRef}>
        {!showRuns && actionsTarget && createPortal(actions, actionsTarget)}
        {message && !dialog && !auth && (
          <div
            className={`notice ${message.error ? "error" : ""}`}
            role={message.error ? "alert" : "status"}
          >
            <span>{message.text}</span>
            <Tool icon={X} label="关闭提示" onClick={() => setMessage(null)} />
          </div>
        )}
        <div className="canvas-band">
          <div className="canvas" data-testid="graph-canvas">
            {editable && (
              <button
                className="add-node material-glass"
                onClick={() => setDialog("nodes")}
              >
                <Plus size={16} />
                添加节点
              </button>
            )}
            <ReactFlow
              defaultMarkerColor={null}
              nodes={displayNodes}
              edges={edges}
              nodeTypes={nodeTypes}
              fitView
              minZoom={0.2}
              maxZoom={2}
              nodesDraggable={editable}
              nodesConnectable={editable}
              elementsSelectable
              deleteKeyCode={editable ? ["Backspace", "Delete"] : null}
              onNodeClick={(_, node) => {
                setSelected(node.id);
                setSelectedStep(null);
                setSelectedEdge(null);
              }}
              onEdgeClick={(_, edge) => {
                setSelected(null);
                setSelectedEdge(edge.id);
              }}
              onPaneClick={() => {
                setSelected(null);
                setSelectedEdge(null);
              }}
              onNodeDragStart={() => {
                if (editable) remember();
              }}
              onNodesChange={(changes) => {
                setNodes((old) => applyNodeChanges(changes, old));
                if (editable && changes.some((c) => c.type === "remove"))
                  setDirty(true);
              }}
              onEdgesChange={(changes) => {
                setEdges((old) => applyEdgeChanges(changes, old));
                if (editable && changes.some((c) => c.type === "remove"))
                  setDirty(true);
              }}
              onConnect={(connection) => {
                remember();
                setEdges((old) =>
                  addEdge(
                    {
                      ...connection,
                      type: "smoothstep",
                      markerEnd: { type: MarkerType.ArrowClosed },
                      label: connection.sourceHandle,
                    },
                    old,
                  ),
                );
              }}
            >
              <Background gap={24} size={1} />
            </ReactFlow>
            <CanvasTools />
            {!nodes.length && (
              <div className="canvas-empty">
                <Workflow size={42} />
                <h2>{editable ? "空白画布" : "正在加载定义…"}</h2>
                <button
                  disabled={!editable}
                  onClick={() => {
                    setDialog("nodes");
                  }}
                >
                  <Plus size={15} />
                  添加节点
                </button>
              </div>
            )}
            <div className="canvas-caption">
              {nodes.length} 节点 <span>·</span> {edges.length} 连线
            </div>
          </div>
          {inspectedEdge && !selectedNode && (
            <aside className="inspector">
              <div className="section-heading">
                <strong>端口连线</strong>
                <Tool
                  icon={X}
                  label="关闭连线详情"
                  onClick={() => setSelectedEdge(null)}
                />
              </div>
              <div className="inspector-content">
                <div className="group-label">源端口</div>
                <code>
                  {inspectedEdge.source}.{inspectedEdge.sourceHandle}
                </code>
                <div className="group-label">目标端口</div>
                <code>
                  {inspectedEdge.target}.{inspectedEdge.targetHandle}
                </code>
                {editable && (
                  <button
                    className="danger wide"
                    onClick={() => {
                      remember();
                      setEdges((old) =>
                        old.filter((edge) => edge.id !== selectedEdge),
                      );
                      setSelectedEdge(null);
                    }}
                  >
                    <Trash2 size={15} />
                    删除连线
                  </button>
                )}
              </div>
            </aside>
          )}
          {selectedNode && (
            <aside
              className={`inspector ${activeRun && !editable ? "run-inspector" : ""}`}
            >
              <div className="section-heading">
                <strong>{selectedNode.id}</strong>
                <Tool
                  icon={X}
                  label="关闭节点详情"
                  onClick={() => setSelected(null)}
                />
              </div>
              <div className="inspector-content">
                <div className="group-label">{selectedNode.data.type}</div>
                {editable ? (
                  <>
                    <label className="field">
                      <span>节点标识</span>
                      <input
                        key={selectedNode.id}
                        defaultValue={selectedNode.id}
                        onBlur={(event) => {
                          const next = event.target.value.trim();
                          if (next === selected) return;
                          if (!next || nodes.some((n) => n.id === next)) {
                            event.target.value = selected;
                            setMessage({
                              error: true,
                              text: "节点标识不能为空或重复",
                            });
                            return;
                          }
                          remember();
                          setNodes((old) =>
                            old.map((n) =>
                              n.id === selected ? { ...n, id: next } : n,
                            ),
                          );
                          setEdges((old) =>
                            old.map((e) => ({
                              ...e,
                              source: e.source === selected ? next : e.source,
                              target: e.target === selected ? next : e.target,
                            })),
                          );
                          setDefinition((old) => ({
                            ...old,
                            entrypoint:
                              old.entrypoint === selected
                                ? next
                                : old.entrypoint,
                          }));
                          setSelected(next);
                        }}
                      />
                    </label>
                    <ConfigFields
                      key={selectedNode.id}
                      schema={selectedType?.schema}
                      value={selectedNode.data.config || {}}
                      onChange={changeConfig}
                    />
                    <button
                      className="wide"
                      disabled={busy}
                      onClick={() =>
                        attempt(async () => {
                          const ports = await api("nodes/preview", {
                            id: selectedNode.id,
                            type: selectedNode.data.type,
                            config: selectedNode.data.config,
                          });
                          setNodes((old) =>
                            old.map((n) =>
                              n.id === selected
                                ? { ...n, data: { ...n.data, ...ports } }
                                : n,
                            ),
                          );
                          setMessage({ text: "节点配置已应用" });
                        })
                      }
                    >
                      <Check size={15} />
                      应用配置
                    </button>
                    <label className="check-field">
                      <input
                        type="checkbox"
                        checked={definition.entrypoint === selected}
                        onChange={() => {
                          remember();
                          setDefinition((d) => ({
                            ...d,
                            entrypoint: selected,
                          }));
                        }}
                      />
                      图入口
                    </label>
                    <button
                      className="danger wide"
                      onClick={() => {
                        remember();
                        setNodes((old) => old.filter((n) => n.id !== selected));
                        setEdges((old) =>
                          old.filter(
                            (e) =>
                              e.source !== selected && e.target !== selected,
                          ),
                        );
                        setSelected(null);
                      }}
                    >
                      <Trash2 size={15} />
                      删除节点
                    </button>
                  </>
                ) : activeRun?.graph === registered ? (
                  <NodeRunDetails
                    key={`${activeRun.id}:${selectedNode.id}`}
                    node={selectedNode.id}
                    firings={firings}
                    observations={observations}
                    step={selectedStep}
                    onStep={setSelectedStep}
                    config={activeRun.node_configs?.[selectedNode.id]}
                  />
                ) : (
                  <>
                    <div className="group-label">输入端口</div>
                    {Object.entries(selectedNode.data.inputs || {}).map(
                      ([p, t]) => (
                        <div className="property" key={p}>
                          <span>{p}</span>
                          <code>{t}</code>
                        </div>
                      ),
                    )}
                    <div className="group-label">输出端口</div>
                    {Object.entries(selectedNode.data.outputs || {}).map(
                      ([p, t]) => (
                        <div className="property" key={p}>
                          <span>{p}</span>
                          <code>{t}</code>
                        </div>
                      ),
                    )}
                    <div className="group-label">输入策略</div>
                    <code>{selectedNode.data.policy}</code>
                  </>
                )}
              </div>
            </aside>
          )}
        </div>
        <section
          className={`execution-panel ${panelCollapsed ? "collapsed" : ""}`}
          style={{ "--panel-height": `${Math.min(panelHeight, panelLimit)}px` }}
        >
          {!panelCollapsed && (
            <ResizeHandle
              label="调整运行面板高度"
              value={Math.min(panelHeight, panelLimit)}
              min={180}
              max={panelLimit}
              onChange={setPanelHeight}
            />
          )}
          <div className="panel-tabs">
            <div role="tablist">
              {[
                ["runs", "执行记录"],
                ["timeline", "节点时间线"],
                ["outputs", "终端输出"],
              ].map(([id, label]) => (
                <button
                  role="tab"
                  aria-selected={panel === id}
                  className={panel === id ? "active" : ""}
                  key={id}
                  onClick={() => {
                    setPanel(id);
                    setPanelCollapsed(false);
                  }}
                >
                  {label}
                  {id === "outputs" && outputs.length > 0 && (
                    <small>{outputs.length}</small>
                  )}
                </button>
              ))}
            </div>
            <div className="active-run">
              {activeRun && (
                <>
                  <span className={`status ${activeRun.status}`}>
                    {names[activeRun.status] || activeRun.status}
                  </span>
                  <Tool
                    icon={Square}
                    label="取消执行"
                    disabled={activeRun.done}
                    onClick={() =>
                      attempt(async () => {
                        const result = await api(
                          `executions/${activeRun.id}/cancel`,
                          {},
                        );
                        setActiveRun(result);
                      })
                    }
                  />
                </>
              )}
              <Tool
                icon={panelCollapsed ? PanelBottomOpen : PanelBottomClose}
                label={panelCollapsed ? "展开运行面板" : "收起运行面板"}
                onClick={() => setPanelCollapsed(!panelCollapsed)}
              />
            </div>
          </div>
          <div className="panel-body">
            {panel === "runs" &&
              (runs.length ? (
                <table>
                  <thead>
                    <tr>
                      <th>图 / 版本</th>
                      <th>状态</th>
                      <th>节点触发</th>
                      <th>开始时间</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {runs
                      .filter(
                        (run) =>
                          run.graph === definitionName ||
                          graphs.some(
                            (graph) =>
                              graph.name === run.graph &&
                              graph.publication?.name === definitionName,
                          ),
                      )
                      .map((run) => (
                        <tr
                          key={run.id}
                          className={activeRun?.id === run.id ? "selected" : ""}
                        >
                          <td>
                            <button
                              className="link"
                              onClick={() => {
                                if (!mayLeave()) return;
                                const graph =
                                  run.graph_snapshot ||
                                  graphs.find((g) => g.name === run.graph);
                                if (graph) openGraph(graph);
                                setActiveRun(run);
                                setPanel("timeline");
                              }}
                            >
                              {run.graph}
                              <small>{run.id.slice(0, 8)}</small>
                            </button>
                          </td>
                          <td>
                            <span className={`status ${run.status}`}>
                              {names[run.status] || run.status}
                            </span>
                          </td>
                          <td>{run.steps}</td>
                          <td>
                            {run.started_at
                              ? new Date(run.started_at).toLocaleTimeString()
                              : "—"}
                          </td>
                          <td>
                            <ChevronRight size={14} />
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              ) : (
                <div className="panel-empty">
                  <Activity size={22} />
                  <span>暂无执行记录</span>
                </div>
              ))}
            {panel === "timeline" &&
              (firings.length ? (
                <table>
                  <thead>
                    <tr>
                      <th>步骤</th>
                      <th>节点</th>
                      <th>状态</th>
                      <th>耗时</th>
                      <th>输出端口</th>
                    </tr>
                  </thead>
                  <tbody>
                    {firings.map((event, index) => (
                      <tr
                        key={event.id}
                        className={
                          selected === event.node &&
                          (selectedStep ??
                            firings
                              .filter((item) => item.node === selected)
                              .at(-1)?.attributes.step) ===
                            event.attributes.step
                            ? "selected"
                            : ""
                        }
                        onClick={() => {
                          setSelected(event.node);
                          setSelectedStep(event.attributes.step);
                          setSelectedEdge(null);
                        }}
                      >
                        <td>
                          <span className="step-index">
                            {event.attributes.step || index + 1}
                          </span>
                        </td>
                        <td>{event.node}</td>
                        <td>
                          <span
                            className={`status ${firingState(event.finish)}`}
                          >
                            {event.finish
                              ? event.finish.error_type ||
                                names[firingState(event.finish)]
                              : "运行中"}
                          </span>
                        </td>
                        <td>
                          {event.duration === null
                            ? "…"
                            : `${event.duration} ms`}
                        </td>
                        <td>
                          {[
                            ...new Set(
                              observations
                                .filter(
                                  (e) =>
                                    e.kind === "output.routed" &&
                                    e.attributes.step === event.attributes.step,
                                )
                                .map((e) => e.attributes.port),
                            ),
                          ].join(", ") || "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <div className="panel-empty">
                  <GitBranch size={22} />
                  <span>
                    {activeRun
                      ? activeRun.error || "等待节点触发"
                      : "尚未选择执行"}
                  </span>
                </div>
              ))}
            {panel === "outputs" &&
              (outputs.length ? (
                <div className="output-list">
                  {outputs.map((value, index) => (
                    <div className="output-row" key={index}>
                      <span>{index + 1}</span>
                      <code>{value.port || "状态"}</code>
                      <pre>
                        {JSON.stringify(
                          "value" in value ? value.value : value.unavailable,
                          null,
                          2,
                        )}
                      </pre>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="panel-empty">
                  <Circle size={22} />
                  <span>暂无终端输出</span>
                </div>
              ))}
          </div>
        </section>
      </main>
      {(dialog || auth) && (
        <div
          className="modal-backdrop"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) {
              setDialog(null);
              setAuth(false);
            }
          }}
        >
          <section
            className="modal material-glass"
            role="dialog"
            aria-modal="true"
            aria-label={
              auth ? "访问令牌" : dialog === "run" ? "运行图" : "添加节点"
            }
          >
            <div className="section-heading">
              <h2>
                {auth ? "访问令牌" : dialog === "run" ? "运行图" : "添加节点"}
              </h2>
              <Tool
                icon={X}
                label="关闭对话框"
                onClick={() => {
                  setDialog(null);
                  setAuth(false);
                }}
              />
            </div>
            <div className="modal-content">
              {message?.error && (
                <div role="alert" className="notice error">
                  {message.text}
                </div>
              )}
              {auth ? (
                <form
                  onSubmit={(event) => {
                    event.preventDefault();
                    sessionStorage.setItem("interlace.token", tokenInput);
                    setToken(tokenInput);
                    onAccessToken?.(tokenInput);
                    setAuth(false);
                    setMessage(null);
                  }}
                >
                  <label className="field">
                    <span>令牌</span>
                    <input
                      type="password"
                      autoFocus
                      value={tokenInput}
                      onChange={(e) => setTokenInput(e.target.value)}
                    />
                  </label>
                  <button className="primary wide">连接</button>
                </form>
              ) : dialog === "nodes" ? (
                <div className="node-catalog">
                  {catalog.map((type) => (
                    <button key={type.type} onClick={() => addNode(type)}>
                      <Workflow size={20} />
                      <span>
                        <strong>{type.title}</strong>
                        <small>{type.type}</small>
                      </span>
                      <Plus size={16} />
                    </button>
                  ))}
                  {!catalog.length && (
                    <div className="empty-small">暂无已登记的节点类型</div>
                  )}
                </div>
              ) : (
                <form
                  onSubmit={(event) => {
                    event.preventDefault();
                    attempt(startRun);
                  }}
                >
                  <div className="run-target">
                    <GitBranch size={16} />
                    <strong>{registered}</strong>
                  </div>
                  <label className="field">
                    <span>入口数据</span>
                    <textarea
                      rows={5}
                      value={inputText}
                      onChange={(e) => setInputText(e.target.value)}
                    />
                  </label>
                  <details>
                    <summary>执行选项</summary>
                    <label className="field">
                      <span>领域配置</span>
                      <textarea
                        value={optionsText}
                        onChange={(e) => setOptionsText(e.target.value)}
                      />
                    </label>
                    <div className="field-pair">
                      <label className="field">
                        <span>时限（秒）</span>
                        <input
                          type="number"
                          min="0.001"
                          step="any"
                          value={timeout}
                          onChange={(e) => setTimeoutValue(e.target.value)}
                        />
                      </label>
                      <label className="field">
                        <span>步数上限</span>
                        <input
                          type="number"
                          min="0"
                          value={maxSteps}
                          onChange={(e) => setMaxSteps(e.target.value)}
                        />
                      </label>
                    </div>
                  </details>
                  <button className="primary wide" disabled={busy}>
                    <Play size={15} />
                    开始运行
                  </button>
                </form>
              )}
            </div>
          </section>
        </div>
      )}
    </div>
  );
}
