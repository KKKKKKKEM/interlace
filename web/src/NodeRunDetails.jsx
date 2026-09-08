import React, { useState } from "react";
import { Clipboard } from "lucide-react";

/** 将节点控制异常映射为实际执行状态。 */
export function firingState(event) {
  if (!event) return "running";
  if (event.error_type === "ExecutionCancelledError") return "cancelled";
  if (["ExecutionTimeoutError", "NodeTimeoutError"].includes(event.error_type))
    return "timed_out";
  return event.status;
}

const stateNames = {
  running: "运行中",
  succeeded: "已完成",
  failed: "失败",
  cancelled: "已取消",
  timed_out: "已超时",
};

/** 展示一个节点某次触发的参数、输出、标准日志和异常。 */
export default function NodeRunDetails({
  node,
  firings,
  observations,
  step,
  onStep,
  config,
}) {
  const [tab, setTab] = useState("inputs");
  const [search, setSearch] = useState("");
  const [level, setLevel] = useState("");
  const [copyState, setCopyState] = useState("");
  const calls = firings.filter((event) => event.node === node);
  const firing =
    calls.find((event) => event.attributes.step === step) || calls.at(-1);
  if (!firing) return <div className="diagnostic-empty">该节点尚未执行</div>;
  const records = observations.filter(
    (event) =>
      event.node === node && event.attributes.step === firing.attributes.step,
  );
  const input = records.find(
    (event) => event.kind === "node.input",
  )?.attributes;
  const outputs = records
    .filter((event) => event.kind === "node.output")
    .map((event) => event.attributes.output);
  const logs = records.filter((event) => event.kind === "node.log");
  const failure = records
    .filter((event) => event.kind === "node.exception")
    .at(-1)?.attributes;
  const stack = firing.finish?.attributes.traceback || failure?.traceback;
  const error = firing.finish?.attributes.error || failure?.message;
  const visibleLogs = logs.filter(
    (event) =>
      (!level || event.attributes.level === level) &&
      (!search ||
        `${event.attributes.logger} ${event.attributes.message}`
          .toLowerCase()
          .includes(search.toLowerCase())),
  );
  const selectedData =
    tab === "inputs"
      ? input
      : tab === "outputs"
        ? outputs
        : tab === "logs"
          ? visibleLogs
          : { error, stack };
  return (
    <div className="node-run-details">
      <label className="field firing-picker">
        <span>节点触发</span>
        <select
          aria-label="节点触发"
          value={firing.attributes.step}
          onChange={(event) => onStep(Number(event.target.value))}
        >
          {calls.map((event) => (
            <option key={event.id} value={event.attributes.step}>
              步骤 {event.attributes.step} ·{" "}
              {new Date(event.occurred_at).toLocaleTimeString()}
            </option>
          ))}
        </select>
      </label>
      <dl className="firing-summary">
        <div>
          <dt>状态</dt>
          <dd className={`status ${firingState(firing.finish)}`}>
            {stateNames[firingState(firing.finish)]}
          </dd>
        </div>
        <div>
          <dt>开始</dt>
          <dd>{new Date(firing.occurred_at).toLocaleTimeString()}</dd>
        </div>
        <div>
          <dt>耗时</dt>
          <dd>{firing.duration === null ? "—" : `${firing.duration} ms`}</dd>
        </div>
      </dl>
      <div className="diagnostic-toolbar">
        <div role="tablist" aria-label="节点运行详情">
          {[
            ["inputs", "参数"],
            ["outputs", "输出"],
            ["logs", "日志"],
            ["error", "异常"],
          ].map(([id, title]) => (
            <button
              key={id}
              role="tab"
              aria-selected={tab === id}
              onClick={() => {
                setTab(id);
                setCopyState("");
              }}
            >
              {title}
            </button>
          ))}
        </div>
        <button
          className="tool"
          title="复制当前详情"
          aria-label="复制当前详情"
          onClick={async () => {
            try {
              await navigator.clipboard.writeText(
                JSON.stringify(selectedData ?? null, null, 2),
              );
              setCopyState("已复制");
            } catch {
              setCopyState("复制失败");
            }
          }}
        >
          <Clipboard size={15} />
        </button>
      </div>
      {copyState && (
        <div className="copy-state" role="status">
          {copyState}
        </div>
      )}
      {tab === "inputs" && (
        <div className="diagnostic-data">
          <h3>输入参数</h3>
          {input ? (
            <pre>{JSON.stringify(input.inputs, null, 2)}</pre>
          ) : (
            <div className="diagnostic-empty">该次触发未采集输入参数</div>
          )}
          <h3>执行配置</h3>
          {input ? (
            <pre>{JSON.stringify(input.options, null, 2)}</pre>
          ) : (
            <div className="diagnostic-empty">无配置记录</div>
          )}
          {config !== undefined && (
            <>
              <h3>节点配置</h3>
              <pre>{JSON.stringify(config, null, 2)}</pre>
            </>
          )}
        </div>
      )}
      {tab === "outputs" && (
        <div className="diagnostic-data">
          {outputs.length ? (
            outputs.map((output, index) => (
              <div key={index} className="diagnostic-output">
                <h3>
                  {output.port || "输出"} <small>{index + 1}</small>
                </h3>
                <pre>
                  {JSON.stringify(
                    "value" in output ? output.value : output,
                    null,
                    2,
                  )}
                </pre>
              </div>
            ))
          ) : (
            <div className="diagnostic-empty">该次触发暂无节点输出</div>
          )}
        </div>
      )}
      {tab === "logs" && (
        <>
          <div className="log-filter">
            <input
              aria-label="搜索节点日志"
              placeholder="搜索日志"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
            <select
              aria-label="日志级别"
              value={level}
              onChange={(event) => setLevel(event.target.value)}
            >
              <option value="">全部级别</option>
              {["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"].map(
                (value) => (
                  <option key={value}>{value}</option>
                ),
              )}
            </select>
          </div>
          <div className="diagnostic-logs">
            {visibleLogs.length ? (
              visibleLogs.map((event) => (
                <div key={event.id} className="log-entry">
                  <div>
                    <time>
                      {new Date(event.occurred_at).toLocaleTimeString()}
                    </time>
                    <span
                      className={`log-level ${event.attributes.level.toLowerCase()}`}
                    >
                      {event.attributes.level}
                    </span>
                    <span title={event.attributes.logger}>
                      {event.attributes.logger}
                    </span>
                  </div>
                  <pre>{event.attributes.message}</pre>
                  {event.attributes.traceback && (
                    <pre className="error-text">
                      {event.attributes.traceback}
                    </pre>
                  )}
                </div>
              ))
            ) : (
              <div className="diagnostic-empty">暂无匹配的日志记录</div>
            )}
          </div>
        </>
      )}
      {tab === "error" && (
        <div className="diagnostic-data">
          {error || stack ? (
            <>
              <h3>
                {firing.finish?.error_type || failure?.error_type || "异常"}
              </h3>
              <pre className="error-text">{stack || error}</pre>
            </>
          ) : (
            <div className="diagnostic-empty">该次触发无异常记录</div>
          )}
        </div>
      )}
    </div>
  );
}
