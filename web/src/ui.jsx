import React, { useEffect, useRef } from "react";
import { X } from "lucide-react";

/** 在管理列表与图工作台中复用具名图标工具。 */
export function Tool({ icon: Icon, label, onClick, disabled, active }) {
  return (
    <button
      type="button"
      className={`tool ${active ? "active" : ""}`}
      aria-label={label}
      title={label}
      data-tooltip={label}
      onClick={onClick}
      disabled={disabled}
    >
      <Icon size={17} />
    </button>
  );
}

/** 使用指针或方向键调整下方运行面板的高度，数值单位为像素。 */
export function ResizeHandle({ label, value, min, max, onChange }) {
  const drag = useRef(null);
  const clamp = (next) => Math.max(min, Math.min(max, next));
  return (
    <div
      className="panel-resize-handle"
      role="separator"
      tabIndex={0}
      aria-label={label}
      aria-orientation="horizontal"
      aria-valuenow={value}
      aria-valuemin={min}
      aria-valuemax={max}
      onPointerDown={(event) => {
        if (event.button !== 0) return;
        drag.current = { y: event.clientY, value };
        event.currentTarget.setPointerCapture(event.pointerId);
      }}
      onPointerMove={(event) => {
        if (drag.current)
          onChange(clamp(drag.current.value + drag.current.y - event.clientY));
      }}
      onPointerUp={() => {
        drag.current = null;
      }}
      onPointerCancel={() => {
        drag.current = null;
      }}
      onLostPointerCapture={() => {
        drag.current = null;
      }}
      onKeyDown={(event) => {
        const next = {
          ArrowUp: value + 20,
          ArrowDown: value - 20,
          Home: min,
          End: max,
        }[event.key];
        if (next !== undefined) {
          event.preventDefault();
          onChange(clamp(next));
        }
      }}
    />
  );
}

/** 使用浏览器原生模态语义提供焦点管理与关闭行为。 */
export function Dialog({ title, onClose, children }) {
  const ref = useRef(null);
  useEffect(() => {
    const element = ref.current;
    element.showModal();
    return () => element.close();
  }, []);
  return (
    <dialog
      className="manager-dialog material-glass"
      ref={ref}
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
      onClick={(event) => {
        if (event.target === ref.current) {
          const box = ref.current.getBoundingClientRect();
          if (
            event.clientX < box.left ||
            event.clientX > box.right ||
            event.clientY < box.top ||
            event.clientY > box.bottom
          )
            onClose();
        }
      }}
      aria-label={title}
    >
      <div className="section-heading">
        <h2>{title}</h2>
        <Tool icon={X} label="关闭对话框" onClick={onClose} />
      </div>
      <div className="modal-content">{children}</div>
    </dialog>
  );
}

/** 统一展示管理列表中的运行状态。 */
export function RunStatus({ status }) {
  const labels = {
    pending: "排队中",
    running: "运行中",
    succeeded: "已完成",
    failed: "失败",
    cancelled: "已取消",
    timed_out: "已超时",
    step_limited: "达到步数上限",
    interrupted: "已中断",
  };
  return status ? (
    <span className={`status ${status}`}>{labels[status] || status}</span>
  ) : (
    <span className="muted">尚未运行</span>
  );
}

/** 将服务时间转换为本地可读时间。 */
export function dateText(value) {
  return value
    ? new Date(value).toLocaleString("zh-CN", { hour12: false })
    : "—";
}
