import React, {
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  useId,
} from "react";
import { Check, Monitor, Moon, Sun } from "lucide-react";

/** 外观选项仅描述用户偏好，实际明暗色由系统偏好或显式选择决定。 */
const themes = [
  { value: "light", label: "浅色", icon: Sun },
  { value: "dark", label: "深色", icon: Moon },
  { value: "system", label: "跟随系统", icon: Monitor },
];

/** 校验持久偏好，未知值恢复为跟随系统。 */
function normalizeTheme(value) {
  return themes.some((theme) => theme.value === value) ? value : "system";
}

/** 共享主题选择器，支持系统变化、跨标签页同步和受限存储环境。 */
export default function ThemePicker() {
  const [preference, setPreference] = useState(() =>
    normalizeTheme(document.documentElement.dataset.themePreference),
  );
  // 原生弹出层负责 Escape、外部点击和顶层展示，引用只管理选择后的焦点。
  const popover = useRef(null);
  const trigger = useRef(null);
  const id = useId();
  const current = themes.find((theme) => theme.value === preference);
  const Icon = current.icon;

  useLayoutEffect(() => {
    const system = window.matchMedia("(prefers-color-scheme: dark)");
    const apply = () => {
      document.documentElement.dataset.themePreference = preference;
      document.documentElement.dataset.theme =
        preference === "system"
          ? system.matches
            ? "dark"
            : "light"
          : preference;
    };
    apply();
    system.addEventListener("change", apply);
    return () => system.removeEventListener("change", apply);
  }, [preference]);

  useEffect(() => {
    const sync = (event) => {
      if (event.key === "interlace.theme" || event.key === null)
        setPreference(normalizeTheme(event.newValue));
    };
    window.addEventListener("storage", sync);
    return () => window.removeEventListener("storage", sync);
  }, []);

  const choose = (value) => {
    setPreference(value);
    try {
      localStorage.setItem("interlace.theme", value);
    } catch {
      // 禁用浏览器存储时，本页仍可切换主题。
    }
    popover.current.hidePopover();
    trigger.current.focus();
  };

  return (
    <>
      <button
        ref={trigger}
        type="button"
        className="tool theme-trigger"
        aria-label="切换主题"
        title={`主题：${current.label}`}
        data-tooltip={`主题：${current.label}`}
        popoverTarget={id}
      >
        <Icon size={17} />
      </button>
      <div
        id={id}
        ref={popover}
        popover="auto"
        className="theme-popover material-glass"
      >
        <div className="theme-options" role="radiogroup" aria-label="外观模式">
          {themes.map(({ value, label, icon: OptionIcon }) => (
            <label className="theme-option" key={value}>
              <input
                type="radio"
                name={id}
                value={value}
                checked={preference === value}
                onChange={() => choose(value)}
              />
              <OptionIcon size={16} />
              <span>{label}</span>
              <Check size={15} className="theme-check" aria-hidden="true" />
            </label>
          ))}
        </div>
      </div>
    </>
  );
}
