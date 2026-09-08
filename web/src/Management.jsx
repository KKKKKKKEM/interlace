import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import {
  Link,
  Outlet,
  useBlocker,
  useLocation,
  useMatch,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import {
  Activity,
  ArrowLeft,
  ArrowRight,
  ChevronRight,
  Folder,
  FolderInput,
  KeyRound,
  List,
  Menu,
  PanelLeftClose,
  PanelLeftOpen,
  Plus,
  RefreshCw,
  Search,
  Settings,
  Trash2,
  Upload,
  Workflow,
  X,
  Play,
} from "lucide-react";
import { Dialog, Tool, RunStatus, dateText } from "./ui.jsx";
import ThemePicker from "./ThemePicker.jsx";

const ServiceContext = createContext(null);
const WorkbenchView = React.lazy(() => import("./WorkbenchView.jsx"));
const sourceNames = { code: "代码", visual: "可视化" };
const statusNames = {
  published: "已发布",
  code: "已注册",
  draft: "草稿",
  unavailable: "未注册",
};
const projectPath = (id) => `/projects/${encodeURIComponent(id)}`;
const definitionPath = (id, name) =>
  `${projectPath(id)}/definitions/${encodeURIComponent(name)}`;

/** 共享认证、项目导航和服务调用状态。 */
function useService() {
  return useContext(ServiceContext);
}

/** 读取路由资源，丢弃旧路由的迟到响应，并提供显式重试。 */
function useResource(path) {
  const { api } = useService();
  const [data, setData] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const serial = useRef(0);
  const lastPath = useRef(path);
  const reload = useCallback(async () => {
    const request = ++serial.current;
    if (lastPath.current !== path) {
      lastPath.current = path;
      setData(null);
    }
    setLoading(true);
    setError("");
    try {
      const value = await api(path);
      if (request === serial.current) setData(value);
      return value;
    } catch (failure) {
      if (request === serial.current) setError(failure.message);
    } finally {
      if (request === serial.current) setLoading(false);
    }
  }, [api, path]);
  useEffect(() => {
    reload();
    return () => {
      serial.current += 1;
    };
  }, [reload]);
  return {
    data: lastPath.current === path ? data : null,
    error: lastPath.current === path ? error : "",
    loading,
    reload,
  };
}

/** 管理后台统一使用项目侧栏和可返回的层级导航。 */
export function Layout() {
  const [token, setTokenValue] = useState(
    sessionStorage.getItem("interlace.token") || "",
  );
  const [tokenInput, setTokenInput] = useState(token);
  const [login, setLogin] = useState(false);
  const [projects, setProjects] = useState([]);
  const [projectError, setProjectError] = useState("");
  const [projectsLoading, setProjectsLoading] = useState(true);
  const [sidebar, setSidebar] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const location = useLocation();
  const match = useMatch("/projects/:projectId/*");
  const detailMatch = useMatch(
    "/projects/:projectId/definitions/:definitionName",
  );
  const projectId = match?.params.projectId;
  const current = projects.find((item) => item.id === projectId);
  const [query] = useSearchParams();
  const workbench = Boolean(
    detailMatch && (query.get("tab") === "graph" || query.get("run")),
  );
  const compact = workbench && !expanded;
  const navigate = useNavigate();
  const setToken = useCallback((value) => {
    sessionStorage.setItem("interlace.token", value);
    setTokenValue(value);
  }, []);
  const api = useCallback(
    async (path, body, method) => {
      const response = await fetch(`./api/${path}`, {
        method: method || (body === undefined ? "GET" : "POST"),
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
      if (response.status === 401) setLogin(true);
      const value = await response.json();
      if (!response.ok) {
        const detail = value.detail || value;
        throw new Error(
          Array.isArray(detail)
            ? detail.map((item) => item.msg).join("；")
            : typeof detail === "string"
              ? detail
              : JSON.stringify(detail),
        );
      }
      return value;
    },
    [token],
  );
  const refreshProjects = useCallback(async () => {
    setProjectError("");
    setProjectsLoading(true);
    try {
      const value = await api("projects");
      setProjects(value);
      return value;
    } catch (error) {
      setProjectError(error.message);
    } finally {
      setProjectsLoading(false);
    }
  }, [api]);
  useEffect(() => {
    refreshProjects();
  }, [refreshProjects]);
  useEffect(() => {
    setSidebar(false);
  }, [location.pathname, location.search]);
  return (
    <ServiceContext.Provider
      value={{
        api,
        token,
        setToken,
        projects,
        refreshProjects,
        projectsLoading,
        projectError,
      }}
    >
      <div className={`management-shell ${compact ? "compact-nav" : ""}`}>
        <header className="management-header material-glass">
          <div className="management-brand">
            <Tool
              icon={Menu}
              label="打开导航"
              onClick={() => setSidebar(!sidebar)}
            />
            <Link className="brand-lockup" to="/projects">
              <span className="brand-mark">
                <Workflow size={20} strokeWidth={1.8} />
              </span>
              <span>Interlace</span>
            </Link>
          </div>
          {workbench && (
            <div className="navigation-toggle">
              <Tool
                icon={compact ? PanelLeftOpen : PanelLeftClose}
                label={compact ? "展开导航" : "收起导航"}
                onClick={() => setExpanded(!expanded)}
              />
            </div>
          )}
          <nav className="breadcrumbs" aria-label="当前位置">
            <Link to="/projects">项目</Link>
            {projectId && (
              <>
                <ChevronRight size={14} />
                <Link to={projectPath(projectId)}>
                  {current?.name || "项目详情"}
                </Link>
              </>
            )}
            {detailMatch && (
              <>
                <ChevronRight size={14} />
                <span>{detailMatch.params.definitionName}</span>
              </>
            )}
          </nav>
          <ThemePicker />
          <Tool
            icon={KeyRound}
            label="访问令牌"
            onClick={() => {
              setTokenInput(token);
              setLogin(true);
            }}
          />
        </header>
        {sidebar && (
          <button
            className="nav-backdrop"
            aria-label="关闭导航"
            onClick={() => setSidebar(false)}
          />
        )}
        <aside
          className={`management-sidebar material-glass ${sidebar ? "open" : ""}`}
        >
          <Link
            className={`management-nav ${!projectId ? "selected" : ""}`}
            to="/projects"
            title="项目管理"
          >
            <Folder size={17} />
            <span>项目管理</span>
          </Link>
          {projectId && (
            <>
              <label className="project-switch">
                <span>当前项目</span>
                <select
                  aria-label="切换项目"
                  value={projectId}
                  onChange={(event) =>
                    navigate(projectPath(event.target.value))
                  }
                >
                  {projects.map((project) => (
                    <option key={project.id} value={project.id}>
                      {project.name}
                    </option>
                  ))}
                </select>
              </label>
              <Link
                className={`management-nav ${!query.get("view") ? "selected" : ""}`}
                to={projectPath(projectId)}
                title="定义列表"
              >
                <List size={17} />
                <span>定义列表</span>
              </Link>
              <Link
                className={`management-nav ${query.get("view") === "runs" ? "selected" : ""}`}
                to={`${projectPath(projectId)}?view=runs`}
                title="运行记录"
              >
                <Activity size={17} />
                <span>运行记录</span>
              </Link>
              <Link
                className={`management-nav ${query.get("view") === "settings" ? "selected" : ""}`}
                to={`${projectPath(projectId)}?view=settings`}
                title="项目设置"
              >
                <Settings size={17} />
                <span>项目设置</span>
              </Link>
            </>
          )}
          <div className="management-sidebar-bottom">
            {projects.length} 个项目
          </div>
        </aside>
        <main className="management-main">
          <Outlet />
        </main>
      </div>
      {login && (
        <Dialog title="访问令牌" onClose={() => setLogin(false)}>
          <form
            onSubmit={(event) => {
              event.preventDefault();
              setToken(tokenInput);
              setLogin(false);
            }}
          >
            <label className="field">
              <span>令牌</span>
              <input
                type="password"
                value={tokenInput}
                onChange={(event) => setTokenInput(event.target.value)}
                autoFocus
              />
            </label>
            <button className="primary wide">连接</button>
          </form>
        </Dialog>
      )}
    </ServiceContext.Provider>
  );
}

/** 显示资源错误与重试，不用空列表掩盖加载失败。 */
function ResourceState({ resource }) {
  if (resource.error)
    return (
      <div className="resource-state" role="alert">
        <span>{resource.error}</span>
        <button onClick={resource.reload}>
          <RefreshCw size={15} />
          重试
        </button>
      </div>
    );
  if (!resource.data)
    return (
      <div className="resource-state" role="status">
        正在加载…
      </div>
    );
  return null;
}

/** 管理列表复用名称搜索框。 */
function SearchField({ value, onChange, label }) {
  return (
    <div className="management-search">
      <Search size={16} />
      <input
        aria-label={label}
        placeholder={label}
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
      {value && <Tool icon={X} label="清空搜索" onClick={() => onChange("")} />}
    </div>
  );
}

/** 入口直接展示持久项目列表。 */
export function ProjectList() {
  const { api, projects, refreshProjects, projectsLoading, projectError } =
    useService();
  const [search, setSearch] = useState("");
  const [create, setCreate] = useState(false);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const navigate = useNavigate();
  const visible = projects.filter((item) =>
    `${item.name} ${item.description}`
      .toLowerCase()
      .includes(search.toLowerCase()),
  );
  return (
    <div className="management-page">
      <div className="page-heading">
        <div>
          <h1>项目</h1>
          <p>{projects.length} 个项目</p>
        </div>
        <button
          className="primary"
          onClick={() => {
            setCreate(true);
            setError("");
            setName("");
            setDescription("");
          }}
        >
          <Plus size={16} />
          新建项目
        </button>
      </div>
      <div className="list-toolbar">
        <SearchField value={search} onChange={setSearch} label="搜索项目" />
        <Tool
          icon={RefreshCw}
          label="刷新项目"
          onClick={refreshProjects}
          disabled={projectsLoading}
        />
      </div>
      {projectError ? (
        <div className="resource-state" role="alert">
          {projectError}
          <button onClick={refreshProjects}>重试</button>
        </div>
      ) : projectsLoading && !projects.length ? (
        <div className="resource-state" role="status">
          正在加载项目…
        </div>
      ) : (
        <div className="management-table">
          <table>
            <thead>
              <tr>
                <th>项目名称</th>
                <th>定义</th>
                <th>草稿</th>
                <th className="secondary-column">创建时间</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {visible.map((project) => (
                <tr key={project.id}>
                  <td>
                    <Link className="row-title" to={projectPath(project.id)}>
                      <Folder size={18} />
                      <span>
                        {project.name}
                        {project.description && (
                          <small>{project.description}</small>
                        )}
                      </span>
                    </Link>
                  </td>
                  <td>{project.definition_count}</td>
                  <td>{project.draft_count}</td>
                  <td className="secondary-column muted">
                    {dateText(project.created_at)}
                  </td>
                  <td>
                    <Link
                      className="row-arrow"
                      to={projectPath(project.id)}
                      aria-label={`进入 ${project.name}`}
                    >
                      <ArrowRight size={16} />
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {!visible.length && <div className="list-empty">没有匹配的项目</div>}
        </div>
      )}
      {create && (
        <Dialog title="新建项目" onClose={() => setCreate(false)}>
          <form
            onSubmit={async (event) => {
              event.preventDefault();
              setBusy(true);
              setError("");
              try {
                const value = await api("projects", { name, description });
                await refreshProjects();
                navigate(projectPath(value.id));
              } catch (failure) {
                setError(failure.message);
              } finally {
                setBusy(false);
              }
            }}
          >
            {error && (
              <div className="form-error" role="alert">
                {error}
              </div>
            )}
            <label className="field">
              <span>项目名称</span>
              <input
                required
                maxLength={128}
                autoFocus
                value={name}
                onChange={(event) => setName(event.target.value)}
              />
            </label>
            <label className="field">
              <span>项目描述</span>
              <textarea
                rows={3}
                maxLength={2000}
                value={description}
                onChange={(event) => setDescription(event.target.value)}
              />
            </label>
            <div className="dialog-actions">
              <button type="button" onClick={() => setCreate(false)}>
                取消
              </button>
              <button className="primary" disabled={busy}>
                创建项目
              </button>
            </div>
          </form>
        </Dialog>
      )}
    </div>
  );
}

/** 项目内以定义列表为主，同时提供项目运行记录与设置。 */
export function ProjectDetail() {
  const { projectId } = useParams();
  const [query] = useSearchParams();
  const { api, refreshProjects } = useService();
  const resource = useResource(
    `projects/${encodeURIComponent(projectId)}/definitions`,
  );
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState("");
  const [create, setCreate] = useState(false);
  const [name, setName] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const file = useRef(null);
  const navigate = useNavigate();
  const view = query.get("view") || "definitions";
  if (!resource.data || resource.error)
    return <ResourceState resource={resource} />;
  const { project, definitions } = resource.data;
  const visible = definitions.filter(
    (item) =>
      item.name.toLowerCase().includes(search.toLowerCase()) &&
      (!status || item.status === status),
  );
  return (
    <div className="management-page">
      <div className="page-heading">
        <div>
          <h1>{project.name}</h1>
          <p>
            {view === "runs"
              ? "运行记录"
              : view === "settings"
                ? "项目设置"
                : `${definitions.length} 条定义`}
          </p>
        </div>
        {view === "definitions" && (
          <button
            className="primary"
            onClick={() => {
              setCreate(true);
              setName("");
              setError("");
            }}
          >
            <Plus size={16} />
            新建定义
          </button>
        )}
      </div>
      {view === "settings" ? (
        <ProjectSettings
          key={project.id}
          project={project}
          count={definitions.length}
          onSaved={async () => {
            await resource.reload();
            await refreshProjects();
          }}
        />
      ) : view === "runs" ? (
        <ProjectRuns projectId={projectId} />
      ) : (
        <>
          <div className="list-toolbar">
            <SearchField value={search} onChange={setSearch} label="搜索定义" />
            <select
              className="list-filter"
              aria-label="定义状态"
              value={status}
              onChange={(event) => setStatus(event.target.value)}
            >
              <option value="">全部状态</option>
              {Object.entries(statusNames).map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
            <div className="toolbar-spacer" />
            <Tool
              icon={Upload}
              label="导入定义"
              disabled={busy}
              onClick={() => file.current.click()}
            />
            <Tool
              icon={RefreshCw}
              label="刷新定义"
              onClick={resource.reload}
              disabled={resource.loading}
            />
          </div>
          {error && !create && (
            <div className="form-error" role="alert">
              {error}
            </div>
          )}
          <div className="management-table">
            <table>
              <thead>
                <tr>
                  <th>定义名称</th>
                  <th>状态</th>
                  <th>来源</th>
                  <th className="secondary-column">节点</th>
                  <th>最近运行</th>
                  <th className="secondary-column">更新时间</th>
                </tr>
              </thead>
              <tbody>
                {visible.map((item) => (
                  <tr key={item.name}>
                    <td>
                      <Link
                        className="row-title"
                        to={definitionPath(projectId, item.name)}
                      >
                        <Workflow size={17} />
                        <span>{item.name}</span>
                      </Link>
                    </td>
                    <td>
                      <span className={`definition-state ${item.status}`}>
                        {statusNames[item.status]}
                      </span>
                      {item.version !== null && (
                        <small className="version-text">v{item.version}</small>
                      )}
                    </td>
                    <td className="muted">{sourceNames[item.source]}</td>
                    <td className="secondary-column">{item.node_count}</td>
                    <td>
                      <RunStatus status={item.last_run?.status} />
                    </td>
                    <td className="secondary-column muted">
                      {dateText(item.updated_at)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {!visible.length && (
              <div className="list-empty">
                {search || status ? "没有匹配的定义" : "此项目暂无定义"}
              </div>
            )}
          </div>
          <input
            ref={file}
            type="file"
            hidden
            accept=".json,application/json"
            onChange={async (event) => {
              const selected = event.target.files[0];
              event.target.value = "";
              if (!selected) return;
              setBusy(true);
              setError("");
              try {
                const definition = await api(
                  "definitions/parse",
                  JSON.parse(await selected.text()),
                );
                const created = await api(
                  `projects/${encodeURIComponent(projectId)}/definitions`,
                  definition,
                );
                await refreshProjects();
                navigate(
                  `${definitionPath(projectId, created.name)}?tab=graph&version=draft`,
                );
              } catch (failure) {
                setError(failure.message);
              } finally {
                setBusy(false);
              }
            }}
          />
        </>
      )}
      {create && (
        <Dialog title="新建定义" onClose={() => setCreate(false)}>
          <form
            onSubmit={async (event) => {
              event.preventDefault();
              setBusy(true);
              setError("");
              try {
                const value = await api(
                  `projects/${encodeURIComponent(projectId)}/definitions`,
                  { name, entrypoint: "", nodes: [], edges: [] },
                );
                await refreshProjects();
                navigate(
                  `${definitionPath(projectId, value.name)}?tab=graph&version=draft`,
                );
              } catch (failure) {
                setError(failure.message);
              } finally {
                setBusy(false);
              }
            }}
          >
            {error && (
              <div className="form-error" role="alert">
                {error}
              </div>
            )}
            <label className="field">
              <span>定义标识</span>
              <input
                required
                autoFocus
                maxLength={128}
                value={name}
                onChange={(event) => setName(event.target.value)}
              />
            </label>
            <div className="dialog-actions">
              <button type="button" onClick={() => setCreate(false)}>
                取消
              </button>
              <button className="primary" disabled={busy}>
                创建定义
              </button>
            </div>
          </form>
        </Dialog>
      )}
    </div>
  );
}

/** 编辑项目的显示信息，空项目可删除。 */
function ProjectSettings({ project, count, onSaved }) {
  const { api, refreshProjects } = useService();
  const [name, setName] = useState(project.name);
  const [description, setDescription] = useState(project.description);
  const [error, setError] = useState("");
  const [saved, setSaved] = useState(false);
  const [busy, setBusy] = useState(false);
  const navigate = useNavigate();
  return (
    <div className="project-settings">
      <form
        onSubmit={async (event) => {
          event.preventDefault();
          setBusy(true);
          setError("");
          setSaved(false);
          try {
            await api(`projects/${project.id}`, { name, description });
            await onSaved();
            setSaved(true);
          } catch (failure) {
            setError(failure.message);
          } finally {
            setBusy(false);
          }
        }}
      >
        <h2>基本信息</h2>
        {error && (
          <div className="form-error" role="alert">
            {error}
          </div>
        )}
        {saved && (
          <div className="form-success" role="status">
            项目信息已保存
          </div>
        )}
        <label className="field">
          <span>项目名称</span>
          <input
            required
            maxLength={128}
            value={name}
            onChange={(event) => {
              setName(event.target.value);
              setSaved(false);
            }}
          />
        </label>
        <label className="field">
          <span>项目描述</span>
          <textarea
            rows={4}
            maxLength={2000}
            value={description}
            onChange={(event) => {
              setDescription(event.target.value);
              setSaved(false);
            }}
          />
        </label>
        <button className="primary" disabled={busy}>
          保存信息
        </button>
      </form>
      <div className="project-delete">
        <h2>删除项目</h2>
        <button
          className="danger"
          disabled={project.id === "default" || count > 0 || busy}
          onClick={async () => {
            if (!window.confirm(`删除项目“${project.name}”？`)) return;
            setBusy(true);
            try {
              await api(`projects/${project.id}`, undefined, "DELETE");
              await refreshProjects();
              navigate("/projects");
            } catch (failure) {
              setError(failure.message);
              setBusy(false);
            }
          }}
        >
          <Trash2 size={15} />
          删除项目
        </button>
      </div>
    </div>
  );
}

/** 项目与定义详情共用的运行记录表格。 */
function RunsTable({ runs, onOpen, compact = false }) {
  return (
    <div className="management-table">
      <table>
        <thead>
          <tr>
            <th>运行实例</th>
            {!compact && <th>定义 / 版本</th>}
            <th>状态</th>
            <th className="secondary-column">节点触发</th>
            <th>开始时间</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <tr key={run.id}>
              <td>
                <button className="link run-link" onClick={() => onOpen(run)}>
                  {run.id.slice(0, 8)}
                </button>
              </td>
              {!compact && <td>{run.graph}</td>}
              <td>
                <RunStatus status={run.status} />
              </td>
              <td className="secondary-column">{run.steps}</td>
              <td className="muted">{dateText(run.started_at)}</td>
              <td>
                <button
                  className="tool"
                  title="查看运行详情"
                  aria-label={`查看运行 ${run.id.slice(0, 8)}`}
                  onClick={() => onOpen(run)}
                >
                  <ArrowRight size={15} />
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {!runs.length && <div className="list-empty">暂无运行记录</div>}
    </div>
  );
}

/** 项目级运行列表只展示当前项目的定义。 */
function ProjectRuns({ projectId }) {
  const resource = useResource(
    `projects/${encodeURIComponent(projectId)}/executions`,
  );
  const navigate = useNavigate();
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState("");
  useEffect(() => {
    const timer = setInterval(resource.reload, 3000);
    return () => clearInterval(timer);
  }, [resource.reload]);
  if (!resource.data || resource.error)
    return <ResourceState resource={resource} />;
  return (
    <>
      <div className="list-toolbar">
        <SearchField
          value={search}
          onChange={setSearch}
          label="搜索运行或定义"
        />
        <select
          className="list-filter"
          value={status}
          onChange={(event) => setStatus(event.target.value)}
          aria-label="运行状态"
        >
          <option value="">全部状态</option>
          <option value="running">运行中</option>
          <option value="succeeded">已完成</option>
          <option value="failed">失败</option>
          <option value="cancelled">已取消</option>
          <option value="timed_out">已超时</option>
        </select>
        <div className="toolbar-spacer" />
        <Tool icon={RefreshCw} label="刷新运行" onClick={resource.reload} />
      </div>
      <RunsTable
        runs={resource.data.filter(
          (run) =>
            `${run.id} ${run.graph}`
              .toLowerCase()
              .includes(search.toLowerCase()) &&
            (!status || run.status === status),
        )}
        onOpen={(run) =>
          navigate(
            `${definitionPath(projectId, run.definition_name)}?tab=runs&run=${encodeURIComponent(run.id)}`,
          )
        }
      />
    </>
  );
}

/** 单条定义详情将概览、编排和实例记录分成独立视图。 */
export function DefinitionDetail() {
  const { projectId, definitionName } = useParams();
  const { api, token, setToken, projects, refreshProjects } = useService();
  const resource = useResource(
    `definitions/${encodeURIComponent(definitionName)}?project_id=${encodeURIComponent(projectId)}`,
  );
  const [query, setQuery] = useSearchParams();
  const navigate = useNavigate();
  const [move, setMove] = useState(false);
  const [target, setTarget] = useState(projectId);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [dirty, setDirtyValue] = useState(false);
  const [actionsTarget, setActionsTarget] = useState(null);
  const dirtyRef = useRef(false);
  const setDirty = useCallback((value) => {
    dirtyRef.current = value;
    setDirtyValue(value);
  }, []);
  const blocker = useBlocker(() => dirtyRef.current);
  useEffect(() => {
    if (blocker.state === "blocked") {
      if (window.confirm("当前修改尚未保存，是否离开？")) blocker.proceed();
      else blocker.reset();
    }
  }, [blocker]);
  const tab = query.get("tab") || "overview";
  const runId = query.get("run");
  useEffect(() => {
    resource.reload();
  }, [tab, runId]);
  useEffect(() => {
    if (tab !== "runs" || runId) return;
    const timer = setInterval(resource.reload, 2500);
    return () => clearInterval(timer);
  }, [tab, runId, resource.reload]);
  if (!resource.data || resource.error)
    return <ResourceState resource={resource} />;
  const data = resource.data;
  if (data.project_id !== projectId)
    return <NotFound message="该项目下没有此定义" />;
  const version = query.get("version") || data.graph || "draft";
  const graph =
    data.graphs.find((item) => item.name === version) || data.graphs[0];
  if (
    query.has("version") &&
    ((version === "draft" && !data.draft) ||
      (version !== "draft" &&
        !data.graphs.some((item) => item.name === version)))
  )
    return <NotFound message="该版本不存在" />;
  const onRun = (run) => {
    setDirty(false);
    setQuery({ tab: "runs", run: run.id });
  };
  return (
    <div
      className={`definition-page ${tab === "graph" || runId ? "has-workbench" : ""} ${runId ? "has-instance" : ""}`}
    >
      <div className="page-heading definition-heading material-glass">
        <div>
          <h1>{data.name}</h1>
          <p>
            <span>{sourceNames[data.source]}定义</span>
            <span className="heading-divider">/</span>
            <span>{statusNames[data.status]}</span>
            {dirty && <span className="unsaved">未保存</span>}
          </p>
        </div>
        <div className="definition-actions">
          <Tool
            icon={FolderInput}
            label="移动到项目"
            disabled={dirty}
            onClick={() => {
              setMove(true);
              setTarget(projectId);
              setError("");
            }}
          />
          <div className="workbench-actions-slot" ref={setActionsTarget} />
          {tab !== "graph" && (
            <>
              {data.draft && (
                <button
                  onClick={() => setQuery({ tab: "graph", version: "draft" })}
                >
                  编辑草稿
                </button>
              )}
              <button
                className="primary"
                disabled={!data.graph}
                onClick={() =>
                  setQuery({ tab: "graph", version: data.graph, execute: "1" })
                }
              >
                <Play size={15} />
                运行
              </button>
            </>
          )}
        </div>
      </div>
      <div
        className="definition-tabs material-glass"
        role="tablist"
        aria-label="定义详情"
      >
        {[
          ["overview", "概览"],
          ["graph", "图编排"],
          ["runs", "运行记录"],
        ].map(([value, label]) => (
          <button
            key={value}
            role="tab"
            aria-selected={tab === value}
            onClick={() => {
              if (value !== tab || runId) setQuery({ tab: value });
            }}
          >
            {label}
          </button>
        ))}
        {tab === "graph" && (
          <select
            aria-label="定义版本"
            value={version}
            onChange={(event) =>
              setQuery({ tab: "graph", version: event.target.value })
            }
          >
            {data.draft && (
              <option value="draft">草稿 · r{data.draft.revision}</option>
            )}
            {data.graphs.map((item) => (
              <option key={item.name} value={item.name}>
                {item.publication
                  ? `发布版本 v${item.publication.version}`
                  : "代码定义"}
              </option>
            ))}
          </select>
        )}
      </div>
      {tab === "overview" && (
        <div className="definition-overview">
          <section className="definition-metadata">
            <div className="overview-heading">
              <h2>定义信息</h2>
              <button
                className="link"
                onClick={() => setQuery({ tab: "graph" })}
              >
                查看图编排
                <ArrowRight size={14} />
              </button>
            </div>
            <dl>
              <div>
                <dt>所属项目</dt>
                <dd>
                  <Link to={projectPath(projectId)}>{data.project.name}</Link>
                </dd>
              </div>
              <div>
                <dt>定义标识</dt>
                <dd>
                  <code>{data.name}</code>
                </dd>
              </div>
              <div>
                <dt>入口节点</dt>
                <dd>
                  {graph?.entrypoint ||
                    data.draft?.definition.entrypoint ||
                    "未设置"}
                </dd>
              </div>
              <div>
                <dt>节点数量</dt>
                <dd>{data.node_count}</dd>
              </div>
              <div>
                <dt>最新版本</dt>
                <dd>
                  {data.version
                    ? `v${data.version}`
                    : data.source === "code"
                      ? "代码定义"
                      : "尚未发布"}
                </dd>
              </div>
              <div>
                <dt>更新时间</dt>
                <dd>{dateText(data.updated_at)}</dd>
              </div>
            </dl>
            {data.graph && (
              <div className="rpc-summary">
                <h3>调用接口</h3>
                <div>
                  <code>POST /api/call</code>
                  <code>{data.graph}</code>
                </div>
              </div>
            )}
          </section>
          <section className="definition-versions">
            <h2>版本</h2>
            {data.draft && (
              <button
                onClick={() => setQuery({ tab: "graph", version: "draft" })}
              >
                <span>
                  草稿<small>修订 {data.draft.revision}</small>
                </span>
                <ChevronRight size={15} />
              </button>
            )}
            {data.graphs.map((item) => (
              <button
                key={item.name}
                onClick={() => setQuery({ tab: "graph", version: item.name })}
              >
                <span>
                  {item.publication
                    ? `v${item.publication.version}`
                    : "代码定义"}
                  <small>{item.nodes.length} 个节点</small>
                </span>
                <ChevronRight size={15} />
              </button>
            ))}
            {!data.graphs.length && !data.draft && (
              <div className="list-empty">暂无可用版本</div>
            )}
          </section>
          <section className="definition-recent">
            <div className="overview-heading">
              <h2>最近运行</h2>
              <button
                className="link"
                onClick={() => setQuery({ tab: "runs" })}
              >
                查看全部
                <ArrowRight size={14} />
              </button>
            </div>
            <RunsTable compact runs={data.runs.slice(0, 5)} onOpen={onRun} />
          </section>
        </div>
      )}
      {tab === "runs" && !runId && (
        <div className="definition-runs">
          <div className="list-toolbar">
            <span className="muted">{data.runs.length} 条运行记录</span>
            <div className="toolbar-spacer" />
            <Tool icon={RefreshCw} label="刷新运行" onClick={resource.reload} />
          </div>
          <RunsTable runs={data.runs} onOpen={onRun} />
        </div>
      )}
      {runId && (
        <div className="instance-heading">
          <button className="link" onClick={() => setQuery({ tab: "runs" })}>
            <ArrowLeft size={14} />
            返回运行记录
          </button>
          <code>{runId}</code>
        </div>
      )}
      {(tab === "graph" || runId) &&
        (graph || data.draft || runId ? (
          <React.Suspense
            fallback={
              <div className="resource-state" role="status">
                正在加载图编排…
              </div>
            }
            key={`${projectId}:${definitionName}:${version}:${runId || ""}`}
          >
            <WorkbenchView
              actionsTarget={actionsTarget}
              definitionName={definitionName}
              projectId={projectId}
              graphName={version}
              runId={runId}
              allowedGraphs={[
                definitionName,
                ...data.graphs.map((item) => item.name),
              ]}
              autoRun={query.get("execute") === "1"}
              showRuns={Boolean(runId)}
              accessToken={token}
              onAccessToken={setToken}
              onDirtyChange={setDirty}
              onPublished={async (value) => {
                setDirty(false);
                await resource.reload();
                await refreshProjects();
                setQuery({ tab: "graph", version: value.graph });
              }}
              onEditDraft={() => setQuery({ tab: "graph", version: "draft" })}
              onRunStarted={onRun}
            />
          </React.Suspense>
        ) : (
          <div className="resource-state">当前服务未注册此定义</div>
        ))}
      {move && (
        <Dialog title="移动到项目" onClose={() => setMove(false)}>
          <form
            onSubmit={async (event) => {
              event.preventDefault();
              setBusy(true);
              setError("");
              try {
                await api("definition-project", {
                  name: data.name,
                  project_id: target,
                });
                await refreshProjects();
                setMove(false);
                navigate(definitionPath(target, data.name), { replace: true });
              } catch (failure) {
                setError(failure.message);
              } finally {
                setBusy(false);
              }
            }}
          >
            {error && (
              <div className="form-error" role="alert">
                {error}
              </div>
            )}
            <label className="field">
              <span>目标项目</span>
              <select
                aria-label="目标项目"
                value={target}
                onChange={(event) => setTarget(event.target.value)}
              >
                {projects.map((project) => (
                  <option key={project.id} value={project.id}>
                    {project.name}
                  </option>
                ))}
              </select>
            </label>
            <div className="dialog-actions">
              <button type="button" onClick={() => setMove(false)}>
                取消
              </button>
              <button
                className="primary"
                disabled={busy || target === projectId}
              >
                确认移动
              </button>
            </div>
          </form>
        </Dialog>
      )}
    </div>
  );
}

/** 不存在的管理资源提供明确返回入口。 */
export function NotFound({ message = "页面不存在" }) {
  return (
    <div className="resource-state">
      <h1>{message}</h1>
      <Link to="/projects">返回项目列表</Link>
    </div>
  );
}
