import { createContext, useContext } from "react";
import { createParser } from "eventsource-parser";

const tokenKey = "interlace.token";

/** 管理后台布局唯一持有令牌，并向图工作台共享请求函数。 */
export const ApiContext = createContext(null);

/** 读取共享的 JSON 请求函数 `api` 与 SSE 读取函数 `stream`。 */
export function useApi() {
  return useContext(ApiContext);
}

/** 读取当前浏览器会话保存的访问令牌。 */
export function readToken() {
  return sessionStorage.getItem(tokenKey) || "";
}

/** 将访问令牌保存到当前浏览器会话。 */
export function saveToken(value) {
  sessionStorage.setItem(tokenKey, value);
}

const authorization = (token) =>
  token ? { Authorization: `Bearer ${token}` } : {};

/** 将 FastAPI 错误体转换为可读文本，校验错误逐条拼接。 */
const errorText = (value) => {
  const detail = value.detail || value;
  return Array.isArray(detail)
    ? detail.map((item) => item.msg).join("；")
    : typeof detail === "string"
      ? detail
      : JSON.stringify(detail);
};

/** 生成携带令牌的 JSON 请求函数；有请求体时默认 POST，401 交给界面打开登录。 */
export function createApi(token, onUnauthorized) {
  return async (path, body, method) => {
    const response = await fetch(`./api/${path}`, {
      method: method || (body === undefined ? "GET" : "POST"),
      headers: {
        "Content-Type": "application/json",
        ...authorization(token),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    if (response.status === 401) onUnauthorized();
    const value = await response.json();
    if (!response.ok) throw new Error(errorText(value));
    return value;
  };
}

/** 生成携带令牌的 SSE 读取函数；连接失败时抛出调用方给定的文案。 */
export function createStream(token) {
  return async (path, { signal, failure, onEvent }) => {
    const response = await fetch(`./api/${path}`, {
      headers: authorization(token),
      signal,
    });
    if (!response.ok) throw new Error(failure);
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
}
