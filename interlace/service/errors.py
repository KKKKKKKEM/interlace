"""服务层可公开说明的错误类型；协议适配器只按这些类型映射状态码。"""

from __future__ import annotations


class ServiceError(Exception):
    """服务主动拒绝请求时的基类，消息由服务编写，可以原样返回调用方。"""


class NotFoundError(ServiceError):
    """请求的项目、定义、图或执行不存在，对应 HTTP 404。"""


class InvalidRequestError(ServiceError):
    """请求内容不符合服务契约，对应 HTTP 422。"""


class ConflictError(ServiceError):
    """请求与当前持久状态冲突，例如修订号过期或定义标识已被占用，对应 HTTP 409。"""


class ServiceCapacityError(ServiceError):
    """服务达到本地活跃执行上限，调用方应稍后重试，对应 HTTP 429。"""


class ServiceClosedError(ServiceError):
    """服务已经关闭，不再接受新的执行，对应 HTTP 409。"""


class OutputEncodingError(ServiceError):
    """执行结果无法按严格 JSON 传输；属于传输失败，不改变执行终态，对应 HTTP 500。"""
