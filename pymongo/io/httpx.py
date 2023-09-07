try:
    import typing

    from greenletio import await_
    from httpx import AsyncClient, RequestError, Response
    from httpx._config import DEFAULT_TIMEOUT_CONFIG
    from httpx._types import (
        AuthTypes,
        CertTypes,
        CookieTypes,
        HeaderTypes,
        ProxiesTypes,
        QueryParamTypes,
        RequestContent,
        RequestData,
        RequestFiles,
        TimeoutTypes,
        URLTypes,
        VerifyTypes,
    )

    def post(
        url: URLTypes,
        *,
        content: typing.Optional[RequestContent] = None,
        data: typing.Optional[RequestData] = None,
        files: typing.Optional[RequestFiles] = None,
        json: typing.Optional[typing.Any] = None,
        params: typing.Optional[QueryParamTypes] = None,
        headers: typing.Optional[HeaderTypes] = None,
        cookies: typing.Optional[CookieTypes] = None,
        auth: typing.Optional[AuthTypes] = None,
        follow_redirects: bool = False,
        timeout: TimeoutTypes = DEFAULT_TIMEOUT_CONFIG,
    ) -> Response:
        client = AsyncClient()
        return await_(
            client.post(
                url,
                content=content,
                data=data,
                files=files,
                json=json,
                params=params,
                headers=headers,
                cookies=cookies,
                auth=auth,
                follow_redirects=follow_redirects,
                timeout=timeout,
            )
        )

except ImportError:
    from httpx import RequestError, post
