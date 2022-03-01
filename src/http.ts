import { getSegment, Segment } from 'aws-xray-sdk-core';
import { SecurityContext, AuthorizerContext } from './sec';

type Headers = {[header: string]: boolean | number | string};
type Parameters = {[name: string]: string | undefined};

export interface HttpEvent {
    headers?: Headers,
    requestContext?: {
        accountId?: string,
        apiId?: string,
        authorizer?: {
            lambda?: AuthorizerContext
        },
        http?: {
            method?: string,
            path?: string
        }
    },
    body?: string,
    pathParameters?: Parameters,
    queryStringParameters?: Parameters,
    rawPath?: string,
    rawQueryString?: string
}

interface ErrorMessage {
    title?: string,
    detail?: string,
    source?: {
        pointer?: string,
        parameter?: string,
        header?: string
    }
}

export interface HttpResponse {
    statusCode?: number,
    headers?: Headers,
    meta?: any,
    links?: any,
    data?: any,
    errors?: ErrorMessage[],
    rawBody?: string,
    isBase64Encoded?: boolean
}

export interface HttpEventResponse {
    statusCode?: number,
    headers?: Headers,
    body?: string,
    isBase64Encoded?: boolean
} 

export type HttpRequestAsyncFunc = (r: HttpRequest) => Promise<HttpResponse>; 
export type HttpRequestFunc = (r: HttpRequest) => HttpResponse | undefined; 

function clean(d: any): any {
    if (Array.isArray(d)) {
        return d.filter(v => v != null)
            .map(v => (v && typeof v === 'object') ? clean(v) : v);
    } else {
        return Object.fromEntries(
            Object.entries(d)
                .filter(([_, v]) => v != null)
                .map(([k, v]) => [k, (typeof v === 'object' ? clean(v) : v)])
        );
    }
}

export class HttpRequest {
    private resp?: HttpResponse;
    private jsonPayload?: any;
    
    private constructor(
        public readonly event: HttpEvent, 
        public readonly secContext: SecurityContext,
        public readonly headers?: Headers,
        public readonly params?: Parameters,
        public readonly queryParams?: Parameters
    ) {
    }

    bodyAsJson(): any {
        if (this.event.body) {
            try {
                this.jsonPayload = clean(JSON.parse(this.event.body));
            } catch (err) {
                console.info(err);
            }
        }

        if (!this.jsonPayload) {
            this.resp =  {
                statusCode: 400,
                errors: [{
                    title: 'Bad Request',
                    detail: 'Payload must be a valid JSON'
                }]
            };
            throw new Error('Decoding error');
        }
        return this.jsonPayload;
    }

    ensure(func: HttpRequestFunc): HttpRequest {
        if (!this.resp) {
            try {
                this.resp = func(this);
            } catch(err) {
                if (!this.resp) {
                    console.error(err);
                    this.resp = {
                        statusCode: 500
                    };
                }
            }
        }
        return this;
    }

    async run(func: HttpRequestAsyncFunc): Promise<HttpEventResponse> {
        if (!this.resp) {
            try {
                this.resp = await func(this);
            } catch(err) {
                if (!this.resp) {
                    console.error(err);
                    this.resp = {
                        statusCode: 500
                    };
                }
            }
        }
        try {
            const segment = getSegment();
            if (this.resp.statusCode >= 500) {
                segment.addFaultFlag();
            } else if (this.resp.statusCode >= 400) {
                segment.addErrorFlag();
            }
            (segment as any).http = {
                request: {
                    method: this.event.requestContext.http.method,
                    url: this.event.requestContext.http.path
                },
                response: {
                    status: this.resp.statusCode
                }
            };
        } catch(_) {
        }
        return {
            statusCode: this.resp.statusCode,
            headers: this.resp.headers,
            body: this.resp.rawBody ?? JSON.stringify({
                meta: this.resp.meta,
                links: this.resp.links,
                data: this.resp.data,
                errors: this.resp.errors
            }),
            isBase64Encoded: this.resp.isBase64Encoded
        };
    }

    static fromEvent(event: HttpEvent): HttpRequest {
        console.info(event);
        return new HttpRequest(
            event, 
            SecurityContext.fromAuthorizer(event.requestContext?.authorizer),
            event.headers ?? {} as Headers,
            event.pathParameters ?? {} as Parameters,
            event.queryStringParameters ?? {} as Parameters
        );
    }
}

export const hasActiveSubscription: HttpRequestFunc = (r) => {
    if (!r.secContext.hasActiveSubscription()) {
        console.info('No active subscription');
        return {
            statusCode: 403
        };
    }
    return undefined;
}

export const hasValidUser: HttpRequestFunc = (r) => {
    if (!r.secContext.hasValidUser()) {
        console.info('No valid user');
        return {
            statusCode: 403
        };
    }
    return undefined;
}

export function isScopeAuthorized(scope: string): HttpRequestFunc {
    return (r: HttpRequest): HttpResponse | undefined => {
        if (!r.secContext.isScopeAuthorized(scope)) {
            console.info(`Scope is not authorized: ${scope}`);
            return {
                statusCode: 403
            };
        }
        return undefined;
    };
}

export function hasHeaders(headerNames: string[]): HttpRequestFunc {
    return (r: HttpRequest): HttpResponse | undefined => {
        if (!r.headers && headerNames) {
            return {
                statusCode: 400,
                errors: [{
                    title: 'Bad Request',
                    detail: 'Missing header',
                    source: {
                        header: headerNames[0]
                    }
                }]
            };
        }
        for (let h of headerNames) {
            if (!r.headers![h]) {
                return {
                    statusCode: 400,
                    errors: [{
                        title: 'Bad Request',
                        detail: 'Missing header',
                        source: {
                            header: h
                        }
                    }]
                };
            }
        }
        return undefined;
    };
}

export function hasPathParams(paramNames: string[]): HttpRequestFunc {
    return (r: HttpRequest): HttpResponse | undefined => {
        if (!r.params && paramNames) {
            console.info(`Missing path parameters: ${paramNames}`);
            return {
                statusCode: 404
            };
        }
        for (let p of paramNames) {
            if (!r.params![p]) {
                console.info(`Missing path parameter: ${p}`);
                return {
                    statusCode: 404
                };
            }
        }
        return undefined;
    };
}

export type ValidationFunctionType = { (data: any): boolean, errors?: { message?: string, instancePath?: string }[] };
export function hasValidPayload(validationFunc: ValidationFunctionType): HttpRequestFunc {
    return (r: HttpRequest): HttpResponse | undefined => {
        if (!validationFunc(r.bodyAsJson())) {
            return {
                statusCode: 400,
                errors: validationFunc.errors?.map((err) => ({
                    title: 'Bad Request',
                    detail: `${err.message}`,
                    source: {
                        pointer: err.instancePath
                    }
                }))
            };
        }
        return undefined;
    };
}

export function getUserFromRequest(r: HttpRequest): string {
    return r.secContext.props.userId ?? r.secContext.props.subscription?.itemId ?? '<unknown>';
}