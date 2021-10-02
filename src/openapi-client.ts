import axios, { AxiosInstance, AxiosRequestConfig } from 'axios';
import axiosRetry, { exponentialDelay } from 'axios-retry';

type BaseEndpointProps<T> = {
    payload: 'requestBody' extends keyof T ? (
        'content' extends keyof T['requestBody'] ? (
            'application/json' extends keyof T['requestBody']['content'] ? 
                T['requestBody']['content']['application/json'] : undefined
        ) : undefined
    ) : undefined,
    pathParameters: 'parameters' extends keyof T ? (
        'path' extends keyof T['parameters'] ?
            T['parameters']['path'] : undefined
    ) : undefined,
    queryParameters?: 'parameters' extends keyof T ? (
        'query' extends keyof T['parameters'] ?
            T['parameters']['query'] : undefined
    ) : undefined,
    headers?: 'parameters' extends keyof T ? (
        'header' extends keyof T['parameters'] ?
            T['parameters']['header'] : undefined
    ) : undefined
};

type StandardEndpointProps = {
    payload?: any,
    pathParameters?: {[key: string]: string},
    queryParameters?: {[key: string]: string},
    headers?: {[key: string]: string}
};

type RequiredPropertyNames<T> = {
    [K in keyof T]-?: T[K] extends undefined ? never: K
}[keyof T];

type EndpointProps<T> = Pick<BaseEndpointProps<T>, RequiredPropertyNames<BaseEndpointProps<T>>>;

type ResponseTypes<T> = 'responses' extends keyof T ? {
    [K in keyof T['responses']]: {
        statusCode: K,
        payload: 'content' extends keyof T['responses'][K] ? (
            'application/json' extends keyof T['responses'][K]['content'] ?
                T['responses'][K]['content']['application/json'] : unknown
        ) : unknown,
        headers: {[key: string]: string} & ('headers' extends keyof T['responses'][K] ? 
            {
                [HK in keyof T['responses'][K]['headers'] as Lowercase<string & HK>]: T['responses'][K]['headers'][HK]
            } : {}
        )
    }
} : never;

type AllResponseTypes<T> = ResponseTypes<T>[keyof ResponseTypes<T>];

function concealAuthorizationHeader(this: any, key: string, value: any) {
    return (key.toLowerCase() === 'authorization') ? '<CONCEALED>' : value;
}

export class OpenAPIClient<OpenAPIType> {
    http: AxiosInstance;

    constructor(
        private baseUrl: string,
        private apiKey: string
    ) {
        this.http = axios.create();
        axiosRetry(this.http, {
            retries: 5,
            retryCondition: (error) => error.response?.status === 429,
            retryDelay: exponentialDelay
        });
        this.http.interceptors.request.use(x => {
            console.info(`Calling ${x.method?.toUpperCase()} ${x.url} with params ${x.params}, headers ${JSON.stringify(x.headers, concealAuthorizationHeader, 4)} and payload ${JSON.stringify(x.data, undefined, 4)}`);
            return x;
        });
        this.http.interceptors.response.use(x => {
            console.info(`Receiving ${x.status} with headers ${JSON.stringify(x.headers, undefined, 4)} and payload ${JSON.stringify(x.data, undefined, 4)}`);
            return x;
        });
    }

    async fetch<
        PathType extends keyof OpenAPIType,
        MethodType extends keyof OpenAPIType[PathType]
    >(
        url: PathType,
        method: MethodType,
        options: EndpointProps<OpenAPIType[PathType][MethodType]>
    ) : Promise<AllResponseTypes<OpenAPIType[PathType][MethodType]>> {
        const opts = options as StandardEndpointProps;
        let finalUrl = url as string;
        if (opts.pathParameters) {
            Object.entries(opts.pathParameters).forEach(([k, v]) => {
                finalUrl = finalUrl.replace(`{${k}}`, v);
            });
        }
        const res = await this.http.request({
            method: method as AxiosRequestConfig['method'],
            url: finalUrl,
            baseURL: this.baseUrl,
            headers: {
                Authorization: `ApiKey ${this.apiKey}`,
                ...(opts.headers ?? {}),
            },
            params: opts.queryParameters,
            data: opts.payload,
            validateStatus: (s) => true
        });
        return {
            statusCode: res.status,
            headers: Object.fromEntries(
                Object.entries(res.headers).map(([k, v]) => [k.toLowerCase(), v])
            ),
            payload: res.data
        } as AllResponseTypes<OpenAPIType[PathType][MethodType]>;
    }
}