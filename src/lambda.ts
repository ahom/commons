import { InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { HttpEvent, HttpEventResponse } from './http';

interface HttpCallOptions {
    name: string,
    request: HttpEvent
};

export async function httpCall(client: LambdaClient, options: HttpCallOptions): Promise<HttpEventResponse> {
    console.info(`Calling lambda ${options.name} with request: ${JSON.stringify(options.request)}`);
    const resp = await client.send(new InvokeCommand({
        FunctionName: options.name,
        Payload: Buffer.from(JSON.stringify(options.request))
    }));

    if (!resp.Payload) {
        throw new Error(`HTTP Lambda Invoke in error, missing Payload: ${resp.FunctionError}`);
    }

    const lambdaResponse: HttpEventResponse = JSON.parse(Buffer.from(resp.Payload).toString());

    if (!lambdaResponse.statusCode) {
        throw new Error(`HTTP Lambda Invoke response in error, missing statusCode: ${JSON.stringify(lambdaResponse)}`);
    }

    return lambdaResponse;
}