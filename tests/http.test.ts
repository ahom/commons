import { HttpRequest } from "../src/http";

describe('HttpRequest', () => {
    test('Run returns HttpResponse', () => {
        return HttpRequest.fromEvent({}).run((r) => Promise.resolve({
            statusCode: 1
        })).then(data => {
            expect(data).toMatchObject({ statusCode: 1 });
        });
    });
    test('Run exception results in 500', () => {
        return HttpRequest.fromEvent({}).run((r) => {
            throw new Error('lal');
        }).then(data => {
            expect(data).toMatchObject({ statusCode: 500 });
        });
    });
    test('Ensure response bypasses run', () => {
        return HttpRequest.fromEvent({})
            .ensure((r) => ({ statusCode: 1 }))
            .run((r) => Promise.resolve({
            statusCode: 2
        })).then(data => {
            expect(data).toMatchObject({ statusCode: 1 });
        });
    });
    test('Ensure null response calls run', () => {
        return HttpRequest.fromEvent({})
            .ensure((r) => undefined)
            .run((r) => Promise.resolve({
            statusCode: 1
        })).then(data => {
            expect(data).toMatchObject({ statusCode: 1 });
        });
    });
});