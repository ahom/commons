process.env.AWS_REGION = 'LOCAL'

import { mocked } from 'ts-jest/utils';

jest.mock('@aws-sdk/client-dynamodb');

import { HttpRequest } from '../src/http';
import { deleteResource, getResource, postResource, putResource, putPartialResource, listResources } from '../src/http-actions';
import { resourceTable } from '../src/db/resource-table';

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { index } from '../src/db';

const dynamoDBClient = new DynamoDBClient({});
const mockedSend = mocked(dynamoDBClient.send);

const table = resourceTable<{ id: string }, { sort: string }>(
    dynamoDBClient,
    'table'
).build();
const tableIndex = index<{ id: string }, { sort: string }>(
    dynamoDBClient,
    'table',
    'index'
).build();

describe('GetResource', () => {
    const getResourceFunc = getResource({
        table,
        keyFn: (r) => ({ id: r.params!.id!, sort: 's' })
    });
    test('200 on existing document', () => {
        mockedSend.mockImplementationOnce(() => ({
            Item: {
                id: { S: 'ID' },
                meta: { M: { etag: { S: 'etag' } } }
            }
        }));
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                }
            })
            .run(getResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(200);
                expect(data.headers).toBeTruthy();
                expect(data.headers!.ETag).toEqual('etag');

                expect(data.body).toBeTruthy();
                const obj = JSON.parse(data.body!);
                expect(obj.data).toMatchObject({
                    id: 'ID',
                    meta: { etag: 'etag' }
                });
            });
    });
    test('404 on missing document', () => {
        mockedSend.mockImplementationOnce(() => null);
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                }
            })
            .run(getResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(404);
            });
    });
    test('500 on error', () => {
        mockedSend.mockImplementationOnce(() => { throw new Error() });
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                }
            })
            .run(getResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(500);
            });
    });
});

describe('PostResource', () => {
    const postResourceFunc = postResource({
        table,
        keyFn: (r, id) => ({
            id,
            sort: 's'
        })
    });
    test('201 on success', () => {
        const userId = 'user_id';
        return HttpRequest.fromEvent({
                body: JSON.stringify({
                    postal_code: '06600'
                }),
                requestContext: { authorizer: { lambda: { userId: userId } } }
            })
            .run(postResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(201);
                expect(data.headers?.ETag).toBeTruthy();

                expect(data.body).toBeTruthy();
                const obj = JSON.parse(data.body!);
                expect(obj.data).toMatchObject({
                    attributes: {
                        postal_code: '06600'
                    }
                });
                expect(obj.data.id).toBeTruthy();
                expect(obj.data.meta.etag).toBeTruthy();
                expect(obj.data.meta.created_at).toBeTruthy();
                expect(obj.data.meta.created_by).toBeTruthy();
                expect(obj.data.meta.created_by).toEqual(userId);
                expect(obj.data.meta.last_updated_at).toBeTruthy();
                expect(obj.data.meta.last_updated_by).toBeTruthy();
                expect(obj.data.meta.last_updated_by).toEqual(userId);
            });
    });
    test('400 on no body', () => {
        return HttpRequest.fromEvent({})
            .run(postResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(400);
            });
    });
    test('201 on override', () => {
        return HttpRequest.fromEvent({
                body: JSON.stringify({
                    postal_code: '06600'
                })
            })
            .run(postResource({
                table,
                keyFn: (r, id) => ({
                    id,
                    sort: 's'
                }),
                overrideFields: (r, d) => ({ postal_code: '1', city: 'Antibes' })
            }))
            .then(data => {
                expect(data.statusCode).toEqual(201);
                expect(data.body).toBeTruthy();
                const obj = JSON.parse(data.body!);
                expect(obj.data).toMatchObject({
                    attributes: {
                        postal_code: '1',
                        city: 'Antibes'
                    }
                });
            });
    });
    test('500 on error', () => {
        mockedSend.mockImplementationOnce(() => { throw new Error() });
        return HttpRequest.fromEvent({
                body: JSON.stringify({
                    postal_code: '06600'
                })
            })
            .run(postResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(500);
            });
    });
});

describe('PutResource', () => {
    const putResourceFunc = putResource({
        table,
        keyFn: (r) => ({ id: r.params!.id!, sort: 's' }),
    });
    test('200 on success', () => {
        mockedSend.mockImplementationOnce(() => ({
            Attributes: {
                id: { S: 'ID' },
                meta: { M: { etag: { S: 'etag' } } }
            }
        }));
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                },
                body: JSON.stringify({
                    postal_code: '06600'
                })
            })
            .run(putResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(200);
                expect(data.headers?.ETag).toBeTruthy();

                expect(data.body).toBeTruthy();
                const obj = JSON.parse(data.body!);
                expect(obj.data).toMatchObject({
                    id: 'ID',
                    meta: { etag: 'etag' }
                });
                expect(obj.data.meta.etag).toBeTruthy();
            });
    });
    test('409 on conflict', () => {
        mockedSend.mockImplementationOnce(() => { throw {
            name: 'ConditionalCheckFailedException'
        }});
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                },
                body: JSON.stringify({
                    postal_code: '06600'
                })
            })
            .run(putResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(409);
            });
    });
    test('500 on error', () => {
        mockedSend.mockImplementationOnce(() => { throw new Error() });
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                },
                body: JSON.stringify({
                    postal_code: '06600'
                })
            })
            .run(putResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(500);
            });
    });
});

describe('DeleteResource', () => {
    const deleteResourceFunc = deleteResource({
        table,
        keyFn: (r) => ({ id: r.params!.id!, sort: 's' }),
    });
    test('200 on success', () => {
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                }
            })
            .run(deleteResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(200);
            });
    });
    test('409 on conflict', () => {
        mockedSend.mockImplementationOnce(() => { throw {
            name: 'ConditionalCheckFailedException'
        }});
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                }
            })
            .run(deleteResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(409);
            });
    });
    test('500 on error', () => {
        mockedSend.mockImplementationOnce(() => { throw new Error() });
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                }
            })
            .run(deleteResourceFunc)
            .then(data => {
                expect(data.statusCode).toEqual(500);
            });
    });
});

describe('ListResources', () => {
    const listResourcesFunc = listResources({
        listable: table,
        hashKeyFn: (r) => ({ id: r.params!.id! })
    });
    test('200 on success', () => {
        mockedSend.mockImplementationOnce(() => ({
            Count: 10,
            Items: [{
                id: { S: 'ID' },
                meta: { M: { etag: { S: 'etag' } } }
            }],
            LastEvaluatedKey: {
                test: 'key'
            } 
        }));
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                },
                queryStringParameters: {
                    'page[size]': '10',
                    'page[cursor]': Buffer.from('2').toString('base64')
                }
            })
            .run(listResourcesFunc)
            .then(data => {
                expect(data.statusCode).toEqual(200);

                expect(data.body).toBeTruthy();
                const obj = JSON.parse(data.body!);
                expect(obj.meta?.page?.cursor).toBeTruthy();
                expect(obj.data).toMatchObject([{ id: 'ID', meta: { etag: 'etag' } }]);
                expect(obj.links?.next).toBeTruthy();
            });
    });
    test('200 on success with index', () => {
        mockedSend.mockImplementationOnce(() => ({
            Count: 10,
            Items: [{
                id: { S: 'ID' },
                meta: { M: { etag: { S: 'etag' } } }
            }],
            LastEvaluatedKey: {
                test: 'key'
            } 
        }));
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                }
            })
            .run(listResources({
                listable: tableIndex,
                hashKeyFn: (r) => ({ id: r.params!.id! })
            })).then(data => {
                expect(data.statusCode).toEqual(200);
            });
    });
    test('200 on success with sortkey conditions', () => {
        mockedSend.mockImplementationOnce(() => ({
            Count: 10,
            Items: [{
                id: { S: 'ID' },
                meta: { M: { etag: { S: 'etag' } } }
            }],
            LastEvaluatedKey: {
                test: 'key'
            } 
        }));
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                }
            })
            .run(listResources({
                listable: table,
                hashKeyFn: (r) => ({ id: r.params!.id! }),
                optionsFn: (r) => ({
                    sortKeyCriterion: {
                        name: 'sort',
                        operator: 'begins_with',
                        value: 'asc'
                    }
                }) 
            })).then(data => {
                expect(data.statusCode).toEqual(200);
            });
    });
    test('200 on last page', () => {
        mockedSend.mockImplementationOnce(() => ({
            Count: 10,
            Items: [{
                id: { S: 'ID' },
                meta: { M: { etag: { S: 'etag' } } }
            }]
        }));
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                },
                queryStringParameters: {
                    'page[size]': '10',
                    'page[cursor]': Buffer.from('2').toString('base64')
                }
            })
            .run(listResourcesFunc)
            .then(data => {
                expect(data.statusCode).toEqual(200);

                expect(data.body).toBeTruthy();
                const obj = JSON.parse(data.body!);
                expect(obj.meta?.page?.cursor).toBeFalsy();
                expect(obj.data).toMatchObject([{ id: 'ID', meta: { etag: 'etag' } }]);
            });
    });
    test('400 on bad query param', () => {
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                },
                queryStringParameters: {
                    'page[cursor]': 'XXX'
                }
            })
            .run(listResourcesFunc)
            .then(data => {
                expect(data.statusCode).toEqual(400);
            });
    });
    test('500 on error', () => {
        mockedSend.mockImplementationOnce(() => { throw new Error() });
        return HttpRequest.fromEvent({
                pathParameters: {
                    id: 'ID'
                },
                queryStringParameters: {
                    'page[size]': '10'
                }
            })
            .run(listResourcesFunc)
            .then(data => {
                expect(data.statusCode).toEqual(500);
            });
    });
});