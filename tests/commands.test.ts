process.env.AWS_REGION = 'LOCAL'

import { mocked } from 'ts-jest/utils';
jest.mock('@aws-sdk/client-dynamodb');

import { DynamoDBClient, PutItemCommand, GetItemCommand, DeleteItemCommand, UpdateItemCommand, QueryCommand, BatchWriteItemCommand, ScanCommand } from '@aws-sdk/client-dynamodb';
const mockedPutItemCommand = mocked(PutItemCommand);
const mockedBatchWriteItemCommand = mocked(BatchWriteItemCommand);
const mockedUpdateItemCommand = mocked(UpdateItemCommand);
const mockedGetItemCommand = mocked(GetItemCommand);
const mockedDeleteItemCommand = mocked(DeleteItemCommand);
const mockedQueryCommand = mocked(QueryCommand);
const mockedScanCommand = mocked(ScanCommand);
const mockedDynamoDBClient = mocked(DynamoDBClient, true);

import { CreateCommand, DeleteCommand, RetrieveCommand, UpdateCommand, ReplaceCommand, ListCommand, BatchWriteCommand, FullScanCommand } from '../src/db/commands';

const dynamoDBClient = new DynamoDBClient({});
const mockedSend = mocked(dynamoDBClient.send);

describe('CreateCommand', () => {
    test('Sends right command to DynamoDBClient', () => {
        return new CreateCommand(
            dynamoDBClient,
            'table', 
            {
                id: '123'
            }, 
            {
                id: 'OVERWRITTEN',
                value: 'lol'
            }
        ).send().then(data => {
            expect(data).toEqual({
                id: '123',
                value: 'lol'
            });

            expect(mockedPutItemCommand).toBeCalledWith(
                expect.objectContaining({
                    TableName: 'table',
                    Item: {
                        id: {
                            S: '123'
                        },
                        value: {
                            S: 'lol'
                        }
                    },
                    ConditionExpression: 'attribute_not_exists(id)'
                })
            );
        });
    });
});

describe('BatchWriteCommand', () => {
    test('Sends right command to DynamoDBClient', () => {
        mockedSend.mockImplementationOnce(() => ({
            UnprocessedItems: []
        }));
        return new BatchWriteCommand(
            dynamoDBClient,
            'table', 
            [
                {
                    key: {
                        id: '123'
                    }, 
                    value: {
                        id: 'OVERWRITTEN',
                        value: 'lol'
                    }
                },
                {
                    key: {
                        id: '234'
                    }, 
                    value: {
                        value: 'lil'
                    }
                },

            ]
        ).send().then(data => {
            expect(mockedBatchWriteItemCommand).toBeCalledWith(
                expect.objectContaining({
                    RequestItems: {
                        table: [
                            {
                                PutRequest: {
                                    Item: {
                                        id: {
                                            S: '123'
                                        },
                                        value: {
                                            S: 'lol'
                                        }
                                    }
                                }
                            },
                            {
                                PutRequest: {
                                    Item: {
                                        id: {
                                            S: '234'
                                        },
                                        value: {
                                            S: 'lil'
                                        }
                                    }
                                }
                            }
                        ]
                    }
                })
            );
        });
    });
    test('Properly split big arrays of items', () => {
        mockedBatchWriteItemCommand.mockClear();
        mockedSend.mockImplementation(() => ({
            UnprocessedItems: []
        }));
        return new BatchWriteCommand(
            dynamoDBClient,
            'table', 
            Array(52).fill({
                key: {
                    id: '123'
                }, 
                value: {
                    value: 'lol'
                }
            })
        ).send().then(data => {
            mockedSend.mockClear();
            expect(mockedBatchWriteItemCommand.mock.calls).toEqual([
                [{
                    RequestItems: {
                        table: Array(25).fill({
                            PutRequest: {
                                Item: {
                                    id: {
                                        S: '123'
                                    },
                                    value: {
                                        S: 'lol'
                                    }
                                }
                            }
                        })
                    }
                }],
                [{
                    RequestItems: {
                        table: Array(25).fill({
                            PutRequest: {
                                Item: {
                                    id: {
                                        S: '123'
                                    },
                                    value: {
                                        S: 'lol'
                                    }
                                }
                            }
                        })
                    }
                }],
                [{
                    RequestItems: {
                        table: Array(2).fill({
                            PutRequest: {
                                Item: {
                                    id: {
                                        S: '123'
                                    },
                                    value: {
                                        S: 'lol'
                                    }
                                }
                            }
                        })
                    }
                }]
            ]);
        });
    });
});

describe('UpdateCommand', () => {
    test('Sends right command to DynamoDBClient', () => {
        return new UpdateCommand(
            dynamoDBClient,
            'table', 
            {
                id: '123'
            }, 
            {
                value: 'lol',
                'nested.value': 'lil'
            },
            {
                additionalConditions: {
                    field: 'value'
                }
            }
        ).send().then(data => {
            expect(data).toBeNull();

            expect(mockedUpdateItemCommand).toBeCalledWith(
                expect.objectContaining({
                    TableName: 'table',
                    Key: {
                        id: {
                            S: '123'
                        }
                    },
                    UpdateExpression: 'SET #_0_0 = :_0, #_1_0.#_1_1 = :_1',
                    ExpressionAttributeNames: {
                        '#_0_0': 'value',
                        '#_1_0': 'nested',
                        '#_1_1': 'value',
                        '#cond_0_0': 'field' 
                    },
                    ExpressionAttributeValues: {
                        ':_0': { S: 'lol' },
                        ':_1': { S: 'lil' },
                        ':cond_0': { S: 'value' }
                    },
                    ConditionExpression: 'attribute_exists(id) AND #cond_0_0 = :cond_0',
                    ReturnValues: 'ALL_NEW'
                })
            );
        });
    });
});

describe('ReplaceCommand', () => {
    test('Sends right command to DynamoDBClient', () => {
        return new ReplaceCommand(
            dynamoDBClient,
            'table', 
            {
                id: '123'
            }, 
            {
                id: 'OVERWRITTEN',
                value: 'lol'
            },
            {
                additionalConditions: {
                    field: 'value'
                }
            }
        ).send().then(data => {
            expect(data).toMatchObject({
                id: '123',
                value: 'lol'
            });

            expect(mockedPutItemCommand).toBeCalledWith(
                expect.objectContaining({
                    TableName: 'table',
                    Item: {
                        id: {
                            S: '123'
                        },
                        value: {
                            S: 'lol'
                        }
                    },
                    ExpressionAttributeNames: {
                        '#_0_0': 'field' 
                    },
                    ExpressionAttributeValues: {
                        ':_0': { S: 'value' }
                    },
                    ConditionExpression: 'attribute_exists(id) AND #_0_0 = :_0',
                })
            );
        });
    });

    test('Do not send empty expression attributes', () => {
        return new ReplaceCommand(
            dynamoDBClient,
            'table', 
            {
                id: '123'
            }, 
            {
                value: 'lol'
            }
        ).send().then(data => {
            expect(data).toMatchObject({
                id: '123',
                value: 'lol'
            });

            expect(mockedPutItemCommand).toBeCalledWith(
                expect.objectContaining({
                    TableName: 'table',
                    Item: {
                        id: {
                            S: '123'
                        },
                        value: {
                            S: 'lol'
                        }
                    },
                    ExpressionAttributeNames:  undefined,
                    ExpressionAttributeValues: undefined,
                    ConditionExpression: 'attribute_exists(id)'
                })
            );
        });
    });
});

describe('RetrieveCommand', () => {
    test('Sends right command to DynamoDBClient', () => {
        return new RetrieveCommand(
            dynamoDBClient,
            'table', 
            {
                id: '123'
            },
            {
                consistentRead: true
            } 
        ).send().then(data => {
            expect(data).toBeNull();

            expect(mockedGetItemCommand).toBeCalledWith(
                expect.objectContaining({
                    TableName: 'table',
                    Key: {
                        id: {
                            S: '123'
                        }
                    },
                    ConsistentRead: true
                })
            );
        });
    });
});

describe('DeleteCommand', () => {
    test('Sends right command to DynamoDBClient', () => {
        return new DeleteCommand(
            dynamoDBClient,
            'table', 
            {
                id: '123'
            }, 
            {
                additionalConditions: {
                    field: 'value'
                }
            }
        ).send().then(data => {
            expect(mockedDeleteItemCommand).toBeCalledWith(
                expect.objectContaining({
                    TableName: 'table',
                    Key: {
                        id: {
                            S: '123'
                        }
                    },
                    ExpressionAttributeNames: {
                        '#_0_0': 'field' 
                    },
                    ExpressionAttributeValues: {
                        ':_0': { S: 'value' }
                    },
                    ConditionExpression: 'attribute_exists(id) AND #_0_0 = :_0',
                })
            );
        });
    });

    test('Do not send empty expression attributes', () => {
        return new DeleteCommand(
            dynamoDBClient,
            'table', 
            {
                id: '123'
            } 
        ).send().then(data => {
            expect(mockedDeleteItemCommand).toBeCalledWith(
                expect.objectContaining({
                    TableName: 'table',
                    Key: {
                        id: {
                            S: '123'
                        }
                    },
                    ExpressionAttributeNames:  undefined,
                    ExpressionAttributeValues: undefined,
                    ConditionExpression: 'attribute_exists(id)'
                })
            );
        });
    });
});

describe('ListCommand', () => {
    test('Sends right command to DynamoDBClient', () => {
        mockedQueryCommand.mockClear();
        return new ListCommand(
            dynamoDBClient,
            'table', 
            {
                id: '123'
            }, 
            {
                limit: 15,
                ascending: true,
                from: { test: 'lol' }
            }
        ).send().then(data => {
            expect(mockedQueryCommand).toBeCalledWith(
                expect.objectContaining({
                    TableName: 'table',
                    KeyConditionExpression: '#_0_0 = :_0',
                    ExpressionAttributeNames: {
                        '#_0_0': 'id'
                    },
                    ExpressionAttributeValues: {
                        ':_0': { S: '123' },
                    },
                    Limit: 15,
                    ScanIndexForward: true,
                    ExclusiveStartKey: {
                        test: 'lol'
                    }
                })
            );
        });
    });
    test('Sends right command to DynamoDBClient with count', () => {
        mockedQueryCommand.mockClear();
        return new ListCommand(
            dynamoDBClient,
            'table', 
            {
                id: '123'
            }, 
            {
                limit: 15,
                ascending: true,
                from: { test: 'lol' },
                count: true
            }
        ).send().then(data => {
            expect(mockedQueryCommand).toBeCalledWith(
                expect.objectContaining({
                    TableName: 'table',
                    KeyConditionExpression: '#_0_0 = :_0',
                    ExpressionAttributeNames: {
                        '#_0_0': 'id'
                    },
                    ExpressionAttributeValues: {
                        ':_0': { S: '123' },
                    },
                    Limit: 15,
                    ScanIndexForward: true,
                    ExclusiveStartKey: {
                        test: 'lol'
                    },
                    Select: 'COUNT'
                })
            );
        });
    });
    test('Sends right command to DynamoDBClient with index and sortKeyCriteria/filterCriteria', () => {
        mockedQueryCommand.mockClear();
        return new ListCommand(
            dynamoDBClient,
            'table', 
            {
                id: '123'
            }, 
            {
                limit: 15,
                ascending: true,
                from: { test: 'lol' },
                indexName: 'index',
                sortKeyCriteria: [
                    { operator: 'begins_with', value: { sort: 'start' }},
                    { operator: '<', value: { sort: 'lower' }}
                ],
                filterCriteria: [
                    [
                        { operator: '>=', value: { ['test.nested']: 12 }},
                        { operator: '=', value: { lol: 'ah' }},
                    ],
                    [
                        { operator: 'begins_with', value: { lil: 'bla' }}
                    ]
                ]
            }
        ).send().then(data => {
            expect(mockedQueryCommand).toBeCalledWith(
                expect.objectContaining({
                    TableName: 'table',
                    KeyConditionExpression: '#_0_0 = :_0 AND begins_with(#skc_0_0_0, :skc_0_0) AND #skc_1_0_0 < :skc_1_0',
                    FilterExpression: '(#fc_0_0_0_0.#fc_0_0_0_1 >= :fc_0_0_0 AND #fc_0_1_0_0 = :fc_0_1_0) OR (begins_with(#fc_1_0_0_0, :fc_1_0_0))',
                    ExpressionAttributeNames: {
                        '#_0_0': 'id',
                        '#skc_0_0_0': 'sort',
                        '#skc_1_0_0': 'sort',
                        '#fc_0_0_0_0': 'test',
                        '#fc_0_0_0_1': 'nested',
                        '#fc_0_1_0_0': 'lol',
                        '#fc_1_0_0_0': 'lil'
                    },
                    ExpressionAttributeValues: {
                        ':_0': { S: '123' },
                        ':skc_0_0': { S: 'start' },
                        ':skc_1_0': { S: 'lower' },
                        ':fc_0_0_0': { N: '12' },
                        ':fc_0_1_0': { S: 'ah' },
                        ':fc_1_0_0': { S: 'bla' }
                    },
                    Limit: 15,
                    ScanIndexForward: true,
                    IndexName: 'index',
                    ExclusiveStartKey: {
                        test: 'lol'
                    }
                })
            );
        });
    });
});

describe('FullScanCommand', () => {
    test('Sends right command to DynamoDBClient', () => {
        mockedScanCommand.mockClear();
        return new FullScanCommand(
            dynamoDBClient,
            'table', 
            {
                from: { test: 'lol' }
            }
        ).send().then(data => {
            expect(mockedScanCommand).toBeCalledWith(
                expect.objectContaining({
                    TableName: 'table',
                    ExclusiveStartKey: {
                        test: 'lol'
                    }
                })
            );
        });
    });
});