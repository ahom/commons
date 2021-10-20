import { DynamoDBClient, GetItemCommand, PutItemCommand, DeleteItemCommand, UpdateItemCommand, TransactWriteItemsCommand, TransactWriteItem, QueryCommand, AttributeValue, BatchWriteItemCommand, ScanCommand } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';

export interface TransactionItemCompatibleCommand {
    toTransactionItem(): TransactWriteItem
}

function encodeNamesAndValues(obj: {[key: string]: any}, prefix?: string) {
    let names: {[key: string]: string} = {};
    let values: {[key: string]: any} = {};
    let mapping: {[key: string]: string} = {};

    const pre = prefix ?? '';

    Object.entries(obj).forEach(([k, v], i) => {
        const keyParts = k.split('.');
        const keyPartNames = keyParts.map((kp, j) => `#${pre}${i}_${j}`);
        for (let j in keyParts) {
            names[keyPartNames[j]] = keyParts[j];
        }
        values[`:${pre}${i}`] = v;
        mapping[keyPartNames.join('.')] = `:${pre}${i}`;
    })

    values = marshall(values, { removeUndefinedValues: true });

    return { names, values, mapping };
}

export class CreateCommand<KeyType = any, ValueType = any> implements TransactionItemCompatibleCommand {
    readonly item: any;
    readonly conditionExpression: string;

    constructor(
        readonly dynamoDBClient: DynamoDBClient,
        readonly tableName: string,
        key: KeyType,
        value: ValueType
    ) {
        this.item = marshall({ ...value, ...key }, { removeUndefinedValues: true });
        this.conditionExpression = `attribute_not_exists(${Object.keys(key)[0]})`;
    }

    async send(): Promise<ValueType> {
        await this.dynamoDBClient.send(new PutItemCommand({
            TableName: this.tableName,
            Item: this.item,
            ConditionExpression: this.conditionExpression
        }));
        return this.data();
    }

    data() {
        return unmarshall(this.item) as ValueType;
    }

    toTransactionItem(): TransactWriteItem {
        return {
            Put: {
                TableName: this.tableName,
                Item: this.item,
                ConditionExpression: this.conditionExpression
            }
        }
    }
}

export class BatchWriteCommand<KeyType = any, ValueType = any> {
    readonly batches: any[][] = [];

    constructor(
        readonly dynamoDBClient: DynamoDBClient,
        readonly tableName: string,
        records: {
            key: KeyType,
            value: ValueType
        }[]
    ) {
        let longListItems = records.map(
            r => marshall({ ...r.value, ...r.key }, { removeUndefinedValues: true })
        );
        
        while (longListItems.length > 0) {
            this.batches.push(longListItems.splice(0, 25));
        }
    }

    private async sendWithRetries(requestItems: any, retryLeft: number, backoffMs: number) {
        if (retryLeft <= 0) {
            throw new Error('Could not complete BatchWrite after exhausting all retries');
        }

        const res = await this.dynamoDBClient.send(new BatchWriteItemCommand({
            RequestItems: requestItems
        }));

        if (res.UnprocessedItems?.[this.tableName]?.length > 0) {
            await new Promise((resolve) => setTimeout(resolve, backoffMs));
            return this.sendWithRetries(res.UnprocessedItems, retryLeft - 1, backoffMs * 2);
        }

        return res;
    }

    async send() {
        return await Promise.all(this.batches.map(
            batch => this.sendWithRetries({
                [this.tableName]: batch.map(
                    item => ({
                        PutRequest: {
                            Item: item
                        }
                    })
                )
            }, 5, 10)
        ));
    }
}

export interface RetrieveCommandOptions {
    readonly consistentRead?: boolean
}

export class RetrieveCommand<KeyType = any, ValueType = any> {
    readonly key: any;

    constructor(
        readonly dynamoDBClient: DynamoDBClient,
        readonly tableName: string,
        key: KeyType,
        readonly options?: RetrieveCommandOptions
    ) {
        this.key = marshall(key);
    }

    async send(): Promise<KeyType & ValueType | null> {
        const item = await this.dynamoDBClient.send(new GetItemCommand({
            TableName: this.tableName,
            Key: this.key,
            ConsistentRead: this.options?.consistentRead
        }));

        if (item?.Item) {
            return unmarshall(item.Item) as KeyType & ValueType;
        } else {
            return null;
        }
    }
}

export interface UpdateCommandOptions {
    readonly additionalConditions?: {[key: string]: any}
}

export class UpdateCommand<KeyType = any, ValueType = any> implements TransactionItemCompatibleCommand {
    readonly key: any;
    readonly updateExpression: string;
    readonly expressionAttributesNames: {[key: string]: string};
    readonly expressionAttributeValues: {[key: string]: any};
    readonly conditionExpression: string;

    constructor(
        readonly dynamoDBClient: DynamoDBClient,
        readonly tableName: string,
        key: KeyType,
        set: {[key: string]: any},
        options?: UpdateCommandOptions
    ) {
        this.key = marshall(key);

        const { names, values, mapping } = encodeNamesAndValues(set);
        this.expressionAttributeValues = values;
        this.expressionAttributesNames = names;
        this.updateExpression = `SET ${Object.entries(mapping).map(([k, v]) => `${k} = ${v}`).join(', ')}`;

        let conditions = [
            `attribute_exists(${Object.keys(key)[0]})`
        ];
        if (options?.additionalConditions) {
            const { names, values, mapping } = encodeNamesAndValues(options.additionalConditions, 'cond');
            this.expressionAttributeValues = {
                ...this.expressionAttributeValues,
                ...values
            };
            this.expressionAttributesNames = {
                ...this.expressionAttributesNames,
                ...names
            };
            conditions = conditions.concat(
                ...Object.entries(mapping).map(([k, v]) => `${k} = ${v}`)
            );
        }
        if (conditions.length > 0) {
            this.conditionExpression = conditions.join(' AND ');
        }
    }

    async send(): Promise<KeyType & ValueType | null> {
        const item = await this.dynamoDBClient.send(new UpdateItemCommand({
            TableName: this.tableName,
            Key: this.key,
            UpdateExpression: this.updateExpression,
            ExpressionAttributeNames: this.expressionAttributesNames,
            ExpressionAttributeValues: this.expressionAttributeValues,
            ConditionExpression: this.conditionExpression,
            ReturnValues: 'ALL_NEW'
        }));

        if (item?.Attributes) {
            return unmarshall(item.Attributes) as KeyType & ValueType;
        } else {
            return null;
        }
    }

    toTransactionItem(): TransactWriteItem {
        return {
            Update: {
                TableName: this.tableName,
                Key: this.key,
                UpdateExpression: this.updateExpression,
                ExpressionAttributeNames: this.expressionAttributesNames,
                ExpressionAttributeValues: this.expressionAttributeValues,
                ConditionExpression: this.conditionExpression
            }
        }
    }
}

export interface ReplaceCommandOptions {
    readonly additionalConditions?: {[key: string]: any}
}

export class ReplaceCommand<KeyType = any, ValueType = any> implements TransactionItemCompatibleCommand {
    readonly item: any;
    readonly expressionAttributesNames: {[key: string]: string};
    readonly expressionAttributeValues: {[key: string]: any};
    readonly conditionExpression: string;

    constructor(
        readonly dynamoDBClient: DynamoDBClient,
        readonly tableName: string,
        key: KeyType,
        value: ValueType,
        options?: ReplaceCommandOptions
    ) {
        this.item = marshall({ ...value, ...key }, { removeUndefinedValues: true });

        let conditions = [
            `attribute_exists(${Object.keys(key)[0]})`
        ];
        if (options?.additionalConditions) {
            const { names, values, mapping } = encodeNamesAndValues(options.additionalConditions);
            this.expressionAttributeValues = values;
            this.expressionAttributesNames = names;
            conditions = conditions.concat(
                ...Object.entries(mapping).map(([k, v]) => `${k} = ${v}`)
            );
        }
        if (conditions.length > 0) {
            this.conditionExpression = conditions.join(' AND ');
        }
    }

    async send(): Promise<KeyType & ValueType> {
        await this.dynamoDBClient.send(new PutItemCommand({
            TableName: this.tableName,
            Item: this.item,
            ExpressionAttributeNames: this.expressionAttributesNames,
            ExpressionAttributeValues: this.expressionAttributeValues,
            ConditionExpression: this.conditionExpression
        }));
        return unmarshall(this.item) as KeyType & ValueType;
    }

    toTransactionItem(): TransactWriteItem {
        return {
            Put: {
                TableName: this.tableName,
                Item: this.item,
                ExpressionAttributeNames: this.expressionAttributesNames,
                ExpressionAttributeValues: this.expressionAttributeValues,
                ConditionExpression: this.conditionExpression,
            }
        }
    }
}

export interface DeleteCommandOptions {
    readonly additionalConditions?: {[key: string]: any}
}

export class DeleteCommand<KeyType = any> implements TransactionItemCompatibleCommand {
    readonly key: any;
    readonly expressionAttributesNames: {[key: string]: string};
    readonly expressionAttributeValues: {[key: string]: any};
    readonly conditionExpression: string;

    constructor(
        readonly dynamoDBClient: DynamoDBClient,
        readonly tableName: string,
        key: KeyType,
        options?: DeleteCommandOptions
    ) {
        this.key = marshall(key);


        let conditions = [
            `attribute_exists(${Object.keys(key)[0]})`
        ];
        if (options?.additionalConditions) {
            const { names, values, mapping } = encodeNamesAndValues(options.additionalConditions);
            this.expressionAttributeValues = values;
            this.expressionAttributesNames = names; 
            conditions = conditions.concat(
                ...Object.entries(mapping).map(([k, v]) => `${k} = ${v}`)
            );
        } 
        if (conditions.length > 0) {
            this.conditionExpression = conditions.join(' AND ');
        }
    }

    async send(): Promise<void> {
        await this.dynamoDBClient.send(new DeleteItemCommand({
            TableName: this.tableName,
            Key: this.key,
            ExpressionAttributeNames: this.expressionAttributesNames,
            ExpressionAttributeValues: this.expressionAttributeValues,
            ConditionExpression: this.conditionExpression
        }));
    }

    toTransactionItem(): TransactWriteItem {
        return {
            Delete: {
                TableName: this.tableName,
                Key: this.key,
                ExpressionAttributeNames: this.expressionAttributesNames,
                ExpressionAttributeValues: this.expressionAttributeValues,
                ConditionExpression: this.conditionExpression
            }
        }
    }
}

export interface ListCommandResult<ValueType = any> {
    readonly count: number,
    readonly cursor?: any,
    readonly items: ValueType[]
}

type SortKeyCriteriaOperator = 'begins_with' | '>' | '>=' | '<' | '<=';

export interface SortKeyCriteria<SortKeyType> {
    readonly operator: SortKeyCriteriaOperator,
    readonly value: SortKeyType
}

function sortKeyCriteriaToCondition(operator: SortKeyCriteriaOperator, name: string, value: string) {
    if (operator === 'begins_with') {
        return `${operator}(${name}, ${value})`;
    }
    return `${name} ${operator} ${value}`;
}

export interface ListCommandOptions<SortKeyType> {
    readonly indexName?: string,
    readonly limit?: number,
    readonly from?: any,
    readonly ascending?: boolean,
    readonly count?: boolean,
    readonly sortKeyCriteria?: SortKeyCriteria<SortKeyType>[]
}

export class ListCommand<HashKeyType = any, SortKeyType = any, ValueType = any> {
    readonly keyConditionExpression: string;
    readonly expressionAttributesNames: {[key: string]: string};
    readonly expressionAttributeValues: {[key: string]: any};
    readonly indexName?: string;
    readonly limit: number;
    readonly ascendingScan: boolean;
    readonly count: boolean;
    readonly exclusiveStartKey: any; 

    constructor(
        readonly dynamoDBClient: DynamoDBClient,
        readonly tableName: string,
        hashKey: HashKeyType,
        options?: ListCommandOptions<SortKeyType>
    ) {
        this.indexName = options?.indexName;
        this.limit = options?.limit ?? 20;
        this.ascendingScan = options?.ascending ?? true;
        this.count = options?.count ?? false;

        if (options?.from) {
            this.exclusiveStartKey = options.from;
        }

        const { names, values, mapping } = encodeNamesAndValues(hashKey);
        this.expressionAttributeValues = values;
        this.expressionAttributesNames = names;
        if (Object.entries(mapping).length != 1) {
            throw Error(`hashKey should only have one field, got: ${JSON.stringify(hashKey)}`);
        }
        let conditions = [
            Object.entries(mapping)[0].join(' = ')
        ];

        if (options?.sortKeyCriteria) {
            for (const idx in options.sortKeyCriteria) {
                const { operator, value } = options.sortKeyCriteria[idx];
                const { names, values, mapping } = encodeNamesAndValues(value, `skc${idx}`);
                this.expressionAttributeValues = {
                    ...this.expressionAttributeValues,
                    ...values
                };
                this.expressionAttributesNames = {
                    ...this.expressionAttributesNames,
                    ...names
                };
                conditions = conditions.concat(
                    ...Object.entries(mapping).map(([k, v]) => sortKeyCriteriaToCondition(operator, k, v))
                );
            }
        }

        if (conditions.length > 0) {
            this.keyConditionExpression = conditions.join(' AND ');
        }
    }

    async send(): Promise<ListCommandResult<KeyType & ValueType>> {
        const results = await this.dynamoDBClient.send(new QueryCommand({
            TableName: this.tableName,
            KeyConditionExpression: this.keyConditionExpression,
            ExpressionAttributeNames: this.expressionAttributesNames,
            ExpressionAttributeValues: this.expressionAttributeValues,
            ExclusiveStartKey: this.exclusiveStartKey,
            ScanIndexForward: this.ascendingScan,
            IndexName: this.indexName,
            Limit: this.limit,
            Select: this.count ? 'COUNT' : undefined
        }));

        return {
            count: results?.Count ?? 0,
            cursor: results?.LastEvaluatedKey,
            items: (results?.Items?.map((item) => unmarshall(item)).map((item) => {
                return {
                    ...item,
                    ...(
                        (item.projections && !item.attributes) ? {
                            attributes: item.projections
                        } : {}
                    )
                };
            }) ?? []) as (KeyType & ValueType)[]
        };
    }
}

export interface FullScanCommandResult<ValueType = any> {
    readonly count: number,
    readonly cursor?: any,
    readonly items: ValueType[]
}

export interface FullScanCommandOptions {
    readonly from?: any
}

export class FullScanCommand<HashKeyType = any, SortKeyType = any, ValueType = any> {
    readonly exclusiveStartKey: any; 

    constructor(
        readonly dynamoDBClient: DynamoDBClient,
        readonly tableName: string,
        options?: FullScanCommandOptions
    ) {
        if (options?.from) {
            this.exclusiveStartKey = options.from;
        }
    }

    async send(): Promise<FullScanCommandResult<KeyType & ValueType>> {
        const results = await this.dynamoDBClient.send(new ScanCommand({
            TableName: this.tableName,
            ExclusiveStartKey: this.exclusiveStartKey
        }));

        return {
            count: results?.Count ?? 0,
            cursor: results?.LastEvaluatedKey,
            items: (results?.Items?.map((item) => unmarshall(item)).map((item) => {
                return {
                    ...item,
                    ...(
                        (item.projections && !item.attributes) ? {
                            attributes: item.projections
                        } : {}
                    )
                };
            }) ?? []) as (KeyType & ValueType)[]
        };
    }
}

export class Transaction {
    static async run(dynamoDBClient: DynamoDBClient, commands: TransactionItemCompatibleCommand[]): Promise<void> {
        await dynamoDBClient.send(new TransactWriteItemsCommand({ 
            TransactItems: commands.map((c) => c.toTransactionItem())
        }));
    }
}